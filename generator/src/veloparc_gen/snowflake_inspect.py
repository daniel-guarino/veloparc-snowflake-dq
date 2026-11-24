"""Read-only inspection of what is already in Snowflake, before writing the bronze layer (Phase 6).

Answers the questions the bronze mapping depends on, from the real objects rather than from the docs:

- connector tables (KAFKA_*): owner, change tracking, rows and offsets per partition, the keys of RECORD_METADATA
  and the type each header arrives with (the default Kafka Connect header converter may turn "1" into a number);
- landing stage: files, bytes and day range per topic, plus the shape of one envelope per topic;
- generator stage: the run manifests, reference tables and state files;
- Python packages available to UDFs (jsonschema and the packages its format checks need).

Only SHOW, LIST and SELECT statements are run. A failing statement is recorded under "errors" and the inspection
goes on, so one run shows every problem. Payload values are fictitious; no account identifiers are printed.
Column aliases avoid Snowflake reserved words (ROWS, PARTITION, ...).
"""

# ruff: noqa: S608  -- identifiers are module constants, or table names from SHOW checked against _IDENT

from __future__ import annotations

import json
import re
from collections import defaultdict

DATABASE = "VLP_DP"
BRONZE = f"{DATABASE}.BRONZE"
LANDING = f"{BRONZE}.LANDING"
GENERATOR = f"{BRONZE}.GENERATOR"
FILE_FORMAT = f"{BRONZE}.FF_LANDING_JSONL"
SAMPLE_ROWS = 200

UDF_PACKAGES = (
    "jsonschema",
    "jsonschema-specifications",
    "referencing",
    "rpds-py",
    "attrs",
    "rfc3339-validator",
    "isoduration",
    "fqdn",
    "idna",
    "jsonpointer",
    "uri-template",
    "webcolors",
    "rfc3987",
)

_DAY = re.compile(r"year=(\d{4})/month=(\d{2})/day=(\d{2})")
_TOPIC = re.compile(r"landing/kafka/([^/]+)/")
_IDENT = re.compile(r"^[A-Z][A-Z0-9_]*$")


def _quote(text: str) -> str:
    return text.replace("'", "''")


def _json_type(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int | float):
        return "number"
    return {dict: "object", list: "array", str: "string"}.get(type(value), type(value).__name__)


def describe_envelope(envelope: dict) -> dict:
    """Shape of one landing line (keys and JSON types), without its values."""
    payload = envelope.get("payload")
    headers = envelope.get("_kafka_headers")
    return {
        "envelope_keys": {k: _json_type(v) for k, v in envelope.items()},
        "header_types": {k: _json_type(v) for k, v in headers.items()} if isinstance(headers, dict) else None,
        "payload_keys": sorted(payload) if isinstance(payload, dict) else None,
    }


def summarize_landing(listed: list[dict]) -> dict[str, dict]:
    """Files, bytes and day range per topic, from LIST @LANDING rows (name, size, ...)."""
    topics: dict[str, dict] = defaultdict(lambda: {"files": 0, "bytes": 0, "first_day": None, "last_day": None})
    for row in listed:
        name = str(row.get("name", ""))
        match = _TOPIC.search(name)
        if not match:
            continue
        info = topics[match.group(1)]
        info["files"] += 1
        info["bytes"] += int(row.get("size") or 0)
        day_match = _DAY.search(name)
        if day_match:
            day = "-".join(day_match.groups())
            info["first_day"] = min(filter(None, (info["first_day"], day)))
            info["last_day"] = max(filter(None, (info["last_day"], day)))
    return dict(sorted(topics.items()))


def _version_key(version: str) -> tuple:
    return tuple(int(p) if p.isdigit() else 0 for p in re.split(r"[.+-]", version))


class Inspector:
    def __init__(self, cur):
        self.cur = cur
        self.errors: list[dict] = []

    def run(self, label: str, sql: str) -> list[dict] | None:
        """Rows as dicts keyed by lower-case column name; None (and an error entry) if the statement fails."""
        try:
            self.cur.execute(sql)
            names = [d[0].lower() for d in (self.cur.description or [])]
            return [dict(zip(names, row, strict=False)) for row in self.cur.fetchall() or []]
        except Exception as exc:  # recorded and reported at the end
            self.errors.append({"query": label, "error": " ".join(str(exc).split())[:500]})
            return None

    def bronze_objects(self) -> dict:
        tables = self.run("show tables", f"SHOW TABLES IN SCHEMA {BRONZE}") or []
        views = self.run("show views", f"SHOW VIEWS IN SCHEMA {BRONZE}") or []
        return {"tables": sorted(str(t["name"]) for t in tables), "views": sorted(str(v["name"]) for v in views)}

    def connector_tables(self) -> dict[str, dict]:
        tables = self.run("show connector tables", f"SHOW TABLES LIKE 'KAFKA%' IN SCHEMA {BRONZE}") or []
        result: dict[str, dict] = {}
        for table in sorted(tables, key=lambda t: t["name"]):
            name = str(table["name"])
            if not _IDENT.match(name):  # names come from SHOW; refuse anything that is not a plain identifier
                continue
            fq = f"{BRONZE}.{name}"
            sample = f"(SELECT RECORD_METADATA FROM {fq} LIMIT {SAMPLE_ROWS}) s"
            result[name] = {
                "owner": table.get("owner"),
                "change_tracking": table.get("change_tracking"),
                "row_count": table.get("rows"),
                "partitions": self.run(
                    f"{name} partitions",
                    f"""SELECT RECORD_METADATA:partition::INT AS kafka_partition, COUNT(*) AS record_count,
                           MIN(RECORD_METADATA:offset::NUMBER) AS min_offset,
                           MAX(RECORD_METADATA:offset::NUMBER) AS max_offset,
                           COUNT(DISTINCT RECORD_METADATA:offset::NUMBER) AS distinct_offsets,
                           MIN(RECORD_METADATA:CreateTime::NUMBER) AS min_create_time_ms,
                           MAX(RECORD_METADATA:CreateTime::NUMBER) AS max_create_time_ms
                    FROM {fq} GROUP BY 1 ORDER BY 1""",
                ),
                "record_metadata_keys": self.run(
                    f"{name} metadata keys",
                    f"""SELECT m.key AS meta_key, TYPEOF(m.value) AS value_type, COUNT(*) AS record_count,
                           ANY_VALUE(LEFT(TO_JSON(m.value), 80)) AS sample_value
                    FROM {sample}, LATERAL FLATTEN(input => s.RECORD_METADATA) m
                    WHERE m.key <> 'headers' GROUP BY 1, 2 ORDER BY 1, 2""",
                ),
                "headers": self.run(
                    f"{name} headers",
                    f"""SELECT h.key AS header_key, TYPEOF(h.value) AS value_type, COUNT(*) AS record_count,
                           ANY_VALUE(LEFT(TO_JSON(h.value), 80)) AS sample_value
                    FROM {sample}, LATERAL FLATTEN(input => s.RECORD_METADATA:headers) h
                    GROUP BY 1, 2 ORDER BY 1, 2""",
                ),
                "record_content_types": self.run(
                    f"{name} content types",
                    f"SELECT TYPEOF(RECORD_CONTENT) AS value_type, COUNT(*) AS record_count FROM {fq} GROUP BY 1",
                ),
            }
        return result

    def landing(self) -> dict:
        listed = self.run("list landing", f"LIST @{LANDING}/landing/kafka/") or []
        topics = summarize_landing(listed)
        for topic, info in topics.items():
            rows = self.run(
                f"{topic} envelope sample",
                f"""SELECT TO_JSON($1) AS envelope
                FROM @{LANDING}/landing/kafka/{_quote(topic)}/ (FILE_FORMAT => '{FILE_FORMAT}') LIMIT 1""",
            )
            info["sample"] = describe_envelope(json.loads(rows[0]["envelope"])) if rows else None
        return topics

    def generator_stage(self) -> list[dict]:
        listed = self.run("list generator", f"LIST @{GENERATOR}") or []
        return [{"name": r.get("name"), "size": r.get("size")} for r in sorted(listed, key=lambda r: r["name"])]

    def udf_packages(self) -> dict[str, dict[str, list[str]]]:
        names = ", ".join(f"'{p}'" for p in UDF_PACKAGES)
        rows = (
            self.run(
                "udf packages",
                f"""SELECT PACKAGE_NAME AS pkg_name, VERSION AS pkg_version, RUNTIME_VERSION AS python_runtime
                FROM {DATABASE}.INFORMATION_SCHEMA.PACKAGES
                WHERE LANGUAGE = 'python' AND PACKAGE_NAME IN ({names})""",
            )
            or []
        )
        found: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
        for row in rows:
            found[str(row["pkg_name"])][str(row.get("python_runtime") or "any")].add(str(row["pkg_version"]))
        return {
            package: {rt: sorted(v, key=_version_key)[-3:] for rt, v in sorted(found.get(package, {}).items())}
            for package in UDF_PACKAGES
        }

    def inspect(self) -> dict:
        result = {
            "bronze_objects": self.bronze_objects(),
            "connector_tables": self.connector_tables(),
            "landing": self.landing(),
            "generator_stage": self.generator_stage(),
            "udf_packages": self.udf_packages(),
        }
        result["errors"] = self.errors
        return result


def inspect(cur) -> dict:
    return Inspector(cur).inspect()


def run() -> dict:
    from veloparc_gen.snowflake_conn import connect

    with connect() as conn, conn.cursor() as cur:
        return inspect(cur)
