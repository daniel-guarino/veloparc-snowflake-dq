"""Read-only inspection of what is already in Snowflake, before writing the bronze layer (Phase 6).

Answers the questions the bronze mapping depends on, from the real objects rather than from the docs:

- connector tables (KAFKA_*): owner, change tracking, rows and offsets per partition, the keys of RECORD_METADATA
  and the type each header arrives with (the default Kafka Connect header converter may turn "1" into a number);
- landing stage: files, bytes and day range per topic, plus the shape of one envelope per topic;
- generator stage: the run manifests, reference tables and state files;
- Python packages available to UDFs (jsonschema and the packages its format checks need).

Only SHOW, LIST and SELECT statements are run. Payload values are fictitious; no account identifiers are printed.
"""

# ruff: noqa: S608  -- identifiers are module constants, or table names from SHOW checked against _IDENT

from __future__ import annotations

import re
from collections import defaultdict

DATABASE = "VLP_DP"
BRONZE = f"{DATABASE}.BRONZE"
LANDING = f"{BRONZE}.LANDING"
GENERATOR = f"{BRONZE}.GENERATOR"
FILE_FORMAT = f"{BRONZE}.FF_LANDING_JSONL"
HEADER_SAMPLE_ROWS = 200

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


def _rows(cur) -> list[dict]:
    """Rows of the last statement as dicts keyed by lower-case column name."""
    names = [d[0].lower() for d in (cur.description or [])]
    return [dict(zip(names, row, strict=False)) for row in cur.fetchall() or []]


def _run(cur, sql: str) -> list[dict]:
    cur.execute(sql)
    return _rows(cur)


def _quote(text: str) -> str:
    return text.replace("'", "''")


def connector_tables(cur) -> dict[str, dict]:
    tables = _run(cur, f"SHOW TABLES LIKE 'KAFKA%' IN SCHEMA {BRONZE}")
    result: dict[str, dict] = {}
    for table in sorted(tables, key=lambda t: t["name"]):
        name = str(table["name"])
        if not _IDENT.match(name):  # names come from SHOW; refuse anything that is not a plain identifier
            continue
        fq = f"{BRONZE}.{name}"
        partitions = _run(
            cur,
            f"""SELECT RECORD_METADATA:partition::INT AS partition, COUNT(*) AS rows,
                   MIN(RECORD_METADATA:offset::NUMBER) AS min_offset,
                   MAX(RECORD_METADATA:offset::NUMBER) AS max_offset,
                   COUNT(DISTINCT RECORD_METADATA:offset::NUMBER) AS distinct_offsets,
                   MIN(RECORD_METADATA:CreateTime::NUMBER) AS min_create_time_ms,
                   MAX(RECORD_METADATA:CreateTime::NUMBER) AS max_create_time_ms
            FROM {fq} GROUP BY 1 ORDER BY 1""",
        )
        metadata_keys = _run(
            cur,
            f"""SELECT m.key AS key, TYPEOF(m.value) AS type, COUNT(*) AS n,
                   ANY_VALUE(LEFT(m.value::VARCHAR, 80)) AS example
            FROM (SELECT RECORD_METADATA FROM {fq} LIMIT {HEADER_SAMPLE_ROWS}) s,
                 LATERAL FLATTEN(input => s.RECORD_METADATA) m
            WHERE m.key <> 'headers' GROUP BY 1, 2 ORDER BY 1, 2""",
        )
        headers = _run(
            cur,
            f"""SELECT h.key AS key, TYPEOF(h.value) AS type, COUNT(*) AS n,
                   ANY_VALUE(LEFT(h.value::VARCHAR, 80)) AS example
            FROM (SELECT RECORD_METADATA FROM {fq} LIMIT {HEADER_SAMPLE_ROWS}) s,
                 LATERAL FLATTEN(input => s.RECORD_METADATA:headers) h
            GROUP BY 1, 2 ORDER BY 1, 2""",
        )
        content = _run(cur, f"SELECT TYPEOF(RECORD_CONTENT) AS type, COUNT(*) AS n FROM {fq} GROUP BY 1 ORDER BY 1")
        result[name] = {
            "owner": table.get("owner"),
            "change_tracking": table.get("change_tracking"),
            "rows": table.get("rows"),
            "partitions": partitions,
            "record_metadata_keys": metadata_keys,
            "headers": headers,
            "record_content_types": content,
        }
    return result


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


def landing(cur) -> dict:
    topics = summarize_landing(_run(cur, f"LIST @{LANDING}/landing/kafka/"))
    for topic, info in topics.items():
        sample = _run(
            cur,
            f"""SELECT OBJECT_KEYS($1) AS envelope_keys, TYPEOF($1:payload) AS payload_type,
                   TYPEOF($1:_kafka_timestamp) AS timestamp_type, TYPEOF($1:_kafka_offset) AS offset_type,
                   OBJECT_KEYS($1:_kafka_headers) AS header_keys,
                   IFF(IS_OBJECT($1:payload), OBJECT_KEYS($1:payload), NULL) AS payload_keys
            FROM @{LANDING}/landing/kafka/{_quote(topic)}/ (FILE_FORMAT => '{FILE_FORMAT}') LIMIT 1""",
        )
        info["sample"] = sample[0] if sample else None
    return topics


def generator_stage(cur) -> list[dict]:
    listed = _run(cur, f"LIST @{GENERATOR}")
    return [{"name": row.get("name"), "size": row.get("size")} for row in sorted(listed, key=lambda r: r["name"])]


def _version_key(version: str) -> tuple:
    return tuple(int(p) if p.isdigit() else 0 for p in re.split(r"[.+-]", version))


def udf_packages(cur) -> dict[str, dict[str, list[str]]]:
    names = ", ".join(f"'{p}'" for p in UDF_PACKAGES)
    rows = _run(
        cur,
        f"""SELECT PACKAGE_NAME AS package, VERSION AS version, RUNTIME_VERSION AS runtime
        FROM {DATABASE}.INFORMATION_SCHEMA.PACKAGES WHERE LANGUAGE = 'python' AND PACKAGE_NAME IN ({names})""",
    )
    found: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in rows:
        found[str(row["package"])][str(row.get("runtime") or "any")].add(str(row["version"]))
    result: dict[str, dict[str, list[str]]] = {}
    for package in UDF_PACKAGES:
        runtimes = found.get(package, {})
        result[package] = {rt: sorted(v, key=_version_key)[-3:] for rt, v in sorted(runtimes.items())}
    return result


def bronze_objects(cur) -> dict[str, list[str]]:
    tables = _run(cur, f"SHOW TABLES IN SCHEMA {BRONZE}")
    views = _run(cur, f"SHOW VIEWS IN SCHEMA {BRONZE}")
    return {"tables": sorted(str(t["name"]) for t in tables), "views": sorted(str(v["name"]) for v in views)}


def inspect(cur) -> dict:
    return {
        "bronze_objects": bronze_objects(cur),
        "connector_tables": connector_tables(cur),
        "landing": landing(cur),
        "generator_stage": generator_stage(cur),
        "udf_packages": udf_packages(cur),
    }


def run() -> dict:
    from veloparc_gen.snowflake_conn import connect

    with connect() as conn, conn.cursor() as cur:
        return inspect(cur)
