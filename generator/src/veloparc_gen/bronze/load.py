"""File loader (ADR 0004): COPY INTO each RAW_* table from its @BRONZE.LANDING folder, audited in LOAD_AUDIT.

COPY's own load metadata makes a re-run skip the files it already loaded (same name and checksum), so the
command is safe to repeat. ON_ERROR = CONTINUE: a row that cannot be parsed is skipped and counted, never the
whole file; the spec's expressions cannot raise, so errors here mean a malformed line in a landing file.

`status()` is read-only: rows per RAW_* table by path, the connector tasks' state and whether their streams
still hold rows.
"""

# ruff: noqa: S608  -- table, stream and topic names come from the spec, never from input

from __future__ import annotations

import os
import uuid
from collections import Counter
from datetime import UTC, datetime

from veloparc_gen.bronze import spec

AUDIT_TABLE = f"{spec.BRONZE}.LOAD_AUDIT"
_AUDIT_COLUMNS = (
    "LOAD_ID",
    "LOADER",
    "TARGET_TABLE",
    "KAFKA_TOPIC",
    "QUERY_ID",
    "STARTED_AT",
    "FILES_LOADED",
    "FILES_PARTIALLY_LOADED",
    "FILES_FAILED",
    "ROWS_PARSED",
    "ROWS_LOADED",
    "ERRORS_SEEN",
    "FIRST_ERROR",
    "FIRST_ERROR_FILE",
    "GITHUB_RUN",
)


class LoadErrors(RuntimeError):
    pass


def _rows(cur) -> list[dict]:
    names = [d[0].lower() for d in (cur.description or [])]
    return [dict(zip(names, row, strict=False)) for row in cur.fetchall() or []]


def summarize_copy(rows: list[dict]) -> dict:
    """Totals from the rows COPY INTO returns (one per file, or a single 'status' row when nothing was new)."""
    statuses: Counter[str] = Counter()
    totals = {"rows_parsed": 0, "rows_loaded": 0, "errors_seen": 0}
    first_error = first_error_file = None
    for row in rows:
        if "file" not in row:  # "Copy executed with 0 files processed."
            continue
        statuses[str(row.get("status", "")).upper()] += 1
        for key in totals:
            totals[key] += int(row.get(key) or 0)
        if row.get("first_error") and first_error is None:
            first_error, first_error_file = str(row["first_error"])[:1000], row["file"]
    return {
        "files_loaded": statuses.get("LOADED", 0),
        "files_partially_loaded": statuses.get("PARTIALLY_LOADED", 0),
        "files_failed": statuses.get("LOAD_FAILED", 0),
        **totals,
        "first_error": first_error,
        "first_error_file": first_error_file,
    }


def github_run(env: dict[str, str] | None = None) -> str | None:
    env = dict(os.environ if env is None else env)
    run_id, sha = env.get("GITHUB_RUN_ID"), env.get("GITHUB_SHA", "")[:12]
    return f"{run_id}@{sha}" if run_id else None


def _audit_sql() -> str:
    placeholders = ", ".join(["%s"] * len(_AUDIT_COLUMNS))
    return f"INSERT INTO {AUDIT_TABLE} ({', '.join(_AUDIT_COLUMNS)}) VALUES ({placeholders})"


def _copy_and_audit(cur, sql: str, loader: str, table: str, topic: str | None, load_id: str, run: str | None) -> dict:
    started = datetime.now(UTC)
    cur.execute(sql)
    query_id = getattr(cur, "sfqid", None)
    summary = summarize_copy(_rows(cur))
    cur.execute(
        _audit_sql(),
        (
            load_id,
            loader,
            table,
            topic,
            query_id,
            started.isoformat(),
            summary["files_loaded"],
            summary["files_partially_loaded"],
            summary["files_failed"],
            summary["rows_parsed"],
            summary["rows_loaded"],
            summary["errors_seen"],
            summary["first_error"],
            summary["first_error_file"],
            run,
        ),
    )
    return {"table": table, "query_id": query_id, **summary}


def load_topic(cur, topic: spec.BronzeTopic, load_id: str, run: str | None) -> dict:
    return _copy_and_audit(cur, spec.copy_sql(topic), "copy", topic.table, topic.topic, load_id, run)


# --------------------------------------------------------------------------- reference tables and manifests

GENERATOR_STAGE = f"{spec.BRONZE}.GENERATOR"
REFERENCE_FORMAT = f"{spec.BRONZE}.FF_REFERENCE_CSV"
MANIFEST_FORMAT = f"{spec.BRONZE}.FF_GENERATOR_JSON"

# Table -> (column, expression over the CSV fields), in the order the generator writes them.
REFERENCE_TABLES = {
    "FX_RATE_DAILY": (
        ("RATE_DATE", "TRY_TO_DATE($1)"),
        ("CURRENCY", "$2"),
        ("RATE_TO_EUR", "TRY_CAST($3 AS NUMBER(18, 6))"),
    ),
    "POSTAL_ZONE": (("COUNTRY_CODE", "$1"), ("POSTAL_PREFIX", "$2"), ("ZONE_CODE", "$3")),
    "COUNTRY_ALIAS": (("ALIAS_NORM", "$1"), ("ISO2", "$2")),
}

_RUN_ID = "REGEXP_SUBSTR(METADATA$FILENAME, 'runs/([^/]+)/', 1, 1, 'e')"
# Table -> (file name pattern, (column, expression over the JSON document)).
MANIFEST_TABLES = {
    "GENERATOR_RUNS": (
        "run_manifest[.]json",
        (("RUN_ID", _RUN_ID), ("MANIFEST", "$1"), ("_FILE_NAME", "METADATA$FILENAME")),
    ),
    "ANOMALY_GROUND_TRUTH": (
        "anomaly_manifest[.]jsonl",
        (
            ("RUN_ID", _RUN_ID),
            ("ANOMALY_ID", "$1:anomaly_id::VARCHAR"),
            ("ANOMALY_TYPE", "$1:anomaly_type::VARCHAR"),
            ("RULE_ID", "$1:rule::VARCHAR"),
            ("DQ_DIMENSION", "$1:dq_dimension::VARCHAR"),
            ("SOURCE", "$1:source::VARCHAR"),
            ("BUSINESS_KEY", "$1:business_key::VARCHAR"),
            ("COUNTRY", "$1:country::VARCHAR"),
            ("EVENT_DATE", "TRY_TO_DATE($1:event_date::VARCHAR)"),
            ("SCHEMA_ANOMALY", "TRY_TO_BOOLEAN($1:schema_anomaly::VARCHAR)"),
            ("ENTRY", "$1"),
            ("_FILE_NAME", "METADATA$FILENAME"),
            ("_FILE_ROW_NUMBER", "METADATA$FILE_ROW_NUMBER"),
        ),
    ),
    "GENERATOR_RULE_VIOLATIONS": (
        "rule_violations[.]jsonl",
        (
            ("RUN_ID", _RUN_ID),
            ("RULE_ID", "$1:rule::VARCHAR"),
            ("SOURCE", "$1:source::VARCHAR"),
            ("BUSINESS_KEY", "$1:key::VARCHAR"),
            ("MESSAGE", "$1:message::VARCHAR"),
            ("ENTRY", "$1"),
            ("_FILE_NAME", "METADATA$FILENAME"),
            ("_FILE_ROW_NUMBER", "METADATA$FILE_ROW_NUMBER"),
        ),
    ),
}


def _copy_select(target: str, columns, source: str, file_format: str, extra: str) -> str:
    names = ", ".join(c for c, _ in columns)
    exprs = ", ".join(e for _, e in columns)
    return (
        f"COPY INTO {target} ({names})\nFROM (SELECT {exprs} FROM {source})\n"
        f"FILE_FORMAT = (FORMAT_NAME = '{file_format}')\n{extra}"
    )


def reference_copy_sql(table: str) -> str:
    """Full replace: the caller deletes the rows first; FORCE reloads the unchanged file."""
    return _copy_select(
        f"{spec.DATABASE}.REF.{table}",
        REFERENCE_TABLES[table],
        f"@{GENERATOR_STAGE}/reference/{table}/",
        REFERENCE_FORMAT,
        "FORCE = TRUE\nON_ERROR = ABORT_STATEMENT",
    )


def manifest_copy_sql(table: str) -> str:
    """Append: COPY skips manifest files already loaded, so each run is loaded once."""
    pattern, columns = MANIFEST_TABLES[table]
    return _copy_select(
        f"{spec.DATABASE}.DQ.{table}",
        columns,
        f"@{GENERATOR_STAGE}/runs/",
        MANIFEST_FORMAT,
        f"PATTERN = '.*{pattern}'\nON_ERROR = ABORT_STATEMENT",
    )


def load_reference(cur, load_id: str, run: str | None) -> list[dict]:
    results = []
    for table in REFERENCE_TABLES:
        cur.execute("BEGIN")
        try:
            cur.execute(f"DELETE FROM {spec.DATABASE}.REF.{table}")
            result = _copy_and_audit(
                cur, reference_copy_sql(table), "copy-reference", f"REF.{table}", None, load_id, run
            )
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        results.append(result)
    return results


def load_manifests(cur, load_id: str, run: str | None) -> list[dict]:
    return [
        _copy_and_audit(cur, manifest_copy_sql(table), "copy-manifest", f"DQ.{table}", None, load_id, run)
        for table in MANIFEST_TABLES
    ]


def select_topics(names: list[str] | None) -> list[spec.BronzeTopic]:
    if not names:
        return list(spec.TOPICS.values())
    unknown = sorted(set(names) - set(spec.TOPICS))
    if unknown:
        raise ValueError(f"unknown topics {unknown}; choose from {sorted(spec.TOPICS)}")
    return [spec.TOPICS[n] for n in names]


def load(cur, names: list[str] | None = None, log=print, env: dict[str, str] | None = None) -> dict:
    load_id, run = str(uuid.uuid4()), github_run(env)
    results = []
    for topic in select_topics(names):
        log(f"COPY {topic.topic} -> {topic.table}")
        result = load_topic(cur, topic, load_id, run)
        log(
            f"  files loaded {result['files_loaded']}, partial {result['files_partially_loaded']}, "
            f"failed {result['files_failed']}; rows loaded {result['rows_loaded']}, errors {result['errors_seen']}"
        )
        results.append(result)
    if names is None:  # a full load also refreshes the reference tables and appends new generator manifests
        log("reference tables -> REF, generator manifests -> DQ")
        results += load_reference(cur, load_id, run) + load_manifests(cur, load_id, run)
    return {"load_id": load_id, "github_run": run, "topics": results}


def problems(result: dict) -> list[str]:
    return [
        f"{r['table']}: {r['errors_seen']} row error(s), {r['files_failed']} failed file(s); first: {r['first_error']}"
        for r in result["topics"]
        if r["errors_seen"] or r["files_failed"]
    ]


def status(cur) -> dict:
    tables = {}
    for topic in spec.TOPICS.values():
        cur.execute(
            f"SELECT COUNT(*) AS total_rows, COUNT(_FILE_NAME) AS file_rows, "
            f"COUNT(*) - COUNT(_FILE_NAME) AS connector_rows, MAX(_INSERTED_AT) AS last_inserted_at "
            f"FROM {spec.BRONZE}.{topic.table}"
        )
        tables[topic.table] = _rows(cur)[0]
    for table in [f"REF.{t}" for t in REFERENCE_TABLES] + [f"DQ.{t}" for t in MANIFEST_TABLES]:
        cur.execute(f"SELECT COUNT(*) AS total_rows FROM {spec.DATABASE}.{table}")
        tables[table] = _rows(cur)[0]
    cur.execute(f"SHOW TASKS LIKE 'TASK_LOAD_RAW%' IN SCHEMA {spec.BRONZE}")
    tasks = {str(t["name"]): t.get("state") for t in _rows(cur)}
    streams = {}
    for topic in spec.TOPICS.values():
        if topic.connector_table:
            cur.execute(f"SELECT SYSTEM$STREAM_HAS_DATA('{spec.stream_name(topic)}') AS has_data")
            streams[spec.stream_name(topic)] = _rows(cur)[0]["has_data"]
    return {"tables": tables, "connector_tasks": tasks, "streams_pending": streams}


def run(names: list[str] | None = None, log=print) -> dict:
    from veloparc_gen.snowflake_conn import connect

    with connect() as conn, conn.cursor() as cur:
        result = load(cur, names, log)
        result["status"] = status(cur)
    return result


def run_status() -> dict:
    from veloparc_gen.snowflake_conn import connect

    with connect() as conn, conn.cursor() as cur:
        return status(cur)
