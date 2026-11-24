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


def load_topic(cur, topic: spec.BronzeTopic, load_id: str, run: str | None) -> dict:
    started = datetime.now(UTC)
    cur.execute(spec.copy_sql(topic))
    query_id = getattr(cur, "sfqid", None)
    summary = summarize_copy(_rows(cur))
    cur.execute(
        _audit_sql(),
        (
            load_id,
            "copy",
            topic.table,
            topic.topic,
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
    return {"table": topic.table, "query_id": query_id, **summary}


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
