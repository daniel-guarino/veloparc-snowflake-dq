"""Read-only preview of the bronze loaders on the data already in Snowflake, before any table is created.

For each topic and each load path, runs the loader's SELECT (the same expressions COPY and the connector task will
use) on a sample, and reports rows, non-NULL counts per column and one sample row (no payload, no headers).
A column that is NULL where the source has a value shows a mapping problem before it reaches a migration.
Only SELECT statements; failures are collected under "errors" (see snowflake_inspect.Inspector).
"""

from __future__ import annotations

from veloparc_gen.bronze import spec
from veloparc_gen.snowflake_inspect import Inspector

SAMPLE_ROWS = 2000
_SKIP_IN_SAMPLE = {"RAW_PAYLOAD", "_KAFKA_HEADERS"}


def sources(topic: spec.BronzeTopic) -> list[tuple[spec.LoadPath, str]]:
    found = [(spec.FILE, f"{spec.landing_location(topic)} (FILE_FORMAT => '{spec.FILE_FORMAT}')")]
    if topic.connector_table:
        found.append((spec.CONNECTOR, f"{spec.BRONZE}.{topic.connector_table}"))
    return found


def profile_sql(topic: spec.BronzeTopic, path: spec.LoadPath, source: str, limit: int = SAMPLE_ROWS) -> str:
    columns = [name for name, _ in topic.column_exprs(path)]
    counts = ",\n    ".join(f"COUNT({c}) AS nn_{c.lstrip('_').lower()}" for c in columns)
    inner = spec.select_sql(topic, path, source).replace("\n", "\n    ")
    return f"SELECT\n    COUNT(*) AS sampled_rows,\n    {counts}\nFROM (\n    {inner}\n    LIMIT {limit}\n) s"


def sample_sql(topic: spec.BronzeTopic, path: spec.LoadPath, source: str) -> str:
    columns = [name for name, _ in topic.column_exprs(path) if name not in _SKIP_IN_SAMPLE]
    inner = spec.select_sql(topic, path, source).replace("\n", "\n    ")
    return f"SELECT {', '.join(columns)}\nFROM (\n    {inner}\n    LIMIT 1\n) s"


def preview(cur, limit: int = SAMPLE_ROWS) -> dict:
    runner = Inspector(cur)
    result: dict[str, dict] = {}
    for topic in spec.TOPICS.values():
        per_path: dict[str, dict] = {}
        for path, source in sources(topic):
            label = f"{topic.table} {path.name}"
            profile = runner.run(f"{label} profile", profile_sql(topic, path, source, limit))
            sample = runner.run(f"{label} sample", sample_sql(topic, path, source))
            counts = profile[0] if profile else None
            per_path[path.name] = {
                "sampled_rows": counts.pop("sampled_rows") if counts else None,
                "non_null": counts,
                "always_null": sorted(k for k, v in (counts or {}).items() if not v),
                "sample": sample[0] if sample else None,
            }
        result[topic.table] = {"topic": topic.topic, "paths": per_path}
    return {"topics": result, "errors": runner.errors}


def run(limit: int = SAMPLE_ROWS) -> dict:
    from veloparc_gen.snowflake_conn import connect

    with connect() as conn, conn.cursor() as cur:
        return preview(cur, limit)
