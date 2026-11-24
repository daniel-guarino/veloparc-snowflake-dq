"""File loader (COPY + audit) and connector tasks script (fake cursor; no Snowflake)."""

import pytest

from veloparc_gen.bronze import load, spec

FILE_COLS = ["file", "status", "rows_parsed", "rows_loaded", "error_limit", "errors_seen", "first_error"]


class FakeCursor:
    def __init__(self, copy_rows=None):
        self.copy_rows = copy_rows
        self.calls: list[tuple[str, tuple | None]] = []
        self.description, self._rows, self.sfqid = [], [], None

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        self.sfqid = f"q{len(self.calls)}"
        if sql.startswith("COPY INTO"):
            if self.copy_rows is None:
                self.description, self._rows = [("status",)], [("Copy executed with 0 files processed.",)]
            else:
                self.description = [(c.upper(),) for c in FILE_COLS]
                self._rows = self.copy_rows
        elif "COUNT(*) AS total_rows" in sql:
            self.description = [("TOTAL_ROWS",), ("FILE_ROWS",), ("CONNECTOR_ROWS",), ("LAST_INSERTED_AT",)]
            self._rows = [(10, 7, 3, None)]
        elif sql.startswith("SHOW TASKS"):
            self.description = [("name",), ("state",)]
            self._rows = [("TASK_LOAD_RAW_TRACKING_EVENTS", "started")]
        elif "SYSTEM$STREAM_HAS_DATA" in sql:
            self.description, self._rows = [("HAS_DATA",)], [(False,)]
        else:
            self.description, self._rows = [], []

    def fetchall(self):
        return self._rows


def test_summarize_copy_counts_statuses_rows_and_first_error():
    rows = [
        {"file": "a", "status": "LOADED", "rows_parsed": 10, "rows_loaded": 10, "errors_seen": 0},
        {"file": "b", "status": "PARTIALLY_LOADED", "rows_parsed": 5, "rows_loaded": 4, "errors_seen": 1,
         "first_error": "Error parsing JSON"},
        {"file": "c", "status": "LOAD_FAILED", "rows_parsed": 0, "rows_loaded": 0, "errors_seen": 1,
         "first_error": "later"},
    ]  # fmt: skip
    summary = load.summarize_copy(rows)
    assert summary == {
        "files_loaded": 1,
        "files_partially_loaded": 1,
        "files_failed": 1,
        "rows_parsed": 15,
        "rows_loaded": 14,
        "errors_seen": 2,
        "first_error": "Error parsing JSON",
        "first_error_file": "b",
    }


def test_summarize_copy_when_nothing_was_new():
    summary = load.summarize_copy([{"status": "Copy executed with 0 files processed."}])
    assert summary["files_loaded"] == summary["rows_loaded"] == summary["errors_seen"] == 0


def test_load_runs_one_copy_and_one_audit_row_per_topic():
    cur = FakeCursor(copy_rows=[("f1", "LOADED", 3, 3, 1, 0, None)])
    result = load.load(cur, log=lambda *_: None, env={"GITHUB_RUN_ID": "42", "GITHUB_SHA": "abcdef1234567890"})
    copies = [sql for sql, _ in cur.calls if sql.startswith("COPY INTO VLP_DP.BRONZE.RAW_")]
    audits = [p for sql, p in cur.calls if sql.startswith("INSERT INTO VLP_DP.BRONZE.LOAD_AUDIT") and p[1] == "copy"]
    assert len(copies) == len(audits) == len(spec.TOPICS) == 8
    first = audits[0]
    assert first[1:5] == ("copy", "RAW_VELOSHIP_SHIPMENTS", "vlp.shipping.veloship-shipment.v1", "q1")
    assert first[-1] == "42@abcdef123456"
    assert first[6:12] == (1, 0, 0, 3, 3, 0)
    assert all(a[0] == result["load_id"] for a in audits)
    assert load.problems(result) == []


def test_audit_statement_binds_every_value():
    sql = load._audit_sql()
    assert sql.count("%s") == len(load._AUDIT_COLUMNS) == 15
    assert "'" not in sql  # values only through bind parameters


def test_problems_flag_row_errors_and_failed_files():
    cur = FakeCursor(copy_rows=[("f1", "PARTIALLY_LOADED", 3, 2, 1, 1, "bad line")])
    result = load.load(cur, ["tracking_event"], log=lambda *_: None, env={})
    assert result["github_run"] is None
    assert load.problems(result) == ["RAW_TRACKING_EVENTS: 1 row error(s), 0 failed file(s); first: bad line"]


def test_select_topics_by_key_and_rejects_unknown_ones():
    assert [t.table for t in load.select_topics(["invoice", "gl_extract"])] == ["RAW_INVOICES", "RAW_GL_EXTRACT"]
    with pytest.raises(ValueError, match="unknown topics"):
        load.select_topics(["dlq"])


def test_status_reports_rows_tasks_and_streams():
    result = load.status(FakeCursor())
    assert result["tables"]["RAW_TRACKING_EVENTS"]["connector_rows"] == 3
    assert result["connector_tasks"] == {"TASK_LOAD_RAW_TRACKING_EVENTS": "started"}
    assert set(result["streams_pending"]) == {
        "VLP_DP.BRONZE.KAFKA_VELOSHIP_SHIPMENTS_STREAM",
        "VLP_DP.BRONZE.KAFKA_TRACKING_EVENTS_STREAM",
        "VLP_DP.BRONZE.KAFKA_INVOICES_STREAM",
        "VLP_DP.BRONZE.KAFKA_CRM_OBJECTS_STREAM",
    }


# --------------------------------------------------------------------------- connector tasks script


def test_connector_tasks_cover_the_four_kafka_topics_in_a_safe_order():
    script = spec.connector_tasks_script()
    for topic in (t for t in spec.TOPICS.values() if t.connector_table):
        block = spec.connector_task_sql(topic)
        assert block in script
        order = [
            f"ALTER TABLE VLP_DP.BRONZE.{topic.connector_table} SET CHANGE_TRACKING = TRUE;",
            f"CREATE STREAM IF NOT EXISTS {spec.stream_name(topic)}",  # never replaced: no replay
            "APPEND_ONLY = TRUE SHOW_INITIAL_ROWS = TRUE",
            f"ALTER TASK IF EXISTS {spec.task_name(topic)} SUSPEND;",
            f"CREATE OR REPLACE TASK {spec.task_name(topic)}",
            "WAREHOUSE = WH_LOAD",
            f"WHEN SYSTEM$STREAM_HAS_DATA('{spec.stream_name(topic)}')",
            f"INSERT INTO VLP_DP.BRONZE.{topic.table} (",
            f"FROM {spec.stream_name(topic)};",
            f"ALTER TASK {spec.task_name(topic)} RESUME;",
        ]
        positions = [block.index(fragment) for fragment in order]
        assert positions == sorted(positions), topic.source
        assert "SCHEDULE" not in block  # triggered task
    assert "CREATE OR REPLACE STREAM" not in script
    assert script.count("CREATE OR REPLACE TASK") == 4


def test_connector_insert_reads_the_stream_not_the_table():
    topic = spec.TOPICS["tracking_event"]
    sql = spec.connector_task_sql(topic)
    insert = sql[sql.index("INSERT INTO") :]
    assert "FROM VLP_DP.BRONZE.KAFKA_TRACKING_EVENTS_STREAM;" in insert
    assert "FROM VLP_DP.BRONZE.KAFKA_TRACKING_EVENTS;" not in insert


def test_file_only_topics_have_no_task():
    with pytest.raises(ValueError, match="no connector table"):
        spec.connector_insert_sql(spec.TOPICS["tariff"], "X")


# --------------------------------------------------------------------------- reference tables and manifests

COPY_FUNCTIONS = {"TRY_TO_DATE", "TRY_CAST", "TRY_TO_BOOLEAN", "REGEXP_SUBSTR", "NUMBER"}


def _functions(sql):
    import re

    select = re.sub(r"'(?:[^']|'')*'", "''", sql)
    select = select[select.index("SELECT") : select.index(" FROM @")]
    return set(re.findall(r"\b([A-Z_][A-Z0-9_]*)\s*\(", select))


@pytest.mark.parametrize("table", sorted(load.REFERENCE_TABLES))
def test_reference_copy_replaces_the_table_from_its_csv(table):
    sql = load.reference_copy_sql(table)
    assert sql.startswith(f"COPY INTO VLP_DP.REF.{table} (")
    assert f"FROM @VLP_DP.BRONZE.GENERATOR/reference/{table}/)" in sql
    assert "FORCE = TRUE" in sql and "ON_ERROR = ABORT_STATEMENT" in sql
    assert _functions(sql) <= COPY_FUNCTIONS


@pytest.mark.parametrize("table", sorted(load.MANIFEST_TABLES))
def test_manifest_copy_appends_with_run_id_from_the_path(table):
    sql = load.manifest_copy_sql(table)
    assert sql.startswith(f"COPY INTO VLP_DP.DQ.{table} (RUN_ID,")
    assert "REGEXP_SUBSTR(METADATA$FILENAME, 'runs/([^/]+)/', 1, 1, 'e')" in sql
    assert "FORCE" not in sql  # files already loaded are skipped
    assert _functions(sql) <= COPY_FUNCTIONS


def test_manifest_fields_match_what_the_generator_writes():
    import dataclasses

    from veloparc_gen.rules import Violation

    fields = {f.name for f in dataclasses.fields(Violation)}
    exprs = dict(load.MANIFEST_TABLES["GENERATOR_RULE_VIOLATIONS"][1])
    assert {"rule", "source", "key", "message"} <= fields
    assert exprs["BUSINESS_KEY"] == "$1:key::VARCHAR"


def test_reference_columns_follow_the_generator_csv_header_order():

    from veloparc_gen.sources import reference

    class World:
        def __init__(self):
            self.reference, self.postal_zones = {}, [("PT", "10", "Z1")]

    world = World()
    reference.build_tables(world)
    assert list(world.reference["POSTAL_ZONE"][0]) == ["country_code", "postal_prefix", "zone_code"]
    assert [c.lower() for c, _ in load.REFERENCE_TABLES["POSTAL_ZONE"]] == list(world.reference["POSTAL_ZONE"][0])
    assert [c.lower() for c, _ in load.REFERENCE_TABLES["COUNTRY_ALIAS"]] == list(world.reference["COUNTRY_ALIAS"][0])


def test_full_load_refreshes_reference_in_a_transaction_and_appends_manifests():
    cur = FakeCursor(copy_rows=[("f", "LOADED", 1, 1, 1, 0, None)])
    load.load(cur, log=lambda *_: None, env={})
    sql = [s for s, _ in cur.calls]
    i = sql.index("DELETE FROM VLP_DP.REF.FX_RATE_DAILY")
    assert sql[i - 1] == "BEGIN" and sql[i + 1].startswith("COPY INTO VLP_DP.REF.FX_RATE_DAILY")
    assert sql[i + 3] == "COMMIT"  # COPY, audit insert, commit
    assert sum(s.startswith("COPY INTO VLP_DP.DQ.") for s in sql) == 3


def test_topic_subset_does_not_touch_reference_or_manifests():
    cur = FakeCursor(copy_rows=[("f", "LOADED", 1, 1, 1, 0, None)])
    load.load(cur, ["invoice"], log=lambda *_: None, env={})
    assert not any("REF." in s or "DQ." in s for s, _ in cur.calls)


def test_reference_copy_failure_rolls_back_the_delete():
    class Failing(FakeCursor):
        def execute(self, sql, params=None):
            if sql.startswith("COPY INTO VLP_DP.REF."):
                self.calls.append((sql, params))
                raise RuntimeError("100038: numeric value not recognized")
            super().execute(sql, params)

    cur = Failing()
    with pytest.raises(RuntimeError):
        load.load_reference(cur, "id", None)
    assert [s for s, _ in cur.calls][-1] == "ROLLBACK"


# --------------------------------------------------------------------------- views and session time zone


def test_views_flag_duplicates_for_every_raw_table_and_gl_completeness():
    script = spec.views_script()
    for topic in spec.TOPICS.values():
        assert f"CREATE OR REPLACE VIEW VLP_DP.BRONZE.V_{topic.table}" in script
        assert f"GRANT SELECT ON VIEW VLP_DP.BRONZE.V_{topic.table} TO ROLE VLP_TRANSFORMER;" in script
    assert "PARTITION BY _KAFKA_TOPIC, _KAFKA_PARTITION, _KAFKA_OFFSET, _EVENT_ID" in script
    assert "_EVENT_ID IS NOT NULL AND _TECH_RN > 1 AS _IS_TECH_DUP" in script
    assert "AS _IS_RESEND" in script
    assert "COUNT(DISTINCT _SOURCE_TXN_SEQ) = MAX(_SOURCE_TXN_COUNT) AS IS_COMPLETE" in script
    assert "WHERE NOT _IS_TECH_DUP AND NOT _IS_RESEND" in script


def test_dedup_flags_match_a_python_model_of_the_rules():
    """Same rules as the view, on rows: (topic, partition, offset, ce_id, inserted_at, kafka_ts)."""
    rows = [
        ("t", 0, 5, "a", 1, 10),  # first
        ("t", 0, 5, "a", 2, 10),  # loaded again -> tech dup
        ("t", 1, 9, "a", 1, 11),  # same ce_id later at another offset -> resend
        ("t", 0, 5, "b", 1, 12),  # same offset, other ce_id (backfill vs broker overlap) -> kept
        ("t", 0, 6, None, 1, 13),  # no ce_id -> never flagged
    ]

    def flags(row):
        topic, part, off, ce, _ins, _ = row
        if ce is None:
            return (False, False)
        same = sorted(r for r in rows if r[:4] == row[:4])
        tech = sorted(same, key=lambda r: r[4]).index(row) > 0
        first = min((r for r in rows if r[0] == topic and r[3] == ce), key=lambda r: (r[5], r[1], r[2], r[4]))
        return (tech, (part, off) != (first[1], first[2]))

    assert [flags(r) for r in rows] == [(False, False), (True, False), (False, True), (False, False), (False, False)]


def test_tasks_and_sessions_run_in_utc():
    assert spec.connector_tasks_script().count("TIMEZONE = 'UTC'") == 4
    import inspect

    from veloparc_gen import snowflake_conn

    assert 'session_parameters={"TIMEZONE": "UTC"}' in inspect.getsource(snowflake_conn.connect)
