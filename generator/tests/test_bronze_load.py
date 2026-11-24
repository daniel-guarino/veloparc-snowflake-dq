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
    copies = [sql for sql, _ in cur.calls if sql.startswith("COPY INTO")]
    audits = [params for sql, params in cur.calls if sql.startswith("INSERT INTO VLP_DP.BRONZE.LOAD_AUDIT")]
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
