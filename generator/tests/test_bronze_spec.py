"""Bronze column spec: 25 metadata columns, identical columns on both load paths, COPY-safe functions, no drift."""

import json
import re

import pytest

from veloparc_gen.bronze import preview, spec
from veloparc_gen.config import DEFAULT_SCHEMA_DIR, REPO_ROOT

# Functions COPY INTO transformations accept that the spec uses
# (https://docs.snowflake.com/en/user-guide/data-load-transform#supported-functions).
COPY_FUNCTIONS = {
    "CASE",
    "CONVERT_TIMEZONE",
    "IFF",
    "NVL",
    "REGEXP_REPLACE",
    "SPLIT_PART",
    "TO_TIMESTAMP_TZ",
    "TRY_CAST",
    "TRY_TO_NUMBER",
}
_CALL = re.compile(r"\b([A-Z_][A-Z0-9_]*)\s*\(")
_STRING = re.compile(r"'(?:[^']|'')*'")

EXPECTED_METADATA = [
    "_OP_TYPE", "_SOURCE_TS", "_SOURCE_SYSTEM", "_SOURCE_USER", "_SOURCE_TXN_ID", "_SOURCE_TXN_SEQ",
    "_SOURCE_TXN_COUNT", "_EVENT_ID", "_EVENT_TYPE", "_ENTITY_TYPE",
    "_KAFKA_TOPIC", "_KAFKA_PARTITION", "_KAFKA_OFFSET", "_KAFKA_KEY", "_KAFKA_TS", "_KAFKA_HEADERS",
    "_FILE_NAME", "_FILE_ROW_NUMBER", "_FILE_CONTENT_KEY", "_FILE_LAST_MODIFIED",
    "_SCHEMA_ID", "_SCHEMA_VERSION", "_TRACE_ID", "_LOAD_TS", "_INSERTED_AT",
]  # fmt: skip
FILE_COLUMNS = {"_FILE_NAME", "_FILE_ROW_NUMBER", "_FILE_CONTENT_KEY", "_FILE_LAST_MODIFIED"}


def test_twenty_five_metadata_columns_in_documented_order():
    assert [c.name for c in spec.METADATA_COLUMNS] == EXPECTED_METADATA
    assert len(spec.METADATA_COLUMNS) == 25


def test_eight_business_topics_and_no_dlq():
    assert len(spec.TOPICS) == 8
    assert "dlq" not in spec.TOPICS
    assert {t.table for t in spec.TOPICS.values()} == {
        "RAW_VELOSHIP_SHIPMENTS", "RAW_TRACKING_EVENTS", "RAW_INVOICES", "RAW_CRM_OBJECTS",
        "RAW_PARTNER_SETTLEMENTS", "RAW_TARIFFS", "RAW_GL_EXTRACT", "RAW_MASTER_DATA",
    }  # fmt: skip
    connector = {t.connector_table for t in spec.TOPICS.values() if t.connector_table}
    assert connector == {"KAFKA_VELOSHIP_SHIPMENTS", "KAFKA_TRACKING_EVENTS", "KAFKA_INVOICES", "KAFKA_CRM_OBJECTS"}


@pytest.mark.parametrize("topic", spec.TOPICS.values(), ids=lambda t: t.source)
def test_schema_id_and_topic_name_come_from_contracts_and_config(topic):
    assert topic.topic.startswith("vlp.") and topic.topic.endswith(".v1")
    schemas = {json.loads(p.read_text())["$id"] for p in DEFAULT_SCHEMA_DIR.glob("*.schema.json")}
    assert topic.schema_id in schemas


@pytest.mark.parametrize("topic", spec.TOPICS.values(), ids=lambda t: t.source)
def test_both_paths_fill_the_same_columns_in_the_same_order(topic):
    file_cols = [n for n, _ in topic.column_exprs(spec.FILE)]
    connector_cols = [n for n, _ in topic.column_exprs(spec.CONNECTOR)]
    assert file_cols == connector_cols
    assert file_cols[:24] == EXPECTED_METADATA[:24]  # _INSERTED_AT is the column default
    assert file_cols[24] == "RAW_PAYLOAD"
    assert all(c.startswith("BK_") for c in file_cols[25:]) and file_cols[25:]


@pytest.mark.parametrize("topic", spec.TOPICS.values(), ids=lambda t: t.source)
def test_file_columns_are_null_only_on_the_connector_path(topic):
    file_exprs = dict(topic.column_exprs(spec.FILE))
    connector_exprs = dict(topic.column_exprs(spec.CONNECTOR))
    for col in FILE_COLUMNS:
        assert connector_exprs[col] == "NULL"
        assert file_exprs[col].startswith("METADATA$")
    assert file_exprs["_KAFKA_KEY"] == "NULL"
    assert connector_exprs["_KAFKA_KEY"] == "RECORD_METADATA:key::VARCHAR"
    # outside the file group and the key, the two paths differ only in where they read from
    for col, expr in file_exprs.items():
        if col in FILE_COLUMNS | {"_KAFKA_KEY", "_LOAD_TS"}:
            continue
        translated = (
            expr.replace("$1:_kafka_headers", "RECORD_METADATA:headers")
            .replace("$1:payload", "RECORD_CONTENT")
            .replace("$1:_kafka_topic", "RECORD_METADATA:topic")
            .replace("$1:_kafka_partition", "RECORD_METADATA:partition")
            .replace("$1:_kafka_offset", "RECORD_METADATA:offset")
            .replace("$1:_kafka_timestamp", "RECORD_METADATA:CreateTime")
        )
        assert translated == connector_exprs[col], col


@pytest.mark.parametrize("topic", spec.TOPICS.values(), ids=lambda t: t.source)
def test_copy_uses_only_functions_copy_transformations_support(topic):
    sql = _STRING.sub("''", spec.copy_sql(topic))
    select = sql[sql.index("SELECT") :]
    used = set(_CALL.findall(select))
    assert used <= COPY_FUNCTIONS, used - COPY_FUNCTIONS
    for forbidden in ("COALESCE", "CURRENT_TIMESTAMP", "TRY_TO_TIMESTAMP", " WHERE ", " LIMIT "):
        assert forbidden not in select


def test_copy_reads_only_its_topic_folder_and_keeps_bad_rows_out_of_errors():
    sql = spec.copy_sql(spec.TOPICS["tracking_event"])
    assert "FROM @VLP_DP.BRONZE.LANDING/landing/kafka/vlp.ops.tracking-event.v1/" in sql
    assert "FILE_FORMAT = (FORMAT_NAME = 'VLP_DP.BRONZE.FF_LANDING_JSONL')" in sql
    assert sql.rstrip().endswith("ON_ERROR = CONTINUE")


def test_header_normalization_handles_both_delivery_forms():
    exprs = dict(spec.TOPICS["tariff"].column_exprs(spec.CONNECTOR))
    # ce_time: epoch ms string on the connector path, ISO in the files
    assert "IFF(TRY_TO_NUMBER(RECORD_METADATA:headers:ce_time::VARCHAR) IS NULL" in exprs["_SOURCE_TS"]
    assert "TO_TIMESTAMP_TZ(TRY_TO_NUMBER(RECORD_METADATA:headers:ce_time::VARCHAR::VARCHAR), 3)" in exprs["_SOURCE_TS"]
    # batch seq/count: integers on the connector path, strings in the files
    assert exprs["_SOURCE_TXN_SEQ"] == "TRY_TO_NUMBER(RECORD_METADATA:headers:vlp_batch_seq::VARCHAR)"


def test_salesforce_offset_gets_a_colon_before_parsing():
    expr = spec.ts_salesforce("x")
    assert "REGEXP_REPLACE(x, '([+-][0-9]{2})([0-9]{2})$', '\\\\1:\\\\2')" in expr
    assert "TRY_CAST(" in expr and "AS TIMESTAMP_TZ" in expr


def test_salesforce_regex_on_python_side_matches_the_intent():
    # Same pattern as the SQL, to document what it does to each format.
    pattern = re.compile(r"([+-][0-9]{2})([0-9]{2})$")
    assert pattern.sub(r"\1:\2", "2026-09-01T10:00:00.000+0000") == "2026-09-01T10:00:00.000+00:00"
    assert pattern.sub(r"\1:\2", "2026-09-01T10:00:00.000+02:00") == "2026-09-01T10:00:00.000+02:00"
    assert pattern.sub(r"\1:\2", "2026-09-01T10:00:00.000Z") == "2026-09-01T10:00:00.000Z"
    assert pattern.sub(r"\1:\2", "2026-09-01") == "2026-09-01"


def test_master_data_natural_key_per_entity():
    exprs = dict(spec.TOPICS["master_data"].column_exprs(spec.FILE))
    key = exprs["BK_NATURAL_KEY"]
    assert "WHEN 'customer' THEN $1:payload:contactId::VARCHAR" in key
    assert "WHEN 'depot' THEN $1:payload:gln::VARCHAR" in key
    assert "WHEN 'partner' THEN $1:payload:partnerId::VARCHAR" in key
    assert exprs["_SOURCE_TS"].startswith("NVL(")


def test_ddl_has_every_column_and_the_inserted_at_default():
    ddl = spec.ddl(spec.TOPICS["gl_extract"])
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS VLP_DP.BRONZE.RAW_GL_EXTRACT (")
    for name in [*EXPECTED_METADATA, "RAW_PAYLOAD", "BK_COMPANY_CODE", "BK_PERIOD", "BK_GL_ACCOUNT"]:
        assert re.search(rf"^\s+{name} ", ddl, re.M), name
    assert "_INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()" in ddl
    assert "'$id of the topic''s JSON Schema'" in ddl  # quotes escaped


@pytest.mark.parametrize("relative", sorted(spec.GENERATED_FILES))
def test_committed_generated_sql_matches_the_spec(relative):
    committed = (REPO_ROOT / relative).read_text(encoding="utf-8")
    assert committed == spec.GENERATED_FILES[relative](), "run `uv run veloparc-gen bronze-sql`"
    assert "{{" not in committed and "{%" not in committed  # schemachange renders Jinja


def test_preview_queries_are_read_only_and_cover_both_paths():
    statements = []
    for topic in spec.TOPICS.values():
        for path, source in preview.sources(topic):
            statements += [preview.profile_sql(topic, path, source, 10), preview.sample_sql(topic, path, source)]
    assert len(statements) == 2 * (8 + 4)
    assert all(s.startswith("SELECT") and "INSERT" not in s and "COPY" not in s for s in statements)
    profile = preview.profile_sql(spec.TOPICS["invoice"], spec.FILE, "SRC", 10)
    assert "COUNT(_SOURCE_TS) AS nn_source_ts" in profile and "LIMIT 10" in profile


class _Cursor:
    """Profile queries return 2 sampled rows with every column non-NULL except _KAFKA_KEY; one sample fails."""

    def __init__(self):
        self.description, self._rows, self.sql = [], [], []

    def execute(self, sql):
        self.sql.append(sql)
        if "RAW_TARIFFS" in sql:  # never referenced: preview only reads sources
            raise AssertionError
        if "COUNT(*) AS sampled_rows" in sql:
            names = ["SAMPLED_ROWS", *re.findall(r" AS (nn_[a-z0-9_]+)", sql)]
            self.description = [(n,) for n in names]
            self._rows = [tuple(2 if n == "SAMPLED_ROWS" else (0 if n == "nn_kafka_key" else 2) for n in names)]
        elif "LIMIT 1\n" in sql and "KAFKA_CRM_OBJECTS" in sql:
            raise RuntimeError("002003 (02000): SQL compilation error: does not exist")
        else:
            self.description, self._rows = [("_OP_TYPE",)], [("I",)]

    def fetchall(self):
        return self._rows


def test_preview_reports_counts_always_null_columns_and_errors():
    result = preview.preview(_Cursor(), limit=5)
    shipments = result["topics"]["RAW_VELOSHIP_SHIPMENTS"]["paths"]
    assert set(shipments) == {"file", "connector"}
    assert shipments["file"]["sampled_rows"] == 2
    assert shipments["file"]["always_null"] == ["nn_kafka_key"]
    assert set(result["topics"]["RAW_TARIFFS"]["paths"]) == {"file"}
    assert [e["query"] for e in result["errors"]] == ["RAW_CRM_OBJECTS connector sample"]


def test_bronze_sql_check_passes_on_the_committed_migration():
    from typer.testing import CliRunner

    from veloparc_gen.cli import app

    result = CliRunner().invoke(app, ["bronze-sql", "--check"])
    assert result.exit_code == 0, result.output
    assert "up to date" in result.output
