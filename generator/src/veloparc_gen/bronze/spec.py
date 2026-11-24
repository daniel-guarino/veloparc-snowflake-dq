"""Bronze column spec (ADR 0004): the single definition of the 8 RAW_* tables and of both loaders.

Every RAW_* table has the same 25 metadata columns (docs/architecture/ingestion_kafka_s3_bronze.md), then
RAW_PAYLOAD and the topic's BK_* columns. Each column has one SQL expression per load path:

- FILE: COPY INTO from @BRONZE.LANDING; $1 is the envelope (payload + _kafka_* + _kafka_headers).
- CONNECTOR: INSERT ... SELECT from a KAFKA_* table (or its stream); RECORD_METADATA + RECORD_CONTENT.

Expressions only use functions that COPY transformations support (CASE, IFF, NVL, TRY_CAST, TRY_TO_NUMBER,
TO_TIMESTAMP_TZ, CONVERT_TIMEZONE, REGEXP_REPLACE, SPLIT_PART, casts, ...), so the same text works in both
loaders, and nothing can raise: an unparsable value becomes NULL and the row is still loaded.
Header values are normalized here because the connector path delivers some of them as numbers (ADR 0004).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from pathlib import Path

import yaml

from veloparc_gen.config import DEFAULT_CONFIG_DIR, DEFAULT_SCHEMA_DIR

DATABASE = "VLP_DP"
BRONZE = f"{DATABASE}.BRONZE"
LANDING_STAGE = f"{BRONZE}.LANDING"
FILE_FORMAT = f"{BRONZE}.FF_LANDING_JSONL"
LANDING_PREFIX = "landing/kafka"


# --------------------------------------------------------------------------- load paths


@dataclass(frozen=True)
class LoadPath:
    name: str
    envelope: dict[str, str]  # topic, partition, offset, key, timestamp_ms, headers, payload
    file: dict[str, str]  # file metadata columns (NULL on the connector path)
    load_ts: str


FILE = LoadPath(
    name="file",
    envelope={
        "topic": "$1:_kafka_topic",
        "partition": "$1:_kafka_partition",
        "offset": "$1:_kafka_offset",
        "key": "NULL",  # the landing envelope carries no Kafka key
        "timestamp_ms": "$1:_kafka_timestamp",
        "headers": "$1:_kafka_headers",
        "payload": "$1:payload",
    },
    file={
        "name": "METADATA$FILENAME",
        "row_number": "METADATA$FILE_ROW_NUMBER",
        "content_key": "METADATA$FILE_CONTENT_KEY",
        "last_modified": "METADATA$FILE_LAST_MODIFIED",
    },
    load_ts="CONVERT_TIMEZONE('UTC', METADATA$START_SCAN_TIME)",
)

CONNECTOR = LoadPath(
    name="connector",
    envelope={
        "topic": "RECORD_METADATA:topic",
        "partition": "RECORD_METADATA:partition",
        "offset": "RECORD_METADATA:offset",
        "key": "RECORD_METADATA:key::VARCHAR",
        "timestamp_ms": "RECORD_METADATA:CreateTime",
        "headers": "RECORD_METADATA:headers",
        "payload": "RECORD_CONTENT",
    },
    file={"name": "NULL", "row_number": "NULL", "content_key": "NULL", "last_modified": "NULL"},
    load_ts="CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ("
    "TRY_TO_NUMBER(RECORD_METADATA:SnowflakeConnectorPushTime::VARCHAR), 3))",
)

PATHS = (FILE, CONNECTOR)


# --------------------------------------------------------------------------- expression helpers


def text(expr: str) -> str:
    return f"{expr}::VARCHAR"


def number(expr: str) -> str:
    """A number whether the value arrives as a JSON number or as a string."""
    return f"TRY_TO_NUMBER({expr}::VARCHAR)"


def ts_iso(varchar_expr: str) -> str:
    """ISO-8601 with an offset or Z, normalized to UTC; NULL if unparsable."""
    return f"CONVERT_TIMEZONE('UTC', TRY_CAST({varchar_expr} AS TIMESTAMP_TZ))"


def ts_epoch_ms(expr: str) -> str:
    return f"CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(TRY_TO_NUMBER({expr}::VARCHAR), 3))"


def ts_iso_or_epoch_ms(varchar_expr: str) -> str:
    """ce_time: ISO-8601 in the files, epoch milliseconds in a string on the connector path."""
    return f"IFF(TRY_TO_NUMBER({varchar_expr}) IS NULL, {ts_iso(varchar_expr)}, {ts_epoch_ms(varchar_expr)})"


def ts_salesforce(varchar_expr: str) -> str:
    """Salesforce date-time: offset without a colon (+0000); a colon is inserted before parsing."""
    return ts_iso(f"REGEXP_REPLACE({varchar_expr}, '([+-][0-9]{{2}})([0-9]{{2}})$', '\\\\1:\\\\2')")


def ts_date(varchar_expr: str) -> str:
    """A calendar date, as midnight UTC."""
    return f"CONVERT_TIMEZONE('UTC', TRY_CAST({varchar_expr} || 'T00:00:00Z' AS TIMESTAMP_TZ))"


def header(path: LoadPath, name: str) -> str:
    return f"{path.envelope['headers']}:{name}"


def field(path: LoadPath, dotted: str) -> str:
    """Payload field, e.g. 'attributes:type'. Path elements are case-sensitive (Salesforce 'Id')."""
    return f"{path.envelope['payload']}:{dotted}"


def ce_time(path: LoadPath) -> str:
    return ts_iso_or_epoch_ms(text(header(path, "ce_time")))


# --------------------------------------------------------------------------- columns


@dataclass(frozen=True)
class Column:
    name: str
    sql_type: str
    comment: str
    expr: Callable[[LoadPath], str] | None = None  # None: filled by a column default


def _h(name: str) -> Callable[[LoadPath], str]:
    return lambda p: text(header(p, name))


METADATA_COLUMNS: tuple[Column, ...] = (
    # A · source journal
    Column("_OP_TYPE", "VARCHAR", "I/U/D/R (header vlp_op; &ENTTYP)", _h("vlp_op")),
    Column("_SOURCE_TS", "TIMESTAMP_TZ", "Business time of the record, UTC (per topic; &TIMSTAMP)"),
    Column("_SOURCE_SYSTEM", "VARCHAR", "Header ce_source (&SYSTEM)", _h("ce_source")),
    Column("_SOURCE_USER", "VARCHAR", "Header vlp_producer (&USER)", _h("vlp_producer")),
    Column("_SOURCE_TXN_ID", "VARCHAR", "Header vlp_batch_id (&CCID)", _h("vlp_batch_id")),
    Column("_SOURCE_TXN_SEQ", "NUMBER(38,0)", "Header vlp_batch_seq", lambda p: number(header(p, "vlp_batch_seq"))),
    Column(
        "_SOURCE_TXN_COUNT", "NUMBER(38,0)", "Header vlp_batch_count", lambda p: number(header(p, "vlp_batch_count"))
    ),
    Column("_EVENT_ID", "VARCHAR", "Header ce_id", _h("ce_id")),
    Column("_EVENT_TYPE", "VARCHAR", "Header ce_type", _h("ce_type")),
    Column("_ENTITY_TYPE", "VARCHAR", "Header vlp_entity", _h("vlp_entity")),
    # B · Kafka
    Column("_KAFKA_TOPIC", "VARCHAR", "Kafka topic", lambda p: text(p.envelope["topic"])),
    Column("_KAFKA_PARTITION", "NUMBER(38,0)", "Kafka partition", lambda p: number(p.envelope["partition"])),
    Column(
        "_KAFKA_OFFSET",
        "NUMBER(38,0)",
        "Kafka offset (simulated in files, broker on the connector path; &SEQNO)",
        lambda p: number(p.envelope["offset"]),
    ),
    Column("_KAFKA_KEY", "VARCHAR", "Kafka key (connector path only)", lambda p: p.envelope["key"]),
    Column(
        "_KAFKA_TS",
        "TIMESTAMP_TZ",
        "Kafka timestamp, UTC (simulated broker time in files, CreateTime on the connector path)",
        lambda p: ts_epoch_ms(p.envelope["timestamp_ms"]),
    ),
    Column(
        "_KAFKA_HEADERS", "OBJECT", "Headers as delivered by each path", lambda p: f"{p.envelope['headers']}::OBJECT"
    ),
    # C · file
    Column("_FILE_NAME", "VARCHAR", "METADATA$FILENAME", lambda p: p.file["name"]),
    Column("_FILE_ROW_NUMBER", "NUMBER(38,0)", "METADATA$FILE_ROW_NUMBER", lambda p: p.file["row_number"]),
    Column("_FILE_CONTENT_KEY", "VARCHAR", "METADATA$FILE_CONTENT_KEY", lambda p: p.file["content_key"]),
    Column("_FILE_LAST_MODIFIED", "TIMESTAMP_NTZ", "METADATA$FILE_LAST_MODIFIED", lambda p: p.file["last_modified"]),
    # D · load and lineage
    Column("_SCHEMA_ID", "VARCHAR", "$id of the topic's JSON Schema"),
    Column("_SCHEMA_VERSION", "VARCHAR", "Header vlp_schema_version", _h("vlp_schema_version")),
    Column(
        "_TRACE_ID",
        "VARCHAR",
        "Trace id of the traceparent header",
        lambda p: f"SPLIT_PART({text(header(p, 'traceparent'))}, '-', 2)",
    ),
    Column(
        "_LOAD_TS",
        "TIMESTAMP_TZ",
        "Arrival in Snowflake, UTC (COPY scan start; connector push time)",
        lambda p: p.load_ts,
    ),
    Column("_INSERTED_AT", "TIMESTAMP_TZ", "Insert into this table (column default)"),
)

INSERTED_AT_DEFAULT = "CURRENT_TIMESTAMP()"


@dataclass(frozen=True)
class BusinessKey:
    name: str
    expr: Callable[[LoadPath], str]
    comment: str


@dataclass(frozen=True)
class BronzeTopic:
    source: str  # key in generator/config/topics.yaml
    table: str
    connector_table: str | None
    source_ts: Callable[[LoadPath], str]
    keys: tuple[BusinessKey, ...]
    comment: str
    topic: str = ""
    schema_id: str = ""

    def column_exprs(self, path: LoadPath) -> list[tuple[str, str]]:
        """(column, expression) for every loaded column, in table order; _INSERTED_AT is left to its default."""
        out: list[tuple[str, str]] = []
        for col in METADATA_COLUMNS:
            if col.name == "_SOURCE_TS":
                out.append((col.name, self.source_ts(path)))
            elif col.name == "_SCHEMA_ID":
                out.append((col.name, "'" + self.schema_id.replace("'", "''") + "'"))
            elif col.expr is not None:
                out.append((col.name, col.expr(path)))
        out.append(("RAW_PAYLOAD", path.envelope["payload"]))
        out.extend((key.name, key.expr(path)) for key in self.keys)
        return out


def _bk(name: str, dotted: str, comment: str | None = None) -> BusinessKey:
    return BusinessKey(name, lambda p: text(field(p, dotted)), comment or f"payload {dotted.replace(':', '.')}")


def _master_data_key(p: LoadPath) -> str:
    entity = text(field(p, "entity"))
    return (
        f"CASE {entity} WHEN 'customer' THEN {text(field(p, 'contactId'))} "
        f"WHEN 'depot' THEN {text(field(p, 'gln'))} "
        f"WHEN 'partner' THEN {text(field(p, 'partnerId'))} END"
    )


_TOPICS = (
    BronzeTopic(
        "veloship_shipment",
        "RAW_VELOSHIP_SHIPMENTS",
        "KAFKA_VELOSHIP_SHIPMENTS",
        lambda p: ts_iso(text(field(p, "receivedAt"))),
        (_bk("BK_MESSAGE_ID", "messageId"),),
        "VeloShip shipment requests and responses",
    ),
    BronzeTopic(
        "tracking_event",
        "RAW_TRACKING_EVENTS",
        "KAFKA_TRACKING_EVENTS",
        lambda p: ts_iso(text(field(p, "recordTime"))),
        (_bk("BK_TRACK_ID", "trackId"), _bk("BK_EVENT_ID", "eventId")),
        "Scan events (EPCIS-like)",
    ),
    BronzeTopic(
        "invoice",
        "RAW_INVOICES",
        "KAFKA_INVOICES",
        lambda p: ts_date(text(field(p, "issueDate"))),
        (_bk("BK_INVOICE_NUMBER", "invoiceNumber"),),
        "Invoices and credit notes",
    ),
    BronzeTopic(
        "crm_object",
        "RAW_CRM_OBJECTS",
        "KAFKA_CRM_OBJECTS",
        lambda p: (
            f"NVL({ts_salesforce(text(field(p, 'LastModifiedDate')))}, {ts_salesforce(text(field(p, 'CreatedDate')))})"
        ),
        (_bk("BK_CRM_ID", "Id"), _bk("BK_CRM_TYPE", "attributes:type")),
        "CRM leads and opportunities",
    ),
    BronzeTopic(
        "partner_settlement",
        "RAW_PARTNER_SETTLEMENTS",
        None,
        ce_time,
        (_bk("BK_SETTLEMENT_ID", "settlementId"),),
        "Monthly partner settlements (file-based)",
    ),
    BronzeTopic(
        "tariff",
        "RAW_TARIFFS",
        None,
        ce_time,
        (_bk("BK_TARIFF_ID", "tariffId"),),
        "Customer tariffs (file-based)",
    ),
    BronzeTopic(
        "gl_extract",
        "RAW_GL_EXTRACT",
        None,
        ce_time,
        (_bk("BK_COMPANY_CODE", "company_code"), _bk("BK_PERIOD", "period"), _bk("BK_GL_ACCOUNT", "gl_account")),
        "Monthly GL extract rows (file-based)",
    ),
    BronzeTopic(
        "master_data",
        "RAW_MASTER_DATA",
        None,
        lambda p: f"NVL({ts_iso(text(field(p, 'updatedAt')))}, {ce_time(p)})",
        (
            _bk("BK_ENTITY", "entity"),
            BusinessKey("BK_NATURAL_KEY", _master_data_key, "contactId (customer), gln (depot), partnerId (partner)"),
        ),
        "Customers, depots and partners (file-based)",
    ),
)


def _load_topics(
    config_dir: Path = DEFAULT_CONFIG_DIR, schema_dir: Path = DEFAULT_SCHEMA_DIR
) -> dict[str, BronzeTopic]:
    with (config_dir / "topics.yaml").open(encoding="utf-8") as fh:
        configured = yaml.safe_load(fh)["topics"]
    topics: dict[str, BronzeTopic] = {}
    for spec in _TOPICS:
        cfg = configured[spec.source]
        schema = json.loads((schema_dir / cfg["schema"]).read_text(encoding="utf-8"))
        topics[spec.source] = replace(spec, topic=cfg["name"], schema_id=schema["$id"])
    return topics


TOPICS: dict[str, BronzeTopic] = _load_topics()


# --------------------------------------------------------------------------- SQL generation


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _indent(lines: list[str], spaces: int = 4) -> str:
    pad = " " * spaces
    return f",\n{pad}".join(lines)


def ddl(topic: BronzeTopic) -> str:
    cols = []
    for col in METADATA_COLUMNS:
        default = f" DEFAULT {INSERTED_AT_DEFAULT}" if col.name == "_INSERTED_AT" else ""
        cols.append(f"{col.name} {col.sql_type}{default} COMMENT {_literal(col.comment)}")
    cols.append("RAW_PAYLOAD VARIANT COMMENT 'Payload exactly as received'")
    cols.extend(f"{key.name} VARCHAR COMMENT {_literal(key.comment)}" for key in topic.keys)
    return (
        f"CREATE TABLE IF NOT EXISTS {BRONZE}.{topic.table} (\n    {_indent(cols)}\n)\n"
        f"COMMENT = {_literal(f'{topic.comment}. Topic {topic.topic}. Append-only (ADR 0004)')};"
    )


def select_sql(topic: BronzeTopic, path: LoadPath, source: str) -> str:
    exprs = [f"{expr} AS {name}" for name, expr in topic.column_exprs(path)]
    return f"SELECT\n    {_indent(exprs)}\nFROM {source}"


def landing_location(topic: BronzeTopic) -> str:
    return f"@{LANDING_STAGE}/{LANDING_PREFIX}/{topic.topic}/"


def copy_sql(topic: BronzeTopic) -> str:
    columns = [name for name, _ in topic.column_exprs(FILE)]
    select = select_sql(topic, FILE, landing_location(topic)).replace("\n", "\n    ")
    return (
        f"COPY INTO {BRONZE}.{topic.table} (\n    {_indent(columns)}\n)\n"
        f"FROM (\n    {select}\n)\n"
        f"FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')\n"
        "ON_ERROR = CONTINUE"
    )


def connector_insert_sql(topic: BronzeTopic, source: str) -> str:
    if topic.connector_table is None:
        raise ValueError(f"{topic.source} has no connector table")
    columns = [name for name, _ in topic.column_exprs(CONNECTOR)]
    return f"INSERT INTO {BRONZE}.{topic.table} (\n    {_indent(columns)}\n)\n{select_sql(topic, CONNECTOR, source)}"


MIGRATION_HEADER = """\
-- GENERATED by `veloparc-gen bronze-sql` from generator/src/veloparc_gen/bronze/spec.py. Do not edit by hand:
-- change the spec, regenerate, and (once applied) ship the change as a new migration.
-- Bronze RAW_* tables (ADR 0004): 25 metadata columns, RAW_PAYLOAD, BK_*. Append-only.
"""


def raw_tables_migration() -> str:
    parts = [MIGRATION_HEADER]
    for topic in TOPICS.values():
        parts.append(ddl(topic) + "\n")
    parts.append(
        "-- Loader writes, transformer (dbt) reads.\n"
        + "".join(f"GRANT SELECT, INSERT ON TABLE {BRONZE}.{t.table} TO ROLE VLP_LOADER;\n" for t in TOPICS.values())
        + "".join(f"GRANT SELECT ON TABLE {BRONZE}.{t.table} TO ROLE VLP_TRANSFORMER;\n" for t in TOPICS.values())
    )
    return "\n".join(parts)


# --------------------------------------------------------------------------- connector loader (stream + task)

LOAD_WAREHOUSE = "WH_LOAD"


def stream_name(topic: BronzeTopic) -> str:
    return f"{BRONZE}.{topic.connector_table}_STREAM"


def task_name(topic: BronzeTopic) -> str:
    return f"{BRONZE}.TASK_LOAD_{topic.table}"


CONNECTOR_TASKS_HEADER = """\
-- GENERATED by `veloparc-gen bronze-sql` from generator/src/veloparc_gen/bronze/spec.py. Do not edit by hand.
-- Repeatable (R__): schemachange re-runs it whenever its content changes.
-- Connector loader (ADR 0004): KAFKA_* (Snowpipe Streaming) -> append-only stream -> triggered task -> RAW_*.
-- The stream is created once (IF NOT EXISTS) so a re-run never replays rows; SHOW_INITIAL_ROWS makes the first
-- task run pick up the rows the connector wrote before the stream existed. The task is replaced and resumed.
"""


def connector_task_sql(topic: BronzeTopic) -> str:
    table = f"{BRONZE}.{topic.connector_table}"
    stream, task = stream_name(topic), task_name(topic)
    insert = connector_insert_sql(topic, stream)
    return (
        f"-- {topic.topic}: {table} -> {BRONZE}.{topic.table}\n"
        f"ALTER TABLE {table} SET CHANGE_TRACKING = TRUE;\n"
        f"CREATE STREAM IF NOT EXISTS {stream} ON TABLE {table}\n"
        f"  APPEND_ONLY = TRUE SHOW_INITIAL_ROWS = TRUE\n"
        f"  COMMENT = 'New connector rows for {topic.table} (ADR 0004)';\n"
        f"ALTER TASK IF EXISTS {task} SUSPEND;\n"
        f"CREATE OR REPLACE TASK {task}\n"
        f"  WAREHOUSE = {LOAD_WAREHOUSE}\n"
        f"  COMMENT = 'Connector loader: {topic.connector_table} -> {topic.table} (ADR 0004)'\n"
        f"  WHEN SYSTEM$STREAM_HAS_DATA('{stream}')\n"
        f"AS\n{insert};\n"
        f"ALTER TASK {task} RESUME;\n"
    )


def connector_tasks_script() -> str:
    parts = [CONNECTOR_TASKS_HEADER]
    parts.extend(connector_task_sql(t) for t in TOPICS.values() if t.connector_table)
    return "\n".join(parts)


GENERATED_FILES = {
    "infra/snowflake/migrations/V1.4.0__bronze_raw_tables.sql": raw_tables_migration,
    "infra/snowflake/migrations/R__bronze_connector_tasks.sql": connector_tasks_script,
}
