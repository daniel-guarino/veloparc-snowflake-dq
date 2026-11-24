"""snowflake-inspect: read-only statements, safe aliases, errors collected, and the shapes it builds (fake cursor)."""

import json
import re

from veloparc_gen import snowflake_inspect as si

TOPIC = "vlp.ops.tracking-event.v1"
LISTED = [
    (f"landing/landing/kafka/{TOPIC}/year=2026/month=09/day=01/hour=16/{TOPIC}+0+0.json.gz", 120),
    (f"landing/landing/kafka/{TOPIC}/year=2026/month=10/day=31/hour=23/{TOPIC}+1+9.json.gz", 80),
    ("landing/landing/kafka/vlp.finance.gl-extract.v1/year=2026/month=10/day=15/x.json.gz", 10),
]
ENVELOPE = {
    "payload": {"trackId": "T1", "recordTime": "2026-09-01T16:00:00+02:00"},
    "_kafka_topic": TOPIC,
    "_kafka_partition": 0,
    "_kafka_offset": 7,
    "_kafka_timestamp": 1788278400000,
    "_kafka_headers": {"ce_id": "x", "vlp_batch_seq": "1"},
}
# Snowflake reserved words that must not be used as bare column aliases.
RESERVED = {"ROWS", "ROW", "PARTITION", "TABLE", "COLUMN", "ORDER", "GROUP", "WHERE", "FROM", "SELECT", "VALUES"}


class FakeCursor:
    """Answers each statement from the first matching (fragment, columns, rows) rule; a rule with rows=None fails."""

    def __init__(self, rules):
        self.rules = rules
        self.sql = []
        self.description = None
        self._rows = []

    def execute(self, sql):
        self.sql.append(sql)
        for fragment, columns, rows in self.rules:
            if fragment in sql:
                if rows is None:
                    raise RuntimeError("001003 (42000): SQL compilation error:\n  unexpected 'x'")
                self.description = [(c,) for c in columns]
                self._rows = rows
                return
        self.description, self._rows = [], []

    def fetchall(self):
        return self._rows


def _rules(**overrides):
    rules = {
        "connector": (
            "SHOW TABLES LIKE 'KAFKA%'",
            ["name", "owner", "rows", "change_tracking"],
            [("KAFKA_TRACKING_EVENTS", "VLP_CONNECTOR", 815, "OFF"), ("KAFKA_bad;--", "X", 0, "OFF")],
        ),
        "tables": ("SHOW TABLES IN SCHEMA", ["name"], [("KAFKA_TRACKING_EVENTS",)]),
        "views": ("SHOW VIEWS", ["name"], []),
        "partitions": ("AS kafka_partition", ["KAFKA_PARTITION", "RECORD_COUNT"], [(0, 400), (1, 415)]),
        "headers": (
            "RECORD_METADATA:headers",
            ["HEADER_KEY", "VALUE_TYPE", "RECORD_COUNT", "SAMPLE_VALUE"],
            [("vlp_batch_seq", "INTEGER", 200, "1")],
        ),
        "meta": ("AS meta_key", ["META_KEY", "VALUE_TYPE"], [("offset", "INTEGER")]),
        "content": ("TYPEOF(RECORD_CONTENT)", ["VALUE_TYPE", "RECORD_COUNT"], [("OBJECT", 815)]),
        "landing": ("LIST @VLP_DP.BRONZE.LANDING", ["name", "size", "md5", "last_modified"],
                    [(n, s, "m", "t") for n, s in LISTED]),
        "envelope": ("TO_JSON($1)", ["ENVELOPE"], [(json.dumps(ENVELOPE),)]),
        "generator": ("LIST @VLP_DP.BRONZE.GENERATOR", ["name", "size"], [("generator/state/offsets.json", 5)]),
        "packages": (
            "INFORMATION_SCHEMA.PACKAGES",
            ["PKG_NAME", "PKG_VERSION", "PYTHON_RUNTIME"],
            [("jsonschema", "4.9.1", "3.11"), ("jsonschema", "4.23.0", "3.11"), ("jsonschema", "4.17.3", "3.11")],
        ),
    }  # fmt: skip
    rules.update(overrides)
    return list(rules.values())


def test_only_read_only_statements_and_no_reserved_aliases():
    cur = FakeCursor(_rules())
    result = si.inspect(cur)
    assert result["errors"] == []
    for sql in cur.sql:
        assert sql.lstrip().split()[0].upper() in {"SHOW", "LIST", "SELECT"}, sql
        aliases = {a.upper() for a in re.findall(r"\bAS\s+([A-Za-z_]+)", sql)}
        assert not aliases & RESERVED, sql


def test_connector_tables_skip_unsafe_names_and_keep_header_types():
    result = si.inspect(FakeCursor(_rules()))["connector_tables"]
    assert list(result) == ["KAFKA_TRACKING_EVENTS"]
    table = result["KAFKA_TRACKING_EVENTS"]
    assert table["owner"] == "VLP_CONNECTOR"
    assert table["partitions"] == [
        {"kafka_partition": 0, "record_count": 400},
        {"kafka_partition": 1, "record_count": 415},
    ]
    assert table["headers"][0]["header_key"] == "vlp_batch_seq"
    assert table["headers"][0]["value_type"] == "INTEGER"


def test_a_failing_statement_is_recorded_and_the_rest_still_runs():
    result = si.inspect(FakeCursor(_rules(partitions=("AS kafka_partition", [], None))))
    assert [e["query"] for e in result["errors"]] == ["KAFKA_TRACKING_EVENTS partitions"]
    assert "SQL compilation error" in result["errors"][0]["error"]
    assert result["connector_tables"]["KAFKA_TRACKING_EVENTS"]["partitions"] is None
    assert result["connector_tables"]["KAFKA_TRACKING_EVENTS"]["headers"]
    assert result["udf_packages"]["jsonschema"]


def test_landing_summary_per_topic():
    summary = si.summarize_landing([{"name": n, "size": s} for n, s in LISTED])
    assert summary[TOPIC] == {"files": 2, "bytes": 200, "first_day": "2026-09-01", "last_day": "2026-10-31"}
    assert summary["vlp.finance.gl-extract.v1"]["files"] == 1


def test_landing_samples_one_envelope_per_topic_and_keeps_only_its_shape():
    cur = FakeCursor(_rules())
    topics = si.Inspector(cur).landing()
    samples = [s for s in cur.sql if "TO_JSON($1)" in s]
    assert len(samples) == len(topics) == 2
    assert all("LIMIT 1" in s and "FF_LANDING_JSONL" in s for s in samples)
    sample = topics[TOPIC]["sample"]
    assert sample["envelope_keys"]["_kafka_timestamp"] == "number"
    assert sample["header_types"] == {"ce_id": "string", "vlp_batch_seq": "string"}
    assert sample["payload_keys"] == ["recordTime", "trackId"]
    assert "T1" not in json.dumps(sample)


def test_envelope_with_a_string_payload_has_no_payload_keys():
    shape = si.describe_envelope({"payload": '{"truncated', "_kafka_headers": {}})
    assert shape["payload_keys"] is None
    assert shape["envelope_keys"]["payload"] == "string"


def test_udf_packages_sorted_by_version_and_missing_ones_empty():
    packages = si.Inspector(FakeCursor(_rules())).udf_packages()
    assert packages["jsonschema"] == {"3.11": ["4.9.1", "4.17.3", "4.23.0"]}
    assert packages["rfc3339-validator"] == {}
