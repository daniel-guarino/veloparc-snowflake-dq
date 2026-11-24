"""snowflake-inspect: read-only statements only, and the shapes it builds from SHOW / LIST / SELECT (fake cursor)."""

from veloparc_gen import snowflake_inspect as si


class FakeCursor:
    """Answers each statement from the first matching (fragment, columns, rows) rule."""

    def __init__(self, rules):
        self.rules = rules
        self.sql = []
        self.description = None
        self._rows = []

    def execute(self, sql):
        self.sql.append(sql)
        for fragment, columns, rows in self.rules:
            if fragment in sql:
                self.description = [(c,) for c in columns]
                self._rows = rows
                return
        self.description, self._rows = [], []

    def fetchall(self):
        return self._rows


TOPIC = "vlp.ops.tracking-event.v1"
LISTED = [
    (f"landing/landing/kafka/{TOPIC}/year=2026/month=09/day=01/hour=16/{TOPIC}+0+0.json.gz", 120),
    (f"landing/landing/kafka/{TOPIC}/year=2026/month=10/day=31/hour=23/{TOPIC}+1+9.json.gz", 80),
    ("landing/landing/kafka/vlp.finance.gl-extract.v1/year=2026/month=10/day=15/x.json.gz", 10),
]


def _cursor():
    return FakeCursor(
        [
            (
                "SHOW TABLES LIKE 'KAFKA%'",
                ["name", "owner", "rows", "change_tracking"],
                [("KAFKA_TRACKING_EVENTS", "VLP_CONNECTOR", 815, "OFF"), ("KAFKA_bad;--", "X", 0, "OFF")],
            ),
            ("SHOW TABLES IN SCHEMA", ["name"], [("KAFKA_TRACKING_EVENTS",)]),
            ("SHOW VIEWS", ["name"], []),
            ("RECORD_METADATA:partition", ["PARTITION", "ROWS"], [(0, 400), (1, 415)]),
            ("RECORD_METADATA:headers", ["KEY", "TYPE", "N", "EXAMPLE"], [("vlp_batch_seq", "INTEGER", 200, "1")]),
            ("FLATTEN(input => s.RECORD_METADATA)", ["KEY", "TYPE", "N", "EXAMPLE"], [("offset", "INTEGER", 200, "7")]),
            ("TYPEOF(RECORD_CONTENT)", ["TYPE", "N"], [("OBJECT", 815)]),
            (
                "LIST @VLP_DP.BRONZE.LANDING",
                ["name", "size", "md5", "last_modified"],
                [(n, s, "m", "t") for n, s in LISTED],
            ),
            ("OBJECT_KEYS($1)", ["ENVELOPE_KEYS", "PAYLOAD_TYPE"], [('["payload"]', "OBJECT")]),
            ("LIST @VLP_DP.BRONZE.GENERATOR", ["name", "size"], [("generator/state/offsets.json", 5)]),
            (
                "INFORMATION_SCHEMA.PACKAGES",
                ["PACKAGE", "VERSION", "RUNTIME"],
                [("jsonschema", "4.9.1", "3.11"), ("jsonschema", "4.23.0", "3.11"), ("jsonschema", "4.17.3", "3.11")],
            ),
        ]
    )


def test_only_read_only_statements():
    cur = _cursor()
    si.inspect(cur)
    assert cur.sql
    for sql in cur.sql:
        assert sql.lstrip().split()[0].upper() in {"SHOW", "LIST", "SELECT"}, sql


def test_connector_tables_skip_unsafe_names_and_keep_header_types():
    result = si.inspect(_cursor())["connector_tables"]
    assert list(result) == ["KAFKA_TRACKING_EVENTS"]
    table = result["KAFKA_TRACKING_EVENTS"]
    assert table["owner"] == "VLP_CONNECTOR"
    assert table["partitions"] == [{"partition": 0, "rows": 400}, {"partition": 1, "rows": 415}]
    assert table["headers"][0] == {"key": "vlp_batch_seq", "type": "INTEGER", "n": 200, "example": "1"}


def test_landing_summary_per_topic():
    summary = si.summarize_landing([{"name": n, "size": s} for n, s in LISTED])
    assert summary[TOPIC] == {"files": 2, "bytes": 200, "first_day": "2026-09-01", "last_day": "2026-10-31"}
    assert summary["vlp.finance.gl-extract.v1"]["files"] == 1


def test_landing_samples_one_envelope_per_topic():
    cur = _cursor()
    topics = si.landing(cur)
    samples = [s for s in cur.sql if "OBJECT_KEYS($1)" in s]
    assert len(samples) == len(topics) == 2
    assert all("LIMIT 1" in s and "FF_LANDING_JSONL" in s for s in samples)
    assert topics[TOPIC]["sample"]["payload_type"] == "OBJECT"


def test_udf_packages_sorted_by_version_and_missing_ones_empty():
    packages = si.udf_packages(_cursor())
    assert packages["jsonschema"] == {"3.11": ["4.9.1", "4.17.3", "4.23.0"]}
    assert packages["rfc3339-validator"] == {}
