"""Connector deployment with fake Snowflake and Aiven: key format, rendering, create/update and status."""

import base64
import json

import pytest
from cryptography.hazmat.primitives import serialization

from veloparc_gen import connector
from veloparc_gen.aiven import AivenApi, AivenError


def test_key_pair_formats_round_trip():
    private_b64, public_b64 = connector.generate_key_pair()
    key = serialization.load_der_private_key(base64.b64decode(private_b64), password=None)
    public = serialization.load_der_public_key(base64.b64decode(public_b64))
    assert key.public_key().public_numbers() == public.public_numbers()
    assert "\n" not in private_b64 and "-----" not in public_b64


def test_template_renders_and_keeps_contract():
    values = {"SNOWFLAKE_URL": "org-acct.snowflakecomputing.com"}
    values["SNOWFLAKE_PRIVATE_KEY"] = "KEY"  # pragma: allowlist secret
    config = connector.render(connector.TEMPLATE.read_text(), values)
    assert config["snowflake.private.key"] == "KEY"
    assert config["snowflake.ingestion.method"] == "SNOWPIPE_STREAMING"
    assert config["snowflake.enable.schematization"] == "false"
    topics = config["topics"].split(",")
    mapped = dict(pair.split(":") for pair in config["snowflake.topic2table.map"].split(","))
    assert set(topics) == set(mapped) and len(topics) == 4
    assert config["errors.deadletterqueue.topic.name"] == "vlp.dlq.s3-sink.v1"
    with pytest.raises(connector.ConnectorFailed):
        connector.render(connector.TEMPLATE.read_text(), {"SNOWFLAKE_URL": "x"})


class FakeConnectApi(AivenApi):
    def __init__(self, existing=(), statuses=None):
        self.calls = []
        self._existing = list(existing)
        self._statuses = list(statuses or [{"state": "RUNNING", "tasks": [{"id": 0, "state": "RUNNING"}]}])
        super().__init__("proj", "connect", transport=self._transport)

    def _transport(self, method, url, body):
        path = url.split("/service/connect")[-1]
        self.calls.append((method, path, body))
        if method == "GET" and path == "/connectors":
            return {"connectors": [{"name": n} for n in self._existing]}
        if method == "GET" and path.endswith("/status"):
            status = self._statuses.pop(0) if len(self._statuses) > 1 else self._statuses[0]
            if isinstance(status, Exception):
                raise status
            return {"status": status}
        return {}


def test_deploy_creates_then_waits_for_running():
    api = FakeConnectApi(
        statuses=[
            AivenError("HTTP 404"),
            {"state": "RUNNING", "tasks": [{"id": 0, "state": "UNASSIGNED"}]},
            {"state": "RUNNING", "tasks": [{"id": 0, "state": "RUNNING"}, {"id": 1, "state": "RUNNING"}]},
        ]
    )
    status = connector.deploy(api, {"name": "vlp-snowflake-sink"}, log=lambda *_: None, sleep=lambda _s: None)
    assert ("POST", "/connectors", {"name": "vlp-snowflake-sink"}) in api.calls
    assert len(status["tasks"]) == 2


def test_deploy_updates_existing_and_reports_failed_task():
    api = FakeConnectApi(
        existing=["vlp-snowflake-sink"],
        statuses=[{"state": "RUNNING", "tasks": [{"id": 0, "state": "FAILED", "trace": "JWT token is invalid"}]}],
    )
    with pytest.raises(connector.ConnectorFailed, match="JWT token is invalid"):
        connector.deploy(api, {"name": "vlp-snowflake-sink"}, log=lambda *_: None, sleep=lambda _s: None)
    assert any(m == "PUT" and p == "/connectors/vlp-snowflake-sink" for m, p, _ in api.calls)


def test_run_sets_public_key_and_never_logs_private_key(monkeypatch, capsys):
    executed = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def execute(self, sql):
            executed.append(sql)

        def fetchall(self):
            return [("RSA_PUBLIC_KEY_FP", "SHA256:abc")]

    class Conn:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def cursor(self):
            return Cursor()

    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "org-acct")
    monkeypatch.setenv("SNOWFLAKE_PAT", "t")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    api = FakeConnectApi()
    logs = []
    connector.run(log=logs.append, connect_snowflake=Conn, api=api)
    created = next(body for m, p, body in api.calls if m == "POST")
    private_key = created["snowflake.private.key"]
    assert created["snowflake.url.name"] == "org-acct.snowflakecomputing.com"
    assert executed[0].startswith("ALTER USER SVC_AIVEN_CONNECTOR SET RSA_PUBLIC_KEY = 'MII")
    assert private_key not in "\n".join(logs)
    out = capsys.readouterr().out
    assert out.count(private_key) == 1 and f"::add-mask::{private_key}" in out  # only the mask command
    assert json.dumps(created)  # serialisable for the API
