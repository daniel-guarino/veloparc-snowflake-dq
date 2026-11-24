"""Kafka publishing (dry-run producer) and the Aiven setup, without any network access."""

import json
from datetime import UTC, date, datetime

import pytest
from conftest import small_config

from veloparc_gen import kafka_setup
from veloparc_gen.aiven import AivenApi
from veloparc_gen.run import publish
from veloparc_gen.sink.envelope import HEADER_NAMES
from veloparc_gen.sink.kafka import DryRunProducer, producer_config
from veloparc_gen.sink.murmur2 import partition_for


@pytest.fixture(scope="module")
def published(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("publish")
    config = small_config(start=date(2026, 11, 2), end=date(2026, 11, 3))
    producer = DryRunProducer({t.name: t.partitions for t in config.topics.values()})
    result = publish(config, tmp / "out", tmp / "state", producer=producer, log=lambda *_: None)
    return config, producer, result


def test_producer_config_matches_the_simulated_broker():
    cfg = producer_config({"bootstrap.servers": "x:1"})
    assert cfg["partitioner"] == "murmur2_random"
    assert cfg["acks"] == "all" and cfg["enable.idempotence"] is True


def test_messages_have_key_partition_headers_and_json_payloads(published):
    config, producer, result = published
    by_name = {t.name: t for t in config.topics.values()}
    kafka_topics = {t.name for t in config.topics.values() if t.transport == "kafka"} - {"vlp.dlq.s3-sink.v1"}
    assert {topic for topic, *_ in producer.sent} <= kafka_topics
    truncated = 0
    for topic, partition, _offset, key, value, headers in producer.sent:
        assert partition == partition_for(key.decode(), by_name[topic].partitions)
        assert [name for name, _ in headers] == list(HEADER_NAMES)
        assert dict(headers)["vlp_op"] != b"R"  # snapshots stay in the backfill
        try:
            assert isinstance(json.loads(value), dict)
        except json.JSONDecodeError:
            truncated += 1  # truncated DLQ copy, on purpose
    assert truncated == result.run_manifest["kafka"]["at_least_once"]["dlq_copies"]


def test_only_records_arriving_in_the_window_are_published(published):
    _, producer, result = published
    for *_rest, headers in producer.sent:
        ce_time = datetime.fromisoformat(dict(headers)["ce_time"].decode().replace("Z", "+00:00"))
        assert ce_time < datetime(2026, 11, 4, tzinfo=UTC)
    manifest = result.run_manifest
    assert manifest["mode"] == "publish"
    assert manifest["kafka"]["failures"] == 0
    assert manifest["rules"]["unexplained"] == []


def test_manifest_points_to_kafka_offsets(published):
    _, producer, result = published
    sent = {(t, p, o) for t, p, o, *_ in producer.sent}
    lines = [json.loads(x) for x in (result.run_dir / "anomaly_manifest.jsonl").read_text().splitlines()]
    kafka_refs = [loc for line in lines for loc in line["landing"] if "offset" in loc]
    assert kafka_refs, "expected at least one anomaly on a kafka topic in the window"
    for loc in kafka_refs:
        assert (loc["topic"], loc["partition"], loc["offset"]) in sent


class FakeApi(AivenApi):
    def __init__(self, existing_topics=(), acls=(), node_count=3, plan="business-4"):
        self.calls = []
        self.node_count = node_count
        self.plan = plan
        self._topics = {t: {"topic_name": t, "partitions": 2} for t in existing_topics}
        self._acls = list(acls)
        super().__init__("proj", "svc", transport=self._transport)

    def _transport(self, method, url, body):
        self.calls.append((method, url.split("/service/svc")[-1], body))
        if method == "GET" and url.endswith("/topic"):
            return {"topics": list(self._topics.values())}
        if method == "GET":
            return {"service": {"plan": self.plan, "node_count": self.node_count, "acl": self._acls}}
        return {}


def test_kafka_setup_is_idempotent():
    config = small_config()
    api = FakeApi(
        existing_topics=["vlp.ops.tracking-event.v1"],
        acls=[{"username": "gen", "topic": "vlp.ops.tracking-event.v1", "permission": "write"}],
    )
    actions = kafka_setup.ensure_topics(api, config, log=lambda *_: None)
    assert actions["vlp.ops.tracking-event.v1"] == "config updated"
    created = [b for m, path, b in api.calls if m == "POST" and path == "/topic"]
    assert len(created) == 4  # shipments, invoices, CRM, DLQ
    assert all(b["config"]["message_timestamp_type"] == "LogAppendTime" for b in created)
    assert all(b["partitions"] == 2 and b["replication"] == 3 for b in created)
    added = kafka_setup.ensure_producer_acl(api, config, "gen", log=lambda *_: None)
    assert "vlp.ops.tracking-event.v1" not in added and len(added) == 4


def test_register_schemas_as_json_with_backward_compatibility():
    config = small_config()
    calls = []

    def fake_request(method, path, body=None):
        calls.append((method, path, body))
        return {"id": len(calls)}

    versions = kafka_setup.register_schemas(config, request=fake_request, log=lambda *_: None)
    assert set(versions) == {
        "vlp.shipping.veloship-shipment.v1-value",
        "vlp.ops.tracking-event.v1-value",
        "vlp.finance.invoice.v1-value",
        "vlp.sales.crm-object.v1-value",
    }
    posts = [b for m, p, b in calls if m == "POST"]
    assert all(b["schemaType"] == "JSON" and json.loads(b["schema"])["$schema"] for b in posts)
    assert all(b == {"compatibility": "BACKWARD"} for m, p, b in calls if m == "PUT")


def test_single_node_service_gets_replication_one():
    api = FakeApi(node_count=1)
    kafka_setup.ensure_topics(api, small_config(), log=lambda *_: None)
    created = [b for m, path, b in api.calls if m == "POST" and path == "/topic"]
    assert created and all(b["replication"] == 1 for b in created)


def test_developer_plan_gets_replication_one_even_with_more_nodes():
    assert kafka_setup.replication_for({"plan": "developer-4", "node_count": 3}) == 1
    assert kafka_setup.replication_for({"plan": "business-4", "node_count": 3}) == 3
    assert kafka_setup.replication_for({"plan": "startup-2", "node_count": 2}) == 2


def test_retries_with_one_replica_when_the_plan_refuses_more():
    from veloparc_gen.aiven import AivenError

    class StrictApi(FakeApi):
        def _transport(self, method, url, body):
            if method == "POST" and url.endswith("/topic") and body["replication"] != 1:
                raise AivenError("HTTP 400: Replication factor for developer tier cannot be different than 1")
            return super()._transport(method, url, body)

    api = StrictApi(node_count=3)
    kafka_setup.ensure_topics(api, small_config(), log=lambda *_: None)
    created = [b for m, path, b in api.calls if m == "POST" and path == "/topic"]
    assert len(created) == 5 and all(b["replication"] == 1 for b in created)


def test_plan_policy_values_are_applied_and_retried():
    from veloparc_gen.aiven import AivenError

    class PolicyApi(FakeApi):
        def _transport(self, method, url, body):
            if method in ("POST", "PUT") and "/topic" in url and body["config"].get("retention_ms") != 259200000:
                raise AivenError(
                    'HTTP 400: {"error_code":"kafka_policy_violation",'
                    '"message":"retention.ms must be 259200000 ms (4320 minutes)."}'
                )
            return super()._transport(method, url, body)

    api = PolicyApi(existing_topics=["vlp.ops.tracking-event.v1"], node_count=1)
    kafka_setup.ensure_topics(api, small_config(), log=lambda *_: None)
    accepted = [b for m, path, b in api.calls if m in ("POST", "PUT") and "/topic" in path]
    assert len(accepted) == 5
    assert all(b["config"]["retention_ms"] == 259200000 for b in accepted)
    assert all(b["config"]["message_timestamp_type"] == "LogAppendTime" for b in accepted)


def test_plan_policy_accepts_text_values():
    from veloparc_gen.aiven import AivenError

    class TextPolicyApi(FakeApi):
        def _transport(self, method, url, body):
            if method == "POST" and url.endswith("/topic") and body["config"].get("cleanup_policy") != "delete":
                raise AivenError(
                    'HTTP 400: {"error_code":"kafka_policy_violation","message":"cleanup.policy must be delete."}'
                )
            return super()._transport(method, url, body)

    api = TextPolicyApi(node_count=1)
    kafka_setup.ensure_topics(api, small_config(), log=lambda *_: None)
    created = [b for m, path, b in api.calls if m == "POST" and path == "/topic"]
    assert len(created) == 5 and all(b["config"]["cleanup_policy"] == "delete" for b in created)


def test_developer_plan_creates_topics_without_custom_config():
    from veloparc_gen.aiven import AivenError

    class DevTierApi(FakeApi):
        def _transport(self, method, url, body):
            if method == "POST" and url.endswith("/topic") and "config" in body:
                raise AivenError("HTTP 400: kafka_policy_violation: unchecked configurations: message.timestamp.type")
            if method == "PUT" and url.endswith("/service/svc"):
                raise AivenError("HTTP 400: not allowed on this plan")
            return super()._transport(method, url, body)

    api = DevTierApi(existing_topics=["vlp.ops.tracking-event.v1"], node_count=2, plan="developer-2-3d")
    lines = []
    actions = kafka_setup.ensure_topics(api, small_config(), log=lines.append)
    created = [b for m, path, b in api.calls if m == "POST" and path == "/topic"]
    assert len(created) == 4 and all("config" not in b and b["replication"] == 1 for b in created)
    assert actions["vlp.ops.tracking-event.v1"] == "exists (plan defaults)"
    assert not [c for c in api.calls if c[0] == "PUT" and c[1].startswith("/topic")]
    assert any("WARNING" in line and "LogAppendTime" in line for line in lines)  # refused, but not fatal


def test_developer_plan_sets_log_append_time_at_service_level():
    api = FakeApi(node_count=2, plan="developer-2-3d")
    kafka_setup.ensure_topics(api, small_config(), log=lambda *_: None)
    assert ("PUT", "", {"user_config": {"kafka": {"log_message_timestamp_type": "LogAppendTime"}}}) in api.calls


def test_schema_registry_refusal_is_a_warning():
    def refusing(method, path, body=None):
        raise RuntimeError("HTTP 422: Invalid schema")

    lines = []
    versions = kafka_setup.register_schemas(small_config(), request=refusing, log=lines.append)
    assert versions == {} and sum("WARNING" in line for line in lines) == 4
