"""Idempotent Kafka setup on Aiven: topics (from topics.yaml, transport=kafka), producer ACL, JSON Schemas.

Run by the `kafka-setup` workflow. Creates what is missing, updates topic configs that drifted, and never
deletes anything. Prints names and settings only, never credentials.
"""

from __future__ import annotations

import base64
import json
import os
import re
import urllib.error
import urllib.request

from veloparc_gen.aiven import AivenApi, AivenError
from veloparc_gen.config import Config
from veloparc_gen.kafka_conn import schema_registry_endpoint


def desired_topics(config: Config) -> dict[str, dict]:
    """Topic name -> {partitions, config} for every topic that travels through Kafka."""
    out = {}
    for topic in config.topics.values():
        if topic.transport != "kafka":
            continue
        settings = {"message_timestamp_type": "LogAppendTime", "cleanup_policy": topic.cleanup_policy}
        if topic.retention_hours is not None:
            settings["retention_ms"] = topic.retention_hours * 3_600_000
        if topic.max_message_bytes is not None:
            settings["max_message_bytes"] = topic.max_message_bytes
        out[topic.name] = {"partitions": topic.partitions, "config": settings}
    return out


SINGLE_REPLICA_PLANS = ("free", "developer", "hobbyist")


def replication_for(service: dict) -> int:
    """One replica per node (max 3); free/developer plans only accept 1, whatever node_count says."""
    plan = str(service.get("plan") or "").lower()
    if plan.startswith(SINGLE_REPLICA_PLANS):
        return 1
    return max(1, min(3, int(service.get("node_count") or 1)))


_POLICY = re.compile(r"([a-z][a-z0-9.]*) must be (\d+)")


def _apply_policy(api_call, config: dict, log) -> dict:
    """Runs api_call(config). If the plan's policy imposes a value ("retention.ms must be 259200000"), uses that
    value, logs the adjustment and retries. Returns the config that was accepted."""
    config = dict(config)
    for _ in range(5):
        try:
            api_call(config)
            return config
        except AivenError as exc:
            match = _POLICY.search(str(exc))
            if "kafka_policy_violation" not in str(exc) or not match:
                raise
            key, value = match.group(1).replace(".", "_"), int(match.group(2))
            if config.get(key) == value:
                raise
            log(f"  plan policy: {key} {config.get(key)} -> {value}")
            config[key] = value
    raise AivenError("topic config still refused after applying the plan's policy")


def ensure_topics(api: AivenApi, config: Config, log=print) -> dict[str, str]:
    existing = api.topics()
    service = api.service()
    replication = replication_for(service)
    log(f"service plan {service.get('plan', '?')}, {service.get('node_count', '?')} node(s): replication {replication}")
    actions = {}
    for name, spec in desired_topics(config).items():
        if name not in existing:

            def create(settings, name=name, spec=spec):
                nonlocal replication
                try:
                    api.create_topic(name, spec["partitions"], replication, settings)
                except AivenError as exc:
                    if replication == 1 or "cannot be different than 1" not in str(exc):
                        raise
                    replication = 1  # the plan only accepts one replica: keep it for the remaining topics
                    api.create_topic(name, spec["partitions"], replication, settings)

            _apply_policy(create, spec["config"], log)
            actions[name] = f"created ({spec['partitions']} partitions, replication {replication})"
        else:
            partitions = max(int(existing[name].get("partitions") or 0), spec["partitions"])
            _apply_policy(lambda settings, n=name, p=partitions: api.update_topic(n, p, settings), spec["config"], log)
            actions[name] = "config updated"
        log(f"topic {name}: {actions[name]}")
    return actions


def ensure_producer_acl(api: AivenApi, config: Config, username: str, log=print) -> list[str]:
    """`write` on each Kafka topic for the generator's service user (no admin rights)."""
    if not username:
        log("acl: KAFKA_GENERATOR_USER not set, skipped")
        return []
    have = {(a.get("username"), a.get("topic"), a.get("permission")) for a in api.acls()}
    added = []
    for name in desired_topics(config):
        if (username, name, "write") in have or (username, name, "readwrite") in have:
            continue
        api.add_acl(username, name, "write")
        added.append(name)
    log(f"acl: write for {username} on {len(desired_topics(config))} topics ({len(added)} added)")
    return added


def _sr_request(method: str, path: str, body: dict | None = None):
    base, auth = schema_registry_endpoint()
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(base + path, data=data, method=method)  # noqa: S310
    request.add_header("Authorization", "Basic " + base64.b64encode(auth.encode()).decode())
    request.add_header("Content-Type", "application/vnd.schemaregistry.v1+json")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            raw = response.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{method} {path} -> HTTP {exc.code}: {exc.read().decode(errors='replace')[:500]}") from exc
    return json.loads(raw) if raw else {}


def register_schemas(config: Config, request=_sr_request, log=print) -> dict[str, int]:
    """Registers contracts/schemas/<schema> as `<topic>-value` (JSON, BACKWARD compatibility)."""
    versions = {}
    for topic in config.topics.values():
        if topic.transport != "kafka" or not topic.schema_file:
            continue
        subject = f"{topic.name}-value"
        schema = (config.schema_dir / topic.schema_file).read_text(encoding="utf-8")
        request("PUT", f"/config/{subject}", {"compatibility": "BACKWARD"})
        result = request("POST", f"/subjects/{subject}/versions", {"schemaType": "JSON", "schema": schema})
        versions[subject] = result.get("id", 0)
        log(f"schema {subject}: id {versions[subject]}")
    return versions


def run(config: Config, api: AivenApi | None = None, log=print) -> dict:
    api = api or AivenApi.from_env()
    return {
        "topics": ensure_topics(api, config, log),
        "acl_added": ensure_producer_acl(api, config, os.environ.get("KAFKA_GENERATOR_USER", "").strip(), log),
        "schemas": register_schemas(config, log=log),
    }
