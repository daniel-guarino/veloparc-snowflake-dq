"""Connection settings for Aiven Kafka and Schema Registry, read from the environment (GitHub secrets).

Required variables: KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, KAFKA_SSL_CA_PEM
(the CA certificate text), SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_BASIC_AUTH (user:password).
Values are never printed.
"""

from __future__ import annotations

import base64
import json
import os
import re
import tempfile
import urllib.request
from pathlib import Path

REQUIRED = (
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_SASL_USERNAME",
    "KAFKA_SASL_PASSWORD",
    "KAFKA_SSL_CA_PEM",
)
_PEM = re.compile(r"-----BEGIN CERTIFICATE-----(.*?)-----END CERTIFICATE-----", re.S)


class MissingSettings(RuntimeError):
    pass


def normalize_pem(text: str) -> str:
    """Rebuilds the certificate(s) even if the newlines were lost or escaped when the secret was pasted."""
    text = text.replace("\\n", "\n")
    blocks = _PEM.findall(text)
    if not blocks:
        raise MissingSettings("KAFKA_SSL_CA_PEM does not contain a PEM certificate")
    out = []
    for body in blocks:
        b64 = "".join(body.split())
        lines = [b64[i : i + 64] for i in range(0, len(b64), 64)]
        out.append("-----BEGIN CERTIFICATE-----\n" + "\n".join(lines) + "\n-----END CERTIFICATE-----\n")
    return "".join(out)


def kafka_config(env: dict[str, str] | None = None, ca_dir: Path | None = None) -> dict[str, str]:
    """librdkafka configuration for SASL_SSL + SCRAM-SHA-256, with the CA written to a private temp file."""
    env = dict(os.environ if env is None else env)
    missing = [name for name in REQUIRED if not env.get(name)]
    if missing:
        raise MissingSettings(f"missing environment variables: {', '.join(missing)}")
    ca_dir = ca_dir or Path(tempfile.mkdtemp(prefix="vlp-kafka-"))
    ca_path = ca_dir / "ca.pem"
    ca_path.write_text(normalize_pem(env["KAFKA_SSL_CA_PEM"]), encoding="utf-8")
    ca_path.chmod(0o600)
    return {
        "bootstrap.servers": env["KAFKA_BOOTSTRAP_SERVERS"].strip(),
        "security.protocol": env.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
        "sasl.mechanism": env.get("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
        "sasl.username": env["KAFKA_SASL_USERNAME"].strip(),
        "sasl.password": env["KAFKA_SASL_PASSWORD"].strip(),
        "ssl.ca.location": str(ca_path),
    }


def schema_registry_get(path: str, env: dict[str, str] | None = None, timeout: float = 15.0):
    env = dict(os.environ if env is None else env)
    url, auth = env.get("SCHEMA_REGISTRY_URL", "").rstrip("/"), env.get("SCHEMA_REGISTRY_BASIC_AUTH", "")
    if not url or ":" not in auth:
        raise MissingSettings("SCHEMA_REGISTRY_URL and SCHEMA_REGISTRY_BASIC_AUTH (user:password) are required")
    request = urllib.request.Request(url + path)  # noqa: S310 (https URL from configuration)
    request.add_header("Authorization", "Basic " + base64.b64encode(auth.strip().encode()).decode())
    with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
        return json.loads(response.read())


def check(timeout: float = 20.0) -> dict:
    """Connects to the broker and the Schema Registry; returns what it could see (no secret values)."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient(kafka_config())
    metadata = admin.list_topics(timeout=timeout)
    topics = sorted(t for t in metadata.topics if not t.startswith("_") and not t.startswith("__"))
    subjects = schema_registry_get("/subjects")
    return {
        "brokers": len(metadata.brokers),
        "topics": {t: len(metadata.topics[t].partitions) for t in topics},
        "schema_registry_subjects": sorted(subjects),
    }
