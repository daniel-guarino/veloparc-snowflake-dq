"""Phase 3c: deploy the Aiven Snowflake sink connector (Snowpipe Streaming, connector 3.x).

1. Generate a fresh RSA key pair in memory (the private key is never written to disk or printed).
2. Set the public key on the Snowflake service user SVC_AIVEN_CONNECTOR (as SVC_VELOPARC_CI / VLP_DEPLOYER).
3. Render connect/snowflake_sink.json with the account URL and the private key, then create or update the
   connector on the Kafka Connect service through the Aiven API.
4. Wait until the connector and its tasks are RUNNING; fail with the task trace otherwise.

Re-running rotates the key: the old public key is replaced and the connector is updated with the new one.
"""

from __future__ import annotations

import base64
import json
import os
import re
import time
from collections.abc import Callable
from pathlib import Path

from veloparc_gen.aiven import AivenApi, AivenError
from veloparc_gen.config import REPO_ROOT

TEMPLATE = REPO_ROOT / "connect" / "snowflake_sink.json"
CONNECTOR_USER = "SVC_AIVEN_CONNECTOR"
_PLACEHOLDER = re.compile(r"\$\{([A-Z_]+)\}")


class ConnectorFailed(RuntimeError):
    pass


def generate_key_pair() -> tuple[str, str]:
    """(private key PKCS#8 DER base64, public key SubjectPublicKeyInfo DER base64): the single-line formats that
    the connector (snowflake.private.key) and Snowflake (RSA_PUBLIC_KEY) expect."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private = key.private_bytes(
        serialization.Encoding.DER, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    public = key.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    return base64.b64encode(private).decode(), base64.b64encode(public).decode()


def mask_in_github_logs(value: str) -> None:
    """Ask GitHub Actions to mask a value generated at runtime (it is not a repository secret)."""
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print(f"::add-mask::{value}", flush=True)


def render(template: str, values: dict[str, str]) -> dict:
    missing = sorted(set(_PLACEHOLDER.findall(template)) - set(values))
    if missing:
        raise ConnectorFailed(f"no value for placeholders: {missing}")
    rendered = _PLACEHOLDER.sub(lambda m: values[m.group(1)], template)
    return json.loads(rendered)


def snowflake_url(account: str) -> str:
    return f"{account}.snowflakecomputing.com"


def wait_until_running(
    api: AivenApi, name: str, timeout: float = 240, interval: float = 10, sleep=time.sleep, log=print
) -> dict:
    deadline = time.monotonic() + timeout
    status: dict = {}
    while True:
        try:
            status = api.connector_status(name)
        except AivenError as exc:  # the status endpoint can 404 for a few seconds after creation
            log(f"  status not available yet ({exc})")
            status = {}
        tasks = status.get("tasks", [])
        failed = [t for t in tasks if t.get("state") == "FAILED"]
        if status.get("state") == "FAILED" or failed:
            trace = "\n".join(str(t.get("trace", ""))[:1500] for t in failed)
            raise ConnectorFailed(f"connector {name} failed: state={status.get('state')}\n{trace}")
        if status.get("state") == "RUNNING" and tasks and all(t.get("state") == "RUNNING" for t in tasks):
            log(f"connector {name}: RUNNING with {len(tasks)} task(s)")
            return status
        if time.monotonic() >= deadline:
            raise ConnectorFailed(f"connector {name} not RUNNING after {timeout:.0f}s: {status}")
        log(f"  connector {name}: {status.get('state', 'pending')}, waiting")
        sleep(interval)


def deploy(api: AivenApi, config: dict, log=print, **wait) -> dict:
    name = config["name"]
    if name in {c.get("name") for c in api.connectors()}:
        api.update_connector(name, config)
        log(f"connector {name}: updated")
    else:
        api.create_connector(config)
        log(f"connector {name}: created")
    return wait_until_running(api, name, log=log, **wait)


def run(
    log=print,
    connect_snowflake: Callable | None = None,
    api: AivenApi | None = None,
    template: Path = TEMPLATE,
) -> dict:
    from veloparc_gen.snowflake_conn import connect, connection_args

    account = connection_args()["account"]
    private_key, public_key = generate_key_pair()
    mask_in_github_logs(private_key)

    with (connect_snowflake or connect)() as conn, conn.cursor() as cur:
        cur.execute(f"ALTER USER {CONNECTOR_USER} SET RSA_PUBLIC_KEY = '{public_key}'")
        cur.execute(f"DESC USER {CONNECTOR_USER}")
        fingerprint = next((row[1] for row in cur.fetchall() if row[0] == "RSA_PUBLIC_KEY_FP"), "?")
    log(f"snowflake: public key set on {CONNECTOR_USER} ({fingerprint})")

    config = render(
        template.read_text(encoding="utf-8"),
        {"SNOWFLAKE_URL": snowflake_url(account), "SNOWFLAKE_PRIVATE_KEY": private_key},
    )
    if api is None:
        project = os.environ.get("AIVEN_PROJECT", "").strip()
        service = (os.environ.get("AIVEN_CONNECT_SERVICE") or os.environ.get("AIVEN_SERVICE", "")).strip()
        api = AivenApi(project, service, os.environ.get("AIVEN_API_TOKEN", "").strip())
    return deploy(api, config, log=log)
