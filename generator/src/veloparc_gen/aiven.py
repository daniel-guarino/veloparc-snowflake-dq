"""Minimal Aiven REST API client (topics, ACLs, service info), used by `veloparc-gen kafka-setup`.

Reads AIVEN_API_TOKEN (secret), AIVEN_PROJECT and AIVEN_SERVICE (variables) from the environment.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

API = "https://api.aiven.io/v1"

Transport = Callable[[str, str, dict | None], Any]


class AivenError(RuntimeError):
    pass


def _http(method: str, url: str, body: dict | None, token: str) -> Any:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method)  # noqa: S310 (fixed https API base)
    request.add_header("authorization", f"aivenv1 {token}")
    request.add_header("content-type", "application/json")
    try:
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")[:500]
        raise AivenError(f"{method} {url.removeprefix(API)} -> HTTP {exc.code}: {detail}") from exc
    return json.loads(raw) if raw else {}


class AivenApi:
    def __init__(self, project: str, service: str, token: str = "", transport: Transport | None = None):
        missing = [name for name, value in (("AIVEN_PROJECT", project), ("AIVEN_SERVICE", service)) if not value]
        if missing:
            raise AivenError(
                f"missing: {', '.join(missing)} (GitHub Environment dev: Variables tab, or a secret with that name)"
            )
        self.base = f"{API}/project/{project}/service/{service}"
        if transport is None:
            if not token:
                raise AivenError("AIVEN_API_TOKEN is required")
            transport = lambda method, url, body: _http(method, url, body, token)  # noqa: E731
        self._call = transport

    @classmethod
    def from_env(cls) -> AivenApi:
        return cls(
            os.environ.get("AIVEN_PROJECT", "").strip(),
            os.environ.get("AIVEN_SERVICE", "").strip(),
            os.environ.get("AIVEN_API_TOKEN", "").strip(),
        )

    def service(self) -> dict:
        return self._call("GET", self.base, None).get("service", {})

    def topics(self) -> dict[str, dict]:
        return {t["topic_name"]: t for t in self._call("GET", f"{self.base}/topic", None).get("topics", [])}

    def create_topic(self, name: str, partitions: int, replication: int, config: dict) -> None:
        body: dict = {"topic_name": name, "partitions": partitions, "replication": replication}
        if config:
            body["config"] = config
        self._call("POST", f"{self.base}/topic", body)

    def update_service_config(self, user_config: dict) -> None:
        """Advanced configuration of the service (only the keys given are changed)."""
        self._call("PUT", self.base, {"user_config": user_config})

    def update_topic(self, name: str, partitions: int, config: dict) -> None:
        self._call("PUT", f"{self.base}/topic/{name}", {"partitions": partitions, "config": config})

    def acls(self) -> list[dict]:
        return list(self.service().get("acl", []))

    def add_acl(self, username: str, topic: str, permission: str) -> None:
        body = {"username": username, "topic": topic, "permission": permission}
        self._call("POST", f"{self.base}/acl", body)

    # ---- Kafka Connect (call on the Kafka Connect service)
    def connectors(self) -> list[dict]:
        return list(self._call("GET", f"{self.base}/connectors", None).get("connectors", []))

    def create_connector(self, config: dict) -> None:
        self._call("POST", f"{self.base}/connectors", config)

    def update_connector(self, name: str, config: dict) -> None:
        self._call("PUT", f"{self.base}/connectors/{name}", config)

    def connector_status(self, name: str) -> dict:
        return self._call("GET", f"{self.base}/connectors/{name}/status", None).get("status", {})
