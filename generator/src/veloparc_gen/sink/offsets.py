"""Next offset per topic and partition, persisted in state/offsets.json between runs."""

from __future__ import annotations

import json
from pathlib import Path


class OffsetStore:
    def __init__(self, path: Path):
        self.path = path
        self.next: dict[str, dict[str, int]] = {}
        if path.exists():
            self.next = json.loads(path.read_text(encoding="utf-8"))

    def take(self, topic: str, partition: int) -> int:
        partitions = self.next.setdefault(topic, {})
        offset = partitions.get(str(partition), 0)
        partitions[str(partition)] = offset + 1
        return offset

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        ordered = {t: dict(sorted(p.items(), key=lambda kv: int(kv[0]))) for t, p in sorted(self.next.items())}
        self.path.write_text(json.dumps(ordered, indent=2) + "\n", encoding="utf-8")
