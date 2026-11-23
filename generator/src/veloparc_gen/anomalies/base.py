"""Anomaly engine: runs one injector per entry of anomalies.yaml over the coherent world and keeps the answer key.

Each manifest entry says what was broken and how to find it again:
- rule / dq_dimension / source / business_key / country / event_date / description (the Phase 9 answer key);
- details: field changed, original and injected values;
- related_rules: other rules that are expected to fire as a consequence (e.g. a quarantined DELIVERED scan
  also makes the settlement count differ, R11), so a detection there is not a false positive;
- landing: topic, partition, offset and file of every affected message (filled in by the sink).
"""

from __future__ import annotations

from collections.abc import Hashable, Iterable
from dataclasses import dataclass, field
from datetime import date
from typing import Any, ClassVar

import numpy as np

from veloparc_gen.config import Anomaly
from veloparc_gen.world import Record, World


@dataclass
class Injection:
    """What one injector did to one picked item."""

    business_key: str
    country: str
    event_date: date
    details: dict
    records: list[Record]
    related_rules: list[str] = field(default_factory=list)


@dataclass
class ManifestEntry:
    anomaly_id: str
    anomaly_type: str
    spec: Anomaly
    injection: Injection
    landing: list[dict] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "anomaly_id": self.anomaly_id,
            "anomaly_type": self.anomaly_type,
            "rule": self.spec.rule,
            "dq_dimension": self.spec.dq_dimension,
            "source": self.spec.source,
            "business_key": self.injection.business_key,
            "country": self.injection.country,
            "event_date": self.injection.event_date.isoformat(),
            "description": self.spec.description,
            "schema_anomaly": self.spec.schema_anomaly,
            "related_rules": self.injection.related_rules,
            "details": self.injection.details,
            "landing": self.landing,
        }


class Injector:
    """Subclasses list the eligible items (in a deterministic order) and break one item at a time."""

    name: ClassVar[str]

    def eligible(self, world: World) -> list[Any]:
        raise NotImplementedError

    def locks(self, item: Any) -> Iterable[Hashable]:
        """Keys that no other anomaly may touch afterwards (keeps one anomaly per business object)."""
        raise NotImplementedError

    def inject(self, world: World, item: Any, rng: np.random.Generator) -> list[Injection]:
        raise NotImplementedError


REGISTRY: dict[str, type[Injector]] = {}


def register(cls: type[Injector]) -> type[Injector]:
    REGISTRY[cls.name] = cls
    return cls


@dataclass
class AnomalyRun:
    entries: list[ManifestEntry] = field(default_factory=list)
    stats: dict[str, dict] = field(default_factory=dict)


def inject_all(world: World) -> AnomalyRun:
    cfg = world.config.anomalies
    run = AnomalyRun()
    if not cfg.enabled:
        return run
    locked: set[Hashable] = set()
    for name, spec in cfg.anomalies.items():
        if name not in REGISTRY:
            raise KeyError(f"no injector registered for anomaly {name!r}")
        injector = REGISTRY[name]()
        pool = [item for item in injector.eligible(world) if locked.isdisjoint(injector.locks(item))]
        wanted = round(spec.rate * len(pool)) if spec.rate is not None else min(spec.count or 0, len(pool))
        rng = world.rng("anomaly", name)
        picked = sorted(int(i) for i in rng.choice(len(pool), size=wanted, replace=False)) if wanted else []
        entries = 0
        for i in picked:
            item = pool[i]
            if not locked.isdisjoint(injector.locks(item)):
                raise RuntimeError(f"{name}: eligible items share a lock; one anomaly per object is required")
            locked.update(injector.locks(item))
            for injection in injector.inject(world, item, rng):
                anomaly_id = f"ANM-{len(run.entries) + 1:06d}"
                for record in injection.records:
                    record.anomaly_ids.append(anomaly_id)
                    if spec.schema_anomaly:
                        record.schema_invalid = True
                run.entries.append(ManifestEntry(anomaly_id, name, spec, injection))
                entries += 1
        run.stats[name] = {
            "rule": spec.rule,
            "rate": spec.rate,
            "count": spec.count,
            "eligible": len(pool),
            "selected": wanted,
            "manifest_entries": entries,
        }
    return run
