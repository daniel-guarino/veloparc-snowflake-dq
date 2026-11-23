"""Anomaly injectors: one per row of the anomalies table in contracts/README.md (see anomalies.yaml)."""

from veloparc_gen.anomalies import billing, other, tracking  # noqa: F401  (registers the injectors)
from veloparc_gen.anomalies.base import REGISTRY, AnomalyRun, ManifestEntry, inject_all

__all__ = ["REGISTRY", "AnomalyRun", "ManifestEntry", "inject_all"]
