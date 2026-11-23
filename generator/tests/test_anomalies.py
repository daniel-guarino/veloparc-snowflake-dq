"""Anomalies: exact counts, schema behaviour, and every rule violation explained by the answer key."""

from datetime import date

import pytest
from conftest import small_config

from veloparc_gen.anomalies import REGISTRY, inject_all
from veloparc_gen.config import Anomalies
from veloparc_gen.pipeline import build_world, check_world_rules
from veloparc_gen.validate import validate_world


def _boosted(config, minimum_rate=0.05):
    """Same config with higher rates, so a small world gets at least one of each anomaly."""
    boosted = {
        name: spec.model_copy(update={"rate": max(spec.rate, minimum_rate)}) if spec.rate is not None else spec
        for name, spec in config.anomalies.anomalies.items()
    }
    sim = config.simulation
    more_credit_notes = sim.invoicing.model_copy(update={"credit_note_invoice_share": 0.3})
    return config.model_copy(
        update={
            "anomalies": Anomalies(enabled=True, anomalies=boosted),
            "simulation": sim.model_copy(update={"invoicing": more_credit_notes}),
        }
    )


@pytest.fixture(scope="module")
def anomalous():
    config = _boosted(small_config(countries=("PT", "PL"), end=date(2026, 9, 10)), minimum_rate=0.3)
    world = build_world(config)
    run = inject_all(world)
    return config, world, run


def test_every_configured_anomaly_has_an_injector():
    config = small_config()
    assert set(config.anomalies.anomalies) == set(REGISTRY)


def test_counts_match_configured_rates(anomalous):
    config, _, run = anomalous
    for name, spec in config.anomalies.anomalies.items():
        stats = run.stats[name]
        expected = round(spec.rate * stats["eligible"]) if spec.rate is not None else min(spec.count, stats["eligible"])
        assert stats["selected"] == expected, name
        entries = [e for e in run.entries if e.anomaly_type == name]
        assert len(entries) == stats["manifest_entries"], name
        if name != "positive_credit_notes":  # one group -> one entry per credit note in it
            assert len(entries) == stats["selected"], name
        assert stats["selected"] >= 1, f"{name}: the test world should contain at least one"


def test_manifest_entries_have_required_fields(anomalous):
    _, _, run = anomalous
    required = {"anomaly_id", "rule", "dq_dimension", "source", "business_key", "country", "event_date", "description"}
    ids = [e.anomaly_id for e in run.entries]
    assert len(ids) == len(set(ids))
    for entry in run.entries:
        row = entry.as_dict()
        assert required <= row.keys()
        assert all(row[k] not in (None, "") for k in required)


def test_schema_gate_with_anomalies(anomalous):
    """Schema anomalies fail their schema, business-rule anomalies still pass it (validate_world raises if not)."""
    _, world, run = anomalous
    report = validate_world(world)
    expected_invalid = sum(len(e.injection.records) for e in run.entries if e.spec.schema_anomaly)
    assert sum(report.expected_invalid.values()) == expected_invalid > 0


def test_every_violation_is_explained_by_the_answer_key(anomalous):
    """No false positives: each violated rule is the rule of an injected anomaly or one of its related rules;
    and every injected business-rule anomaly is detected by its rule."""
    _, world, run = anomalous
    violations = check_world_rules(world)
    violated = {v.rule for v in violations}
    injected = {e.spec.rule for e in run.entries if e.spec.rule != "SCHEMA"}
    related = {r for e in run.entries for r in e.injection.related_rules}
    assert violated <= injected | related, violated - injected - related
    assert injected <= violated, injected - violated


def test_anomalies_off_means_empty_manifest():
    world = build_world(small_config(anomalies_enabled=False))
    run = inject_all(world)
    assert run.entries == []
    assert run.stats == {}
