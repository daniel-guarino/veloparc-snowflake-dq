"""A coherent world (no anomalies) satisfies every cross-source rule and every schema."""

from datetime import date

from conftest import small_config

from veloparc_gen.pipeline import build_world, check_world_rules
from veloparc_gen.validate import validate_world


def test_clean_world_meets_r1_to_r16(clean_world):
    violations = check_world_rules(clean_world)
    assert violations == [], violations[:10]


def test_clean_world_passes_schemas(clean_world):
    report = validate_world(clean_world)
    assert report.failures == []
    assert set(report.checked) == {
        "veloship_shipment",
        "tracking_event",
        "invoice",
        "crm_object",
        "partner_settlement",
        "tariff",
        "gl_extract",
        "master_data",
    }


def test_clean_world_has_every_source_and_path(clean_world):
    statuses = {e.status for e in clean_world.records["tracking_event"]}
    assert statuses == {
        "DATA_RECEIVED",
        "PICKUP",
        "HUB",
        "IN_DELIVERY",
        "DELIVERED",
        "NOT_DELIVERED",
        "PARCELSHOP",
        "CANCELLED",
    }
    doc_types = {r.doc_type for r in clean_world.records["invoice"]}
    assert doc_types == {"INVOICE", "CREDIT_NOTE"}


def test_two_currencies_meet_rules():
    """PLN exercises FX (R10, R13) and the PLN tariffs and settlements."""
    world = build_world(small_config(countries=("PT", "PL"), end=date(2026, 9, 2), anomalies_enabled=False))
    assert check_world_rules(world) == []
    validate_world(world)
    assert {r.currency for r in world.records["invoice"]} == {"EUR", "PLN"}


def test_every_zone_has_one_partner_and_a_depot(clean_world):
    for zone in clean_world.zones("PT"):
        assert zone in clean_world.zone_partner
        assert clean_world.depots_in("PT", zone, ("DEPOT",))
