"""Anomalies on shipments, CRM leads, tariffs and partner settlements."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from veloparc_gen.anomalies.base import Injection, Injector, register
from veloparc_gen.sources import tariffs as tariff_source
from veloparc_gen.sources.reference import COUNTRY_ALIASES
from veloparc_gen.world import Record, Tariff, World, at_utc, money

# Non-ISO spellings, some resolvable through REF.COUNTRY_ALIAS, some not (typos).
_COUNTRY_VARIANTS = {
    "PT": ["Portugal", "portugal ", "Portugual", "PRT"],
    "ES": ["Spain", "España", "Espanha", "Spian"],
    "DE": ["Germany", "Deutschland", "Alemanha", "Deutchland"],
    "PL": ["Poland", "Polska", "Polónia", "Polnd"],
}


@register
class ShipmentExtraUnit(Injector):
    name = "shipment_extra_unit"

    def eligible(self, world):
        return [r for r in world.records["veloship_shipment"] if not r.dropped]

    def locks(self, item):
        return [("msg", item.message_id)]

    def inject(self, world, shipment, rng):
        weight = round(float(rng.uniform(0.3, 5.0)), 1)
        shipment.extra_units.append(weight)
        n = len(shipment.parcels)
        return [
            Injection(
                business_key=shipment.message_id,
                country=shipment.country,
                event_date=shipment.ship_date,
                details={"ShipmentUnit": n + 1, "ParcelData": n, "extra_unit_weight": weight},
                records=[shipment],
            )
        ]


@register
class LeadCountryNotIso2(Injector):
    name = "lead_country_not_iso2"

    def eligible(self, world):
        return [r for r in world.records["crm_object"] if r.data["attributes"]["type"] == "Lead"]

    def locks(self, item):
        return [("crm", item.key)]

    def inject(self, world, lead, rng):
        original = lead.data["Country"]
        variants = _COUNTRY_VARIANTS.get(original, [original.lower()])
        injected = variants[int(rng.integers(len(variants)))]
        lead.data = {**lead.data, "Country": injected}
        resolvable = injected.strip().upper() in COUNTRY_ALIASES.get(original, [])
        return [
            Injection(
                business_key=lead.key,
                country=lead.country,
                event_date=lead.source_ts.date(),
                details={
                    "field": "Country",
                    "original": original,
                    "injected": injected,
                    "resolvable_via_country_alias": resolvable,
                },
                records=[lead],
            )
        ]


@register
class OverlappingTariff(Injector):
    """A new version is published while the open-ended one is still valid: two tariffs for the same
    (contactId, product, zone) on the same days."""

    name = "overlapping_tariff"

    def eligible(self, world):
        return [
            versions[0]
            for _, versions in sorted(world.tariffs.items())
            if len(versions) == 1 and versions[0].valid_to is None
        ]

    def locks(self, item):
        return [("tariff", item.contact_id, item.product, item.zone)]

    def inject(self, world, tariff, rng):
        days = world.window_days
        start = max(days[int(rng.integers(len(days)))], tariff.valid_from + timedelta(days=1))
        uplift = Decimal("1.03")
        overlap = Tariff(
            tariff_id=tariff.tariff_id[:-4] + "0002",
            contact_id=tariff.contact_id,
            product=tariff.product,
            zone=tariff.zone,
            currency=tariff.currency,
            valid_from=start,
            valid_to=None,
            fuel_pct=tariff.fuel_pct,
            bands=[(lo, hi, money(p * uplift)) for lo, hi, p in tariff.bands],
        )
        published = max(at_utc(start - timedelta(days=5), 9.0), world.initial_load_at)
        record = world.add(
            Record(
                source="tariff",
                key=overlap.tariff_id,
                source_ts=published,
                arrival=world.arrival_after(published, rng),
                country=world.customers[tariff.contact_id].country,
                business_key=overlap.tariff_id,
                data=tariff_source.payload(overlap),
            )
        )
        return [
            Injection(
                business_key=overlap.tariff_id,
                country=record.country,
                event_date=start,
                details={
                    "overlaps": tariff.tariff_id,
                    "open_since": tariff.valid_from.isoformat(),
                    "new_valid_from": start.isoformat(),
                },
                records=[record],
                related_rules=["R8"],  # a lookup by date in silver finds two tariffs
            )
        ]


@register
class SettlementCountMismatch(Injector):
    name = "settlement_count_mismatch"

    def eligible(self, world):
        return list(world.records.get("partner_settlement", []))

    def locks(self, item):
        return [("stl", item.key)]

    def inject(self, world: World, settlement, rng):
        original = settlement.data["deliveredParcels"]
        delta = max(1, round(original * float(rng.uniform(0.03, 0.08))))
        settlement.data = {**settlement.data, "deliveredParcels": original + delta}
        period = settlement.data["period"]
        return [
            Injection(
                business_key=settlement.key,
                country=settlement.country,
                event_date=date.fromisoformat(period + "-01"),
                details={
                    "field": "deliveredParcels",
                    "original": original,
                    "injected": original + delta,
                    "partnerId": settlement.data["partnerId"],
                    "zone": settlement.data["zone"],
                },
                records=[settlement],
            )
        ]
