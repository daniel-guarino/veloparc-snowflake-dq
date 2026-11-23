"""Step 9 · reference tables for dbt seeds: FX_RATE_DAILY, POSTAL_ZONE, COUNTRY_ALIAS."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from veloparc_gen.world import World, days

# Names and spellings seen in free-text CRM fields; the clean contract is ISO 3166-1 alpha-2.
COUNTRY_ALIASES = {
    "PT": ["PORTUGAL", "PORTUGUESE REPUBLIC", "PRT"],
    "ES": ["SPAIN", "ESPAÑA", "ESPANA", "ESPANHA", "ESP"],
    "DE": ["GERMANY", "DEUTSCHLAND", "ALEMANHA", "ALEMANIA", "DEU"],
    "PL": ["POLAND", "POLSKA", "POLÓNIA", "POLONIA", "POL"],
}


def build_fx(world: World) -> None:
    """Daily EUR rate per currency, random walk, from a week before the window to 3 months after it."""
    start = world.sim.window.start - timedelta(days=7)
    end = world.sim.window.end + timedelta(days=92)
    for currency, series in sorted(world.sim.fx.items()):
        rng = world.rng("fx", currency)
        rate = series.start
        for day in days(start, end):
            world.fx[(currency, day)] = Decimal(str(round(rate, 6)))
            rate *= float(1 + rng.normal(0, series.daily_sigma))
    world.reference["FX_RATE_DAILY"] = [
        {"rate_date": d.isoformat(), "currency": c, "rate_to_eur": str(r)} for (c, d), r in sorted(world.fx.items())
    ] + [{"rate_date": d.isoformat(), "currency": "EUR", "rate_to_eur": "1.000000"} for d in days(start, end)]


def build_tables(world: World) -> None:
    world.reference["POSTAL_ZONE"] = [
        {"country_code": c, "postal_prefix": p, "zone_code": z} for c, p, z in world.postal_zones
    ]
    world.reference["COUNTRY_ALIAS"] = [
        {"alias_norm": alias, "iso2": iso2} for iso2, aliases in COUNTRY_ALIASES.items() for alias in aliases
    ]
