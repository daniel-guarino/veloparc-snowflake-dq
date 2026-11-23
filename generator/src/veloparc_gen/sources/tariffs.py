"""Step 2 · tariffs per customer x product x zone: contiguous bands, increasing price, no overlap (R14)."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from veloparc_gen.world import Batch, Record, Tariff, World, money, num

SOURCE = "tariff"


def build(world: World) -> None:
    sim = world.sim
    cfg = sim.tariffs
    for customer in world.customers.values():
        rng = world.rng("tariffs", customer.contact_id)
        currency = world.currency(customer.country)
        to_currency = Decimal("1") / Decimal(str(sim.fx[currency].start)) if currency != "EUR" else Decimal("1")
        discount = rng.uniform(cfg.customer_discount.min, cfg.customer_discount.max)
        for product in customer.products:
            bands_cfg = cfg.bands[product]
            fuel = Decimal(str(round(rng.uniform(cfg.fuel_surcharge_pct.min, cfg.fuel_surcharge_pct.max) * 2) / 2))
            changes = rng.random() < cfg.price_change_share and customer.first_seen.date() < cfg.price_change_date
            for zone in world.zones(customer.country):
                factor = Decimal(str(cfg.zone_factor[zone.split("-")[1]] * (1 - discount)))
                prices = [money(Decimal(str(p)) * factor * to_currency) for p in bands_cfg.base_price_eur]
                versions = [(customer.first_seen.date(), None, prices)]
                if changes:
                    uplift = Decimal(str(round(rng.uniform(1.02, 1.06), 4)))
                    versions = [
                        (customer.first_seen.date(), cfg.price_change_date - timedelta(days=1), prices),
                        (cfg.price_change_date, None, [money(p * uplift) for p in prices]),
                    ]
                for seq, (valid_from, valid_to, band_prices) in enumerate(versions, start=1):
                    limits = [Decimal("0")] + [Decimal(str(x)) for x in bands_cfg.limits_kg]
                    tariff = Tariff(
                        tariff_id=f"TRF-{customer.contact_id}-{product}-{zone.split('-')[1]}-{seq:04d}",
                        contact_id=customer.contact_id,
                        product=product,
                        zone=zone,
                        currency=currency,
                        valid_from=valid_from,
                        valid_to=valid_to,
                        fuel_pct=fuel,
                        bands=[(limits[i], limits[i + 1], band_prices[i]) for i in range(len(band_prices))],
                    )
                    world.tariffs.setdefault((customer.contact_id, product, zone), []).append(tariff)
    _records(world)


def payload(t: Tariff) -> dict:
    return {
        "tariffId": t.tariff_id,
        "contactId": t.contact_id,
        "product": t.product,
        "zone": t.zone,
        "currency": t.currency,
        "validFrom": t.valid_from.isoformat(),
        "validTo": t.valid_to.isoformat() if t.valid_to else None,
        "fuelSurchargePct": num(t.fuel_pct),
        "weightBands": [{"fromKg": num(lo), "toKg": num(hi), "price": num(p)} for lo, hi, p in t.bands],
    }


def _records(world: World) -> None:
    """All tariffs (including planned future versions) are in the initial load of the pricing extract."""
    rng = world.rng("tariff_records")
    tariffs = [t for versions in world.tariffs.values() for t in versions]
    batch_id = world.uuid4(rng)
    for seq, t in enumerate(tariffs, start=1):
        world.add(
            Record(
                source=SOURCE,
                key=t.tariff_id,
                source_ts=world.initial_load_at,
                arrival=world.initial_load_at + timedelta(milliseconds=seq),
                country=world.customers[t.contact_id].country,
                business_key=t.tariff_id,
                op="R",
                data=payload(t),
                batch=Batch(batch_id, seq, len(tariffs)),
            )
        )
