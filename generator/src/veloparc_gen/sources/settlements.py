"""Step 7 · partner settlements per partner, zone and month, counted from DELIVERED scans (R11, R12)."""

from __future__ import annotations

from datetime import timedelta

from veloparc_gen.world import Batch, Record, World, at_utc, money, next_month_day, num, period_of

SOURCE = "partner_settlement"


def build(world: World) -> None:
    delivered: dict[tuple[str, str], int] = {}
    failed: dict[tuple[str, str], int] = {}
    for parcel in world.parcels.values():
        for event in parcel.events:
            if event.status in ("DELIVERED", "NOT_DELIVERED"):
                target = delivered if event.status == "DELIVERED" else failed
                key = (parcel.zone, period_of(event.local_date()))
                target[key] = target.get(key, 0) + 1

    rng = world.rng("settlements")
    by_period: dict[str, list[dict]] = {}
    for zone, period in sorted(delivered):
        partner = world.partners[world.zone_partner[zone]]
        count = delivered[(zone, period)]
        stops = min(count, max(1, round(count / partner.parcels_per_stop)))
        country = zone.split("-")[0]
        year, month = period.split("-")
        by_period.setdefault(period, []).append(
            {
                "settlementId": f"STL-{country}-{year}-{month}-{partner.partner_id}-{zone.split('-')[1]}",
                "partnerId": partner.partner_id,
                "partnerName": partner.name,
                "period": period,
                "zone": zone,
                "currency": world.currency(country),
                "stops": stops,
                "deliveredParcels": count,
                "failedAttempts": failed.get((zone, period), 0),
                "costPerStop": num(partner.cost_per_stop),
                "totalCost": num(money(stops * partner.cost_per_stop)),
            }
        )

    for period, rows in sorted(by_period.items()):
        sent = at_utc(next_month_day(period, world.sim.settlements.issue_delay_days), 6.0)
        batch_id = world.uuid4(rng)
        for seq, row in enumerate(rows, start=1):
            world.add(
                Record(
                    source=SOURCE,
                    key=row["settlementId"],
                    source_ts=sent,
                    arrival=sent + timedelta(seconds=seq),
                    country=row["zone"][:2],
                    business_key=row["settlementId"],
                    data=row,
                    batch=Batch(batch_id, seq, len(rows)),
                )
            )
