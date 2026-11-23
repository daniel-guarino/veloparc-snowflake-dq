"""Step 8 · general ledger extract per company and month, equal to the invoices in EUR (R13).

Computed from the clean invoices, before anomalies are injected: anomalies on invoices then show up as a
difference between the ledger and the invoices, which is what R13 has to catch."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from veloparc_gen.world import Batch, Record, World, at_utc, next_month_day, num

SOURCE = "gl_extract"
ACCOUNTS = [
    ("700100", "Parcel revenue domestic"),
    ("700200", "Parcel revenue international"),  # always 0: the case only has domestic shipments
    ("700900", "Revenue adjustments"),  # credit notes
]


def build(world: World) -> None:
    totals: dict[tuple[str, str, str], Decimal] = {}
    for doc in world.records.get("invoice", []):
        account = "700900" if doc.doc_type == "CREDIT_NOTE" else "700100"
        key = (doc.country, doc.period, account)
        totals[key] = totals.get(key, Decimal(0)) + doc.net_eur()

    rng = world.rng("gl")
    for country, period in sorted({(c, p) for c, p, _ in totals}):
        sent = at_utc(next_month_day(period, world.sim.gl.issue_delay_days), 5.0)
        batch_id = world.uuid4(rng)
        for seq, (account, name) in enumerate(ACCOUNTS, start=1):
            row = {
                "company_code": f"VLP_{country}",
                "country_code": country,
                "period": period,
                "gl_account": account,
                "account_name": name,
                "amount_eur": num(totals.get((country, period, account), Decimal("0.00"))),
            }
            key = f"VLP_{country}|{period}|{account}"
            world.add(
                Record(
                    source=SOURCE,
                    key=key,
                    source_ts=sent,
                    arrival=sent + timedelta(seconds=seq),
                    country=country,
                    business_key=key,
                    data=row,
                    batch=Batch(batch_id, seq, len(ACCOUNTS)),
                )
            )
