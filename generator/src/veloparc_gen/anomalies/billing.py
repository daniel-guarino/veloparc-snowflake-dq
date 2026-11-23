"""Anomalies on invoices and credit notes. Totals are recomputed from the lines, so R7 keeps holding and only
the targeted rule breaks. The GL was computed before injection, so revenue changes also move R13."""

from __future__ import annotations

from decimal import Decimal

from veloparc_gen.anomalies.base import Injection, Injector, register
from veloparc_gen.gs1 import vat_number
from veloparc_gen.sources.invoices import InvoiceRecord, price_line
from veloparc_gen.world import World, num


def _docs(world: World, doc_type: str) -> list[InvoiceRecord]:
    return [r for r in world.records.get("invoice", []) if r.doc_type == doc_type and not r.dropped]


@register
class CreditNoteWithoutRelated(Injector):
    name = "credit_note_without_related"

    def eligible(self, world):
        return _docs(world, "CREDIT_NOTE")

    def locks(self, item):
        return [("doc", item.number)]

    def inject(self, world, note, rng):
        original, note.related = note.related, None
        return [
            Injection(
                business_key=note.number,
                country=note.country,
                event_date=note.issue_date,
                details={"field": "relatedInvoiceNumber", "original": original, "injected": None},
                records=[note],
                related_rules=["R9", "R13"],  # quarantined: the ledger keeps the adjustment, silver does not
            )
        ]


@register
class InvoicedWithoutPickup(Injector):
    """A cancelled parcel (never picked up) is billed on the customer's invoice of that month."""

    name = "invoiced_without_pickup"

    def eligible(self, world):
        docs = {}
        for doc in _docs(world, "INVOICE"):
            if len(doc.lines) < world.sim.invoicing.max_lines_per_invoice:
                docs.setdefault((doc.contact_id, doc.period), doc)
        items = []
        for parcel in world.parcels.values():
            doc = docs.get((parcel.contact_id, f"{parcel.ship_date:%Y-%m}"))
            if parcel.cancelled and doc is not None:
                items.append((parcel, doc))
        return items

    def locks(self, item):
        parcel, _ = item
        return [("track", parcel.track_id)]

    def inject(self, world, item, rng):
        parcel, doc = item
        line = price_line(world, parcel)
        doc.lines.append(line)
        return [
            Injection(
                business_key=f"{doc.number}|{parcel.track_id}",
                country=doc.country,
                event_date=doc.issue_date,
                details={
                    "invoiceNumber": doc.number,
                    "lineNo": len(doc.lines),
                    "trackId": parcel.track_id,
                    "netAmount": num(line.net),
                    "last_scan": "CANCELLED",
                },
                records=[doc],
                related_rules=["R13"],
            )
        ]


@register
class DeliveredWithoutInvoice(Injector):
    """A delivered parcel is left out of its invoice (only invoices with 2+ lines, parcels never credited)."""

    name = "delivered_without_invoice"

    def eligible(self, world):
        credited = {line.track_id for note in _docs(world, "CREDIT_NOTE") for line in note.lines}
        items = []
        for doc in _docs(world, "INVOICE"):
            if len(doc.lines) >= 2:
                items += [(doc, line) for line in doc.lines if line.track_id not in credited]
        return [(doc, line) for doc, line in items if world.parcels[line.track_id].delivered]

    def locks(self, item):
        _, line = item
        return [("track", line.track_id)]

    def inject(self, world, item, rng):
        doc, line = item
        doc.lines.remove(line)
        return [
            Injection(
                business_key=line.track_id,
                country=doc.country,
                event_date=doc.issue_date,
                details={"invoiceNumber": doc.number, "trackId": line.track_id, "removed_netAmount": num(line.net)},
                records=[doc],
                related_rules=["R13"],
            )
        ]


@register
class PositiveCreditNotes(Injector):
    """Every credit note of one country and month is sent with positive amounts. Only country-months where
    that moves invoice revenue 1.5-4% away from the ledger are eligible (target: 2-3%)."""

    name = "positive_credit_notes"

    def eligible(self, world):
        groups: dict[tuple[str, str], list[InvoiceRecord]] = {}
        for note in _docs(world, "CREDIT_NOTE"):
            groups.setdefault((note.country, note.period), []).append(note)
        revenue: dict[tuple[str, str], Decimal] = {}
        for doc in world.records.get("invoice", []):
            revenue[(doc.country, doc.period)] = revenue.get((doc.country, doc.period), Decimal(0)) + doc.net_eur()
        scored = []
        for key, notes in sorted(groups.items()):
            credited = sum((-n.net_eur() for n in notes), Decimal(0))
            gap = 2 * credited / revenue[key] * 100 if revenue.get(key) else Decimal(0)
            scored.append((abs(gap - Decimal("2.5")), key, notes, gap))
        good = [s for s in scored if Decimal("1.5") <= s[3] <= Decimal("4")]
        chosen = good or sorted(scored, key=lambda s: s[0])[:1]
        return [(key, notes, gap) for _, key, notes, gap in chosen]

    def locks(self, item):
        key, notes, _ = item
        return [("cn-group", key)] + [("doc", n.number) for n in notes]

    def inject(self, world, item, rng):
        (country, period), notes, gap = item
        out = []
        for note in notes:
            note.force_positive = True
            out.append(
                Injection(
                    business_key=note.number,
                    country=country,
                    event_date=note.issue_date,
                    details={
                        "country_period": f"{country}|{period}",
                        "credit_notes_in_group": len(notes),
                        "net": num(note.totals()[0]),
                        "expected_ledger_gap_pct": round(float(gap), 2),
                    },
                    records=[note],
                    related_rules=["R8", "R13"],  # positive basePrice on a credit note also breaks R8
                )
            )
        return out


@register
class UnknownCustomerInvoice(Injector):
    name = "unknown_customer_invoice"

    def eligible(self, world):
        return _docs(world, "INVOICE")

    def locks(self, item):
        return [("doc", item.number)]

    def inject(self, world, doc, rng):
        prefix = world.sim.countries[doc.country].contact_id_prefix

        def make() -> str:
            digits = "".join(str(d) for d in rng.integers(0, 10, size=9 - len(prefix)))
            return prefix + digits + "XYZ"[int(rng.integers(3))]

        original = doc.contact_id
        doc.contact_id = world.unique_id("contact", make)  # never a real customer: other letters, unique
        doc.vat_number = vat_number(doc.country, rng)
        return [
            Injection(
                business_key=doc.number,
                country=doc.country,
                event_date=doc.issue_date,
                details={"field": "customer.contactId", "original": original, "injected": doc.contact_id},
                records=[doc],
            )
        ]
