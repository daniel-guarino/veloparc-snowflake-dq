"""Step 6 · monthly invoices per customer: one line per collected TrackID priced with the right tariff band
(R7, R8), negative credit notes that reference an existing invoice (R9), FX rate of the issue date (R10)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal

from veloparc_gen.world import Batch, Parcel, Record, World, at_utc, money, next_month_day, num, period_of

SOURCE = "invoice"


@dataclass(slots=True, eq=False)
class Line:
    track_id: str
    tariff_id: str
    product: str
    weight: Decimal
    zone: str
    base: Decimal  # always positive here; the document sign is applied when the payload is built
    surcharges: list[tuple[str, Decimal]]

    @property
    def net(self) -> Decimal:
        return self.base + sum((a for _, a in self.surcharges), Decimal(0))


@dataclass(slots=True, eq=False)
class InvoiceRecord(Record):
    """Totals are always recomputed from the lines, so anomalies that add or drop lines keep R7 true."""

    number: str = ""
    doc_type: str = "INVOICE"
    issue_date: date | None = None
    period: str = ""
    contact_id: str = ""
    vat_number: str = ""
    currency: str = "EUR"
    fx: Decimal = Decimal(1)
    vat_rate: Decimal = Decimal(0)
    related: str | None = None
    lines: list[Line] = field(default_factory=list)
    force_positive: bool = False  # R9 anomaly: credit note sent with positive amounts

    @property
    def sign(self) -> Decimal:
        return Decimal(-1) if self.doc_type == "CREDIT_NOTE" and not self.force_positive else Decimal(1)

    def totals(self) -> tuple[Decimal, Decimal, Decimal]:
        net = sum((money(self.sign * line.net) for line in self.lines), Decimal(0))
        vat = money(net * self.vat_rate / 100)
        return net, vat, net + vat

    def build(self, world: World) -> dict:
        sign = self.sign
        lines = []
        for no, line in enumerate(self.lines, start=1):
            item: dict = {
                "lineNo": no,
                "trackId": line.track_id,
                "tariffId": line.tariff_id,
                "product": line.product,
                "weightKg": num(line.weight),
                "zone": line.zone,
                "basePrice": num(sign * line.base),
            }
            if line.surcharges:
                item["surcharges"] = [{"type": t, "amount": num(sign * a)} for t, a in line.surcharges]
            item["netAmount"] = num(money(sign * line.net))
            lines.append(item)
        net, vat, gross = self.totals()
        payload: dict = {
            "invoiceNumber": self.number,
            "docType": self.doc_type,
            "issueDate": self.issue_date.isoformat(),
            "billingPeriod": self.period,
        }
        if self.related is not None:
            payload["relatedInvoiceNumber"] = self.related
        payload |= {
            "customer": {"contactId": self.contact_id, "vatNumber": self.vat_number},
            "currency": self.currency,
            "fxRateToEur": float(self.fx),
            "lines": lines,
            "totals": {"net": num(net), "vat": num(vat), "gross": num(gross)},
        }
        return payload

    def net_eur(self) -> Decimal:
        return money(self.totals()[0] * self.fx)


def price_line(world: World, parcel: Parcel) -> Line:
    tariff = world.tariff_for(parcel.contact_id, parcel.product, parcel.zone, parcel.ship_date)
    base = tariff.band_price(parcel.weight)
    surcharges = [("FUEL", money(base * tariff.fuel_pct / 100))]
    if parcel.zone.endswith(f"Z{world.sim.network.zones_per_country}"):
        remote = Decimal(str(world.sim.tariffs.remote_area_surcharge_eur))
        surcharges.append(("REMOTE_AREA", money(remote / world.fx_rate(tariff.currency, world.sim.window.start))))
    return Line(parcel.track_id, tariff.tariff_id, parcel.product, parcel.weight, parcel.zone, base, surcharges)


def build(world: World) -> None:
    sim = world.sim
    cfg = sim.invoicing
    groups: dict[tuple[str, str], list[Parcel]] = {}
    for parcel in world.parcels.values():
        if parcel.picked_up:
            groups.setdefault((parcel.contact_id, period_of(parcel.ship_date)), []).append(parcel)

    docs: list[InvoiceRecord] = []
    for contact_id, period in sorted(groups):
        customer = world.customers[contact_id]
        parcels = sorted(groups[(contact_id, period)], key=lambda p: (p.ship_date, p.track_id))
        issue = next_month_day(period, cfg.issue_day)
        currency = world.currency(customer.country)
        size = cfg.max_lines_per_invoice
        for start in range(0, len(parcels), size):
            docs.append(
                _doc(
                    world,
                    customer,
                    period,
                    issue,
                    currency,
                    [price_line(world, p) for p in parcels[start : start + size]],
                )
            )

    credit_notes: list[InvoiceRecord] = []
    for doc in docs:
        rng = world.rng("credit_note", doc.contact_id, doc.period, doc.lines[0].track_id)
        if rng.random() >= cfg.credit_note_invoice_share:
            continue
        k = max(1, round(len(doc.lines) * cfg.credit_note_line_share))
        picked = sorted(rng.choice(len(doc.lines), size=k, replace=False))
        delay = int(rng.integers(cfg.credit_note_delay_days.min, cfg.credit_note_delay_days.max + 1))
        customer = world.customers[doc.contact_id]
        note = _doc(
            world,
            customer,
            doc.period,
            doc.issue_date + timedelta(days=delay),
            doc.currency,
            [doc.lines[i] for i in picked],
        )
        note.doc_type = "CREDIT_NOTE"
        note.related = doc  # resolved to the number below
        credit_notes.append(note)

    _number(world, docs + credit_notes)
    for note in credit_notes:
        note.related = note.related.number
    _records(world, docs, credit_notes)


def _doc(world, customer, period, issue, currency, lines) -> InvoiceRecord:
    return InvoiceRecord(
        source=SOURCE,
        key="",
        source_ts=at_utc(issue),
        arrival=at_utc(issue),
        country=customer.country,
        business_key="",
        issue_date=issue,
        period=period,
        contact_id=customer.contact_id,
        vat_number=customer.vat,
        currency=currency,
        fx=world.fx_rate(currency, issue),
        vat_rate=Decimal(str(world.sim.countries[customer.country].vat_rate_pct)),
        lines=lines,
    )


def _number(world: World, docs: list[InvoiceRecord]) -> None:
    """Sequential numbers per country and year, in issue order: PT-2026-0045871."""
    counters: dict[tuple[str, int], int] = {}
    order = sorted(docs, key=lambda d: (d.issue_date, d.doc_type, d.contact_id, d.lines[0].track_id))
    for doc in order:
        key = (doc.country, doc.issue_date.year)
        if key not in counters:
            counters[key] = int(world.rng("invoice_numbers", *key).integers(10000, 60000))
        counters[key] += 1
        doc.number = f"{doc.country}-{doc.issue_date.year}-{counters[key]:07d}"
        doc.key = doc.business_key = doc.number


def _records(world: World, invoices: list[InvoiceRecord], credit_notes: list[InvoiceRecord]) -> None:
    """Invoices come from a nightly billing run per country and issue date (one producer batch);
    credit notes are produced one by one during office hours."""
    rng = world.rng("invoice_records")
    runs: dict[tuple[str, date], list[InvoiceRecord]] = {}
    for doc in sorted(invoices, key=lambda d: d.number):
        runs.setdefault((doc.country, doc.issue_date), []).append(doc)
    for (_, issue), run in sorted(runs.items()):
        batch_id = world.uuid4(rng)
        for seq, doc in enumerate(run, start=1):
            doc.source_ts = at_utc(issue, 2.0) + timedelta(seconds=seq)
            doc.arrival = world.arrival_after(doc.source_ts, rng)
            doc.batch = Batch(batch_id, seq, len(run))
            world.add(doc)
    for note in sorted(credit_notes, key=lambda d: d.number):
        note.source_ts = at_utc(note.issue_date, float(rng.uniform(9, 17)))
        note.arrival = world.arrival_after(note.source_ts, rng)
        note.batch = Batch(world.uuid4(rng), 1, 1)
        world.add(note)
