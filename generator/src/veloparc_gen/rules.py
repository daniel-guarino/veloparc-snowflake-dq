"""Cross-source rules R1-R16 (contracts/README.md), checked in Python over the payloads.

Used by the tests (a clean world must have zero violations) and by every run, whose run_manifest reports the
violations found next to the anomalies injected: it is the first, local version of the Phase 9 evaluation.
Only payloads that pass their schema are given to the checker, as silver would only see those."""

from __future__ import annotations

import itertools
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal

from veloparc_gen import gs1
from veloparc_gen.world import money

_GRAMMAR = re.compile(r"D(C|PH{1,3}(IV|INSV|SV))")
_TOKEN = {
    "DATA_RECEIVED": "D",
    "PICKUP": "P",
    "HUB": "H",
    "IN_DELIVERY": "I",
    "DELIVERED": "V",
    "NOT_DELIVERED": "N",
    "PARCELSHOP": "S",
    "CANCELLED": "C",
}
_ISO2 = re.compile(r"[A-Z]{2}")


@dataclass(frozen=True, slots=True)
class Violation:
    rule: str
    source: str
    key: str
    message: str


def _d(value) -> Decimal:
    return Decimal(str(value))


def _ts(value: str) -> datetime:
    if value.endswith("+0000"):
        value = value[:-5] + "+00:00"
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def check(
    payloads: dict[str, list[dict]],
    *,
    fx: dict[tuple[str, date], Decimal],
    postal_zones: dict[tuple[str, str], str],
    max_delay_minutes: float = 15.0,
    gl_tolerance: Decimal = Decimal("0.005"),
) -> list[Violation]:
    v: list[Violation] = []
    shipments = payloads.get("veloship_shipment", [])
    events = payloads.get("tracking_event", [])
    invoices = payloads.get("invoice", [])
    tariffs = payloads.get("tariff", [])
    settlements = payloads.get("partner_settlement", [])
    gl_rows = payloads.get("gl_extract", [])
    master = payloads.get("master_data", [])
    crm = payloads.get("crm_object", [])

    def zone_of(country: str, postal: str) -> str | None:
        return postal_zones.get((country, postal[:1]))

    # ---- indexes
    parcel: dict[str, dict] = {}  # trackId -> shipment facts
    for s in shipments:
        shp = s["request"]["Shipment"]
        units = shp["ShipmentUnit"]
        data = s["response"]["CreatedShipment"]["ParcelData"]
        address = shp["Consignee"]["Address"]
        if len(units) != len(data):  # R1
            v.append(Violation("R1", "veloship_shipment", s["messageId"], f"{len(units)} units, {len(data)} TrackIDs"))
        for unit, p in zip(units, data, strict=False):
            parcel[p["TrackID"]] = {
                "contact": shp["Shipper"]["ContactID"],
                "product": shp["Product"],
                "ship_date": date.fromisoformat(shp["ShippingDate"]),
                "weight": _d(unit["Weight"]),
                "country": address["CountryCode"],
                "zone": zone_of(address["CountryCode"], address["ZIPCode"]),
            }
            if "SSCC" in p and not gs1.is_valid(p["SSCC"]):  # R15
                v.append(Violation("R15", "veloship_shipment", p["TrackID"], f"invalid SSCC {p['SSCC']}"))

    depots: dict[str, dict] = {}
    customers: dict[str, dict] = {}
    partners: dict[str, dict] = {}
    for m in master:
        if m["entity"] == "depot":
            depots[m["gln"]] = m
        elif m["entity"] == "customer":
            customers[m["contactId"]] = m  # latest version wins
        else:
            partners[m["partnerId"]] = m

    # ---- R15 master data, R16
    for gln in depots:
        if not gs1.is_valid(gln):
            v.append(Violation("R15", "master_data", gln, "invalid GLN check digit"))
    for c in customers.values():
        if not gs1.vat_prefix_ok(c["vatNumber"], c["countryCode"]):
            v.append(Violation("R15", "master_data", c["contactId"], "vatNumber prefix != countryCode"))
    zone_partners: dict[str, list[str]] = defaultdict(list)
    for p in partners.values():
        if not gs1.is_valid(p["baseDepotGln"]):
            v.append(Violation("R15", "master_data", p["partnerId"], "invalid baseDepotGln"))
        for z in p["zones"]:
            zone_partners[z].append(p["partnerId"])
    countries = {c for c, _ in postal_zones}
    all_zones = {z for (c, _), z in postal_zones.items()} | set(zone_partners)
    for z in sorted(all_zones):
        if z[:2] in countries and len(zone_partners.get(z, [])) != 1:
            v.append(Violation("R16", "master_data", z, f"partners: {zone_partners.get(z, [])}"))

    # ---- tracking: R2, R3, R4, R5, R6, R15
    by_track: dict[str, list[dict]] = defaultdict(list)
    seen: Counter = Counter()
    recorded_days: dict[str, set[date]] = defaultdict(set)
    event_days: dict[str, set[date]] = defaultdict(set)
    for e in events:
        tid = e["trackId"]
        if tid not in parcel:
            v.append(Violation("R2", "tracking_event", e["eventId"], f"trackId {tid} not in any shipment"))
            continue
        by_track[tid].append(e)
        gln = e.get("bizLocation", {}).get("gln")
        seen[(tid, e["status"], gln)] += 1
        event_time, record_time = _ts(e["eventTime"]), _ts(e["recordTime"])
        if record_time < event_time:
            v.append(Violation("R5", "tracking_event", e["eventId"], "recordTime before eventTime"))
        elif record_time - event_time > timedelta(minutes=max_delay_minutes):
            v.append(Violation("R5", "tracking_event", e["eventId"], f"recorded {record_time - event_time} late"))
        country = parcel[tid]["country"]
        event_days[country].add(event_time.date())
        recorded_days[country].add(record_time.date())
        if gln is not None:
            if not gs1.is_valid(gln):
                v.append(Violation("R15", "tracking_event", e["eventId"], f"invalid GLN {gln}"))
            if gln not in depots:
                v.append(Violation("R6", "tracking_event", e["eventId"], f"GLN {gln} is not a depot"))
    for key, n in seen.items():
        if n > 1:
            v.append(Violation("R4", "tracking_event", "|".join(map(str, key)), f"repeated {n} times"))
    for country, active in event_days.items():
        for day in sorted(active - recorded_days[country]):
            v.append(Violation("R5", "tracking_event", f"{country}|{day}", "no scans recorded on a day with activity"))

    picked_up, delivered = set(), set()
    delivered_by_zone_month: Counter = Counter()
    for tid, evs in by_track.items():
        evs.sort(key=lambda e: (_ts(e["eventTime"]), e["eventId"]))
        statuses = [e["status"] for e in evs]
        tokens = "".join(_TOKEN.get(s, "?") for s in statuses)
        if not _GRAMMAR.fullmatch(tokens):
            v.append(Violation("R3", "tracking_event", tid, " > ".join(statuses)))
        if "PICKUP" in statuses:
            picked_up.add(tid)
        if "DELIVERED" in statuses:
            delivered.add(tid)
        for e in evs:
            if e["status"] == "DELIVERED":
                local_day = datetime.fromisoformat(e["eventTime"]).date()
                delivered_by_zone_month[(parcel[tid]["zone"], f"{local_day:%Y-%m}")] += 1
        last_gln = evs[-1].get("bizLocation", {}).get("gln")
        if statuses[-1] == "DELIVERED" and last_gln in depots:
            d = depots[last_gln]
            if zone_of(d["countryCode"], d.get("postalCode", "")) != parcel[tid]["zone"]:
                v.append(Violation("R6", "tracking_event", tid, f"last scan at {last_gln} outside consignee zone"))

    # ---- tariffs: R14
    tariff_by_id = {t["tariffId"]: t for t in tariffs}
    by_scope: dict[tuple, list[dict]] = defaultdict(list)
    for t in tariffs:
        by_scope[(t["contactId"], t["product"], t["zone"])].append(t)
        bands = t["weightBands"]
        if _d(bands[0]["fromKg"]) != 0:
            v.append(Violation("R14", "tariff", t["tariffId"], "first band does not start at 0"))
        for a, b in itertools.pairwise(bands):
            if _d(a["toKg"]) != _d(b["fromKg"]) or not _d(a["price"]) < _d(b["price"]):
                v.append(Violation("R14", "tariff", t["tariffId"], "bands not contiguous or price not increasing"))
        if t.get("validTo") and t["validTo"] <= t["validFrom"]:
            v.append(Violation("R14", "tariff", t["tariffId"], "validTo <= validFrom"))
    for versions in by_scope.values():
        versions.sort(key=lambda t: t["validFrom"])
        for a, b in itertools.pairwise(versions):
            if a.get("validTo") is None or a["validTo"] >= b["validFrom"]:
                v.append(Violation("R14", "tariff", b["tariffId"], f"validity overlaps {a['tariffId']}"))

    # ---- invoices: R2, R7, R8, R9, R10
    invoice_numbers = {i["invoiceNumber"] for i in invoices if i["docType"] == "INVOICE"}
    invoiced = set()
    revenue: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
    for inv in invoices:
        number, credit = inv["invoiceNumber"], inv["docType"] == "CREDIT_NOTE"
        contact = inv["customer"]["contactId"]
        if contact not in customers:
            v.append(Violation("R2", "invoice", number, f"customer {contact} not in master data"))
        rate = _d(inv["fxRateToEur"])
        issue = date.fromisoformat(inv["issueDate"])
        expected_rate = Decimal(1) if inv["currency"] == "EUR" else fx.get((inv["currency"], issue))
        if expected_rate is None or rate != expected_rate:
            v.append(Violation("R10", "invoice", number, f"fxRateToEur {rate} != {expected_rate}"))
        net_total = Decimal(0)
        for line in inv["lines"]:
            tid = line["trackId"]
            ref = f"{number}#{line['lineNo']}"
            base = money(line["basePrice"])
            surcharges = sum((money(s["amount"]) for s in line.get("surcharges", [])), Decimal(0))
            if base + surcharges != money(line["netAmount"]):
                v.append(Violation("R7", "invoice", ref, "netAmount != basePrice + surcharges"))
            net_total += money(line["netAmount"])
            if credit and money(line["netAmount"]) >= 0:
                v.append(Violation("R9", "invoice", ref, "credit note line not negative"))
            if tid not in parcel:
                v.append(Violation("R2", "invoice", ref, f"trackId {tid} not in any shipment"))
                continue
            if not credit:
                invoiced.add(tid)
                if tid not in picked_up:
                    v.append(Violation("R2", "invoice", ref, f"trackId {tid} billed without PICKUP"))
            tariff = tariff_by_id.get(line["tariffId"])
            facts = parcel[tid]
            if tariff is None or not (
                tariff["validFrom"] <= facts["ship_date"].isoformat() <= (tariff.get("validTo") or "9999-12-31")
            ):
                v.append(Violation("R8", "invoice", ref, f"tariff {line['tariffId']} not valid on ship date"))
                continue
            price = next(
                (_d(b["price"]) for b in tariff["weightBands"] if _d(b["fromKg"]) < facts["weight"] <= _d(b["toKg"])),
                None,
            )
            sign = Decimal(-1) if credit else Decimal(1)
            if price is None or base != money(sign * price):
                v.append(Violation("R8", "invoice", ref, f"basePrice {base} != band price {price}"))
            fuel = [money(s["amount"]) for s in line.get("surcharges", []) if s["type"] == "FUEL"]
            if fuel and fuel[0] != money(base * _d(tariff.get("fuelSurchargePct", 0)) / 100):
                v.append(Violation("R8", "invoice", ref, "FUEL surcharge != basePrice x fuelSurchargePct"))
        totals = inv["totals"]
        if net_total != money(totals["net"]) or money(totals["net"]) + money(totals["vat"]) != money(totals["gross"]):
            v.append(Violation("R7", "invoice", number, "totals do not add up"))
        if credit and (money(totals["net"]) >= 0 or inv.get("relatedInvoiceNumber") not in invoice_numbers):
            v.append(Violation("R9", "invoice", number, "credit note not negative or related invoice unknown"))
        revenue[(number[:2], inv["billingPeriod"])] += money(money(totals["net"]) * rate)
    for tid in sorted(delivered - invoiced):
        v.append(Violation("R2", "invoice", tid, "delivered parcel never invoiced"))

    # ---- settlements: R11, R12
    for s in settlements:
        sid = s["settlementId"]
        if money(_d(s["stops"]) * _d(s["costPerStop"])) != money(s["totalCost"]):
            v.append(Violation("R12", "partner_settlement", sid, "totalCost != stops x costPerStop"))
        if s["stops"] > s["deliveredParcels"]:
            v.append(Violation("R12", "partner_settlement", sid, "stops > deliveredParcels"))
        scans = delivered_by_zone_month.get((s["zone"], s["period"]), 0)
        if scans != s["deliveredParcels"]:
            v.append(
                Violation("R11", "partner_settlement", sid, f"deliveredParcels {s['deliveredParcels']} != {scans}")
            )

    # ---- general ledger: R13
    ledger: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
    for row in gl_rows:
        ledger[(row["country_code"], row["period"])] += money(row["amount_eur"])
    for key in sorted(set(ledger) | set(revenue)):
        gl_amount, inv_amount = ledger.get(key, Decimal(0)), revenue.get(key, Decimal(0))
        if abs(gl_amount - inv_amount) > abs(gl_amount) * gl_tolerance:
            v.append(Violation("R13", "gl_extract", "|".join(key), f"ledger {gl_amount} vs invoices {inv_amount}"))

    # ---- CRM: country of leads must be ISO 3166-1 alpha-2 (normalisation rule for silver)
    for obj in crm:
        if obj["attributes"]["type"] == "Lead" and "Country" in obj and not _ISO2.fullmatch(obj["Country"]):
            v.append(Violation("LEAD_COUNTRY_ISO2", "crm_object", obj["Id"], f"Country {obj['Country']!r}"))
    return v


def summarize(violations: list[Violation]) -> dict[str, int]:
    return dict(sorted(Counter(x.rule for x in violations).items()))
