"""Validate contracts/examples/*.json against their JSON Schemas and the cross-source rules.

Run from the repository root:
    pip install "jsonschema[format]"
    python contracts/validate_examples.py

Exits with status 1 if any example fails, so it can run in CI.
"""

from __future__ import annotations

import json
import sys
from decimal import Decimal
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parent
SCHEMAS = ROOT / "schemas"
EXAMPLES = ROOT / "examples"

# example file -> schema file
EXAMPLE_SCHEMA = {
    "veloship_shipment.json": "veloship_shipment.schema.json",
    "tracking_event.json": "tracking_event.schema.json",
    "invoice.json": "invoice.schema.json",
    "invoice_credit_note.json": "invoice.schema.json",
    "crm_opportunity.json": "crm_object.schema.json",
    "crm_lead.json": "crm_object.schema.json",
    "partner_settlement.json": "partner_settlement.schema.json",
    "tariff.json": "tariff.schema.json",
    "gl_extract_row.json": "gl_extract_row.schema.json",
    "master_data_customer.json": "master_data.schema.json",
    "master_data_depot.json": "master_data.schema.json",
    "master_data_partner.json": "master_data.schema.json",
}


def load(path: Path) -> dict:
    # parse_float=Decimal keeps money arithmetic exact
    return json.loads(path.read_text(encoding="utf-8"), parse_float=Decimal)


def gs1_check_digit_ok(code: str) -> bool:
    body, check = code[:-1], int(code[-1])
    total = sum(int(d) * (3 if i % 2 == 0 else 1) for i, d in enumerate(reversed(body)))
    return (10 - total % 10) % 10 == check


def money(x) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"))


def schema_errors() -> list[str]:
    errors: list[str] = []
    for example, schema in EXAMPLE_SCHEMA.items():
        instance = json.loads((EXAMPLES / example).read_text(encoding="utf-8"))
        validator = Draft202012Validator(
            json.loads((SCHEMAS / schema).read_text(encoding="utf-8")),
            format_checker=FormatChecker(),
        )
        for err in validator.iter_errors(instance):
            errors.append(f"{example}: {err.json_path}: {err.message}")
    missing = {p.name for p in EXAMPLES.glob("*.json")} - EXAMPLE_SCHEMA.keys()
    errors += [f"{name}: no schema mapped in validate_examples.py" for name in sorted(missing)]
    return errors


def rule_errors() -> list[str]:
    e = {name: load(EXAMPLES / name) for name in EXAMPLE_SCHEMA}
    errors: list[str] = []

    # R1: one TrackID per shipment unit
    shp = e["veloship_shipment.json"]
    units = shp["request"]["Shipment"]["ShipmentUnit"]
    parcels = shp["response"]["CreatedShipment"]["ParcelData"]
    if len(units) != len(parcels):
        errors.append("R1: len(ParcelData) != len(ShipmentUnit)")
    track_ids = {p["TrackID"] for p in parcels}

    # R2: events and invoice lines reference existing TrackIDs
    if e["tracking_event.json"]["trackId"] not in track_ids:
        errors.append("R2: tracking_event.trackId not in shipment")

    # R14: contiguous bands, first from 0, increasing price
    trf = e["tariff.json"]
    bands = trf["weightBands"]
    if bands[0]["fromKg"] != 0:
        errors.append("R14: first band must start at 0")
    for a, b in zip(bands, bands[1:]):
        if a["toKg"] != b["fromKg"] or not a["price"] < b["price"]:
            errors.append(f"R14: bands {a} -> {b} not contiguous/increasing")

    def band_price(weight) -> Decimal:
        for band in bands:
            if band["fromKg"] < weight <= band["toKg"] or (band["fromKg"] == 0 and weight == 0):
                return money(band["price"])
        raise ValueError(f"no band for {weight}")

    # R7, R8, R9 on invoice and credit note
    for name in ("invoice.json", "invoice_credit_note.json"):
        inv = e[name]
        sign = Decimal(-1) if inv["docType"] == "CREDIT_NOTE" else Decimal(1)
        net_total = Decimal(0)
        for line in inv["lines"]:
            if line["trackId"] not in track_ids:
                errors.append(f"R2: {name} line {line['lineNo']} trackId not in shipment")
            surcharges = sum((money(s["amount"]) for s in line.get("surcharges", [])), Decimal(0))
            if money(line["basePrice"]) + surcharges != money(line["netAmount"]):
                errors.append(f"R7: {name} line {line['lineNo']} netAmount != basePrice + surcharges")
            if money(line["basePrice"]) != sign * band_price(line["weightKg"]):
                errors.append(f"R8: {name} line {line['lineNo']} basePrice != tariff band price")
            fuel = [money(s["amount"]) for s in line.get("surcharges", []) if s["type"] == "FUEL"]
            expected_fuel = money(money(line["basePrice"]) * Decimal(str(trf["fuelSurchargePct"])) / 100)
            if fuel and fuel[0] != expected_fuel:
                errors.append(f"R8: {name} line {line['lineNo']} FUEL {fuel[0]} != {expected_fuel}")
            net_total += money(line["netAmount"])
        t = inv["totals"]
        if net_total != money(t["net"]) or money(t["net"]) + money(t["vat"]) != money(t["gross"]):
            errors.append(f"R7: {name} totals do not add up")
        if sign < 0 and (money(t["net"]) >= 0 or inv.get("relatedInvoiceNumber") != e["invoice.json"]["invoiceNumber"]):
            errors.append("R9: credit note must be negative and reference an existing invoice")

    # R12: settlement totals
    stl = e["partner_settlement.json"]
    if money(Decimal(stl["stops"]) * Decimal(str(stl["costPerStop"]))) != money(stl["totalCost"]):
        errors.append("R12: totalCost != stops x costPerStop")
    if stl["stops"] > stl["deliveredParcels"]:
        errors.append("R12: stops > deliveredParcels")

    # R15: GS1 check digits and VAT prefix
    glns = [e["master_data_depot.json"]["gln"], e["master_data_partner.json"]["baseDepotGln"],
            e["tracking_event.json"]["bizLocation"]["gln"]]
    for code in glns + [p["SSCC"] for p in parcels if "SSCC" in p]:
        if not gs1_check_digit_ok(code):
            errors.append(f"R15: invalid GS1 check digit in {code}")
    cust = e["master_data_customer.json"]
    if not cust["vatNumber"].startswith(cust["countryCode"]):
        errors.append("R15: vatNumber prefix != countryCode")

    # Referential coherence between examples
    if e["tracking_event.json"]["bizLocation"]["gln"] != e["master_data_depot.json"]["gln"]:
        errors.append("R6: event GLN is not a known depot")
    if e["crm_lead.json"]["ConvertedOpportunityId"] != e["crm_opportunity.json"]["Id"]:
        errors.append("lead.ConvertedOpportunityId does not match the opportunity")
    if stl["zone"] not in e["master_data_partner.json"]["zones"]:
        errors.append("settlement zone not served by the partner")
    return errors


def main() -> int:
    errors = schema_errors()
    if not errors:
        errors = rule_errors()
    for err in errors:
        print(f"FAIL {err}")
    if not errors:
        print(f"OK {len(EXAMPLE_SCHEMA)} examples valid against schemas and rules R1, R2, R6, R7, R8, R9, R12, R14, R15")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
