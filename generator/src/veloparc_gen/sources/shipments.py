"""Step 4 · VeloShip shipments: existing customer, 1-5 parcels, one TrackID per parcel (R1)."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

import numpy as np

from veloparc_gen import gs1
from veloparc_gen.world import ALNUM_UPPER, Batch, Parcel, Record, World, at_local, num, utc_z

SOURCE = "veloship_shipment"
_MIDDLEWARE = ["SAP", "Primavera", "Shopify", "WooCommerce", "Magento", "PrestaShop", "Custom API"]
_POOL = 3000


@dataclass(slots=True, eq=False)
class ShipmentRecord(Record):
    """Built lazily from the parcels; ``extra_units`` is set by the R1 anomaly."""

    message_id: str = ""
    ship_date: object = None
    product: str = ""
    middleware: str = ""
    reference: str | None = None
    consignee: dict = field(default_factory=dict)
    parcels: list[Parcel] = field(default_factory=list)
    services: list[list[str]] = field(default_factory=list)
    extra_units: list[float] = field(default_factory=list)

    def build(self, world: World) -> dict:
        units = []
        for parcel, services in zip(self.parcels, self.services, strict=True):
            unit: dict = {"ShipmentUnitReference": [f"BOX-{len(units) + 1}"], "Weight": num(parcel.weight)}
            if services:
                unit["Service"] = [{"Service": {"ServiceName": s}} for s in services]
            units.append(unit)
        for weight in self.extra_units:
            units.append({"ShipmentUnitReference": [f"BOX-{len(units) + 1}"], "Weight": weight})
        shipment: dict = {}
        if self.reference:
            shipment["ShipmentReference"] = [self.reference]
        shipment |= {
            "ShippingDate": self.ship_date.isoformat(),
            "Product": self.product,
            "Middleware": self.middleware,
            "Shipper": {"ContactID": self.parcels[0].contact_id},
            "Consignee": self.consignee,
            "ShipmentUnit": units,
        }
        return {
            "messageId": self.message_id,
            "receivedAt": utc_z(self.source_ts),
            "request": {"Shipment": shipment},
            "response": {
                "CreatedShipment": {
                    "ParcelData": [
                        {"TrackID": p.track_id, "ParcelNumber": p.parcel_number, "SSCC": p.sscc} for p in self.parcels
                    ]
                }
            },
        }


class _Pools:
    """Consignee names, streets and cities per country, drawn once from Faker (fast and reproducible)."""

    def __init__(self, world: World, country: str):
        fake = world.faker(country, "consignees")
        self.names = [fake.name()[:40] for _ in range(_POOL)]
        self.streets = [fake.street_name()[:40] for _ in range(_POOL)]
        self.cities = [fake.city()[:40] for _ in range(_POOL)]


def day_volume(world: World, day) -> float:
    cfg = world.sim.shipments
    factor = cfg.weekend_factor if day.weekday() >= 5 else 1.0
    for peak in cfg.peaks:
        if peak.date == day:
            factor *= peak.factor
    share = sum(c.share for c in world.sim.countries.values())
    return cfg.per_working_day * world.sim.scale * share * factor


def build(world: World) -> list[ShipmentRecord]:
    """Creates shipments and parcels for every day of the window; tracking events are added by tracking.py."""
    sim = world.sim
    cfg = sim.shipments
    customers = list(world.customers.values())
    weights = np.array([c.volume_weight for c in customers])
    weights = weights / weights.sum()
    pools = {c: _Pools(world, c) for c in world.countries}
    n_options = [int(k) for k in cfg.parcels_per_shipment]
    n_probs = list(cfg.parcels_per_shipment.values())
    serials: dict[str, int] = {}
    shipments: list[ShipmentRecord] = []
    parcel_no = 0

    for day in world.window_days:
        rng = world.rng("shipments", day.isoformat())
        total = int(rng.poisson(day_volume(world, day)))
        counts = rng.multinomial(total, weights)
        for ci in np.flatnonzero(counts):
            customer = customers[ci]
            zone_list = world.zones(customer.country)
            tz_name = world.timezone(customer.country)
            pool = pools[customer.country]
            hours = cfg.business_hours_local
            for _ in range(int(counts[ci])):
                product = customer.products[0]
                if len(customer.products) > 1 and rng.random() < 0.3:
                    product = customer.products[1]
                zone = zone_list[int(rng.integers(len(zone_list)))]
                received = at_local(day, float(rng.uniform(hours["start"], hours["end"])), tz_name)
                n_parcels = n_options[rng.choice(len(n_options), p=n_probs)]
                weight_cfg = cfg.weight_kg[product]
                raw = rng.lognormal(np.log(weight_cfg.median), weight_cfg.sigma, size=n_parcels)
                parcel_weights = [Decimal(str(round(min(max(w, 0.1), cfg.max_weight_kg), 1))) for w in raw]
                message_id = world.uuid4(rng)
                consignee: dict = {}
                if rng.random() < 0.6:
                    consignee["ConsigneeID"] = f"CUST-{int(rng.integers(10000, 99999))}"
                consignee["Address"] = {
                    "Name1": pool.names[int(rng.integers(_POOL))],
                    "CountryCode": customer.country,
                    "ZIPCode": world.postal_code(customer.country, zone, rng),
                    "City": pool.cities[int(rng.integers(_POOL))],
                    "Street": pool.streets[int(rng.integers(_POOL))],
                    "StreetNumber": str(int(rng.integers(1, 300))),
                }
                cancelled = rng.random() < cfg.cancelled_share
                shop = rng.random() < cfg.shop_delivery_share
                parcels, services = [], []
                prefix = world.sim.countries[customer.country].gs1_company_prefix
                for weight in parcel_weights:
                    parcel_no += 1
                    serial = serials.get(customer.country, 0) + 1
                    serials[customer.country] = serial
                    track_id = world.unique_id("track", lambda rng=rng: world.random_string(rng, ALNUM_UPPER, 8))
                    parcel = Parcel(
                        track_id=track_id,
                        parcel_number=f"{5800000000 + parcel_no:011d}",
                        sscc=gs1.make_sscc(int(rng.integers(10)), prefix, serial),
                        weight=weight,
                        message_id=message_id,
                        contact_id=customer.contact_id,
                        product=product,
                        country=customer.country,
                        zone=world.zone_of_postal(customer.country, consignee["Address"]["ZIPCode"]),
                        ship_date=day,
                        cancelled=cancelled,
                    )
                    world.parcels[track_id] = parcel
                    parcels.append(parcel)
                    svc = ["service_shop_delivery"] if shop else []
                    if rng.random() < 0.1:
                        svc.append("service_carbon_neutral")
                    services.append(svc)
                reference = None
                if customer.segment != "B2B_SHIPPER" or rng.random() < 0.5:
                    reference = f"ORD-{day.year}-{int(rng.integers(100000, 999999))}"
                record = ShipmentRecord(
                    source=SOURCE,
                    key=message_id,
                    source_ts=received,
                    arrival=world.arrival_after(received, rng),
                    country=customer.country,
                    business_key=message_id,
                    batch=Batch(world.uuid4(rng), 1, 1),
                    message_id=message_id,
                    ship_date=day,
                    product=product,
                    middleware=_MIDDLEWARE[int(rng.integers(len(_MIDDLEWARE)))],
                    reference=reference,
                    consignee=consignee,
                    parcels=parcels,
                    services=services,
                )
                shipments.append(world.add(record))
    return shipments


def shop_delivery(record: ShipmentRecord, index: int) -> bool:
    return "service_shop_delivery" in record.services[index]
