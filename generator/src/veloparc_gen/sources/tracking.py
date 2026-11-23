"""Step 5 · tracking events per TrackID, following the R3 state machine, in existing depots (R6), with
recordTime seconds to minutes after eventTime (R5) and no repeated (trackId, status, GLN) (R4).

Valid paths (R3):
    DATA_RECEIVED -> CANCELLED
    DATA_RECEIVED -> PICKUP -> HUB{1,3} -> IN_DELIVERY -> DELIVERED
    DATA_RECEIVED -> PICKUP -> HUB{1,3} -> IN_DELIVERY -> NOT_DELIVERED -> PARCELSHOP -> DELIVERED
    DATA_RECEIVED -> PICKUP -> HUB{1,3} -> PARCELSHOP -> DELIVERED                  (shop delivery)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

import numpy as np

from veloparc_gen import gs1
from veloparc_gen.sources.shipments import ShipmentRecord, shop_delivery
from veloparc_gen.world import Depot, Parcel, Record, World, at_local, local_iso, tz, utc_z

SOURCE = "tracking_event"
BIZ_STEP = {
    "PICKUP": "urn:epcglobal:cbv:bizstep:accepting",
    "HUB": "urn:epcglobal:cbv:bizstep:arriving",
    "IN_DELIVERY": "urn:epcglobal:cbv:bizstep:departing",
    "DELIVERED": "urn:epcglobal:cbv:bizstep:receiving",
    "PARCELSHOP": "urn:epcglobal:cbv:bizstep:arriving",
}
_REASONS = ["NOT_HOME", "NOT_HOME", "NOT_HOME", "REFUSED", "ADDRESS_ERROR", "DAMAGED"]
_FIRMWARE = ["4.2.1", "4.2.3", "4.3.0"]


@dataclass(slots=True, eq=False)
class EventRecord(Record):
    """One scan. Built lazily; anomalies change these fields (status, gln, record_time, ...)."""

    event_id: str = ""
    track_id: str = ""
    status: str = ""
    event_time: datetime | None = None
    record_time: datetime | None = None
    gln: str | None = None
    location_name: str | None = None
    reason: str | None = None
    scanner: str | None = None
    firmware: str | None = None
    epc: str = ""
    tz_name: str = ""

    def build(self, world: World) -> dict:
        payload: dict = {
            "eventId": self.event_id,
            "eventType": "ObjectEvent",
            "eventTime": local_iso(self.event_time, self.tz_name),
            "recordTime": utc_z(self.record_time),
            "trackId": self.track_id,
            "epcList": [self.epc],
            "status": self.status,
        }
        step = BIZ_STEP.get(self.status)
        if step:
            payload["bizStep"] = step
        if self.gln is not None:
            payload["bizLocation"] = {"gln": self.gln, "name": self.location_name}
        payload["reasonCode"] = self.reason
        if self.scanner:
            payload["device"] = {"scannerId": self.scanner, "firmware": self.firmware}
        return payload

    def local_date(self) -> date:
        return self.event_time.astimezone(tz(self.tz_name)).date()


def _next_operating_day(day: date) -> date:
    return day + timedelta(days=1) if day.weekday() == 6 else day


class _Route:
    """What happens to a shipment; all its parcels travel together, with small per-parcel time jitter."""

    def __init__(self, world: World, shipment: ShipmentRecord, rng: np.random.Generator):
        sim = world.sim
        parcel = shipment.parcels[0]
        country, zone = parcel.country, parcel.zone
        self.tz_name = world.timezone(country)
        customer = world.customers[parcel.contact_id]
        self.origin = world.depots[customer.home_depot_gln]
        self.received = shipment.source_ts
        self.cancelled = parcel.cancelled
        local_received = self.received.astimezone(tz(self.tz_name))
        if self.cancelled:
            self.cancel_at = self.received + timedelta(minutes=int(rng.integers(30, 360)))
            return
        if local_received.hour < 15:
            pickup_day = local_received.date()
            pickup_hour = float(rng.uniform(max(15.0, local_received.hour + 1), 19.0))
        else:
            pickup_day = local_received.date() + timedelta(days=1)
            pickup_hour = float(rng.uniform(10.0, 13.0))
        pickup_day = _next_operating_day(pickup_day)
        self.pickup = at_local(pickup_day, pickup_hour, self.tz_name)

        mix = sim.tracking.delivery_mix
        kind = ["d1", "d2", "not_delivered_first"][rng.choice(3, p=[mix["d1"], mix["d2"], mix["not_delivered_first"]])]
        self.kind = "shop" if shop_delivery(shipment, 0) and kind != "not_delivered_first" else kind
        delivery_day = _next_operating_day(pickup_day + timedelta(days=2 if kind == "d2" else 1))
        self.delivery_start = at_local(delivery_day, float(rng.uniform(7.0, 9.5)), self.tz_name)

        hubs = world.depots_in(country, types=("HUB",))
        n_hub_opts = [int(k) for k in sim.tracking.hubs_per_parcel]
        n_hubs = min(n_hub_opts[rng.choice(len(n_hub_opts), p=list(sim.tracking.hubs_per_parcel.values()))], len(hubs))
        self.hubs: list[Depot] = [hubs[i] for i in rng.choice(len(hubs), size=n_hubs, replace=False)]
        span = (self.delivery_start - self.pickup).total_seconds() - 3600
        offsets = sorted(rng.uniform(3600, max(span, 3600 * (n_hubs + 1)), size=n_hubs))
        self.hub_times = [self.pickup + timedelta(seconds=float(o)) for o in offsets]
        if self.hub_times and self.hub_times[-1] >= self.delivery_start:
            self.delivery_start = self.hub_times[-1] + timedelta(hours=1)

        depots = world.depots_in(country, zone, ("DEPOT",))
        shops = world.depots_in(country, zone, ("PARCELSHOP", "LOCKER"))
        self.dest = depots[int(rng.integers(len(depots)))]
        self.shop = shops[int(rng.integers(len(shops)))] if shops else self.dest
        self.reason = _REASONS[int(rng.integers(len(_REASONS)))]
        self.delivered_after = timedelta(seconds=int(rng.uniform(1800, 8 * 3600)))
        self.shop_wait = timedelta(seconds=int(rng.uniform(2 * 3600, 48 * 3600)))

    def steps(self) -> list[tuple[str, datetime, Depot | None, str | None]]:
        steps: list[tuple[str, datetime, Depot | None, str | None]] = [("DATA_RECEIVED", self.received, None, None)]
        if self.cancelled:
            return [*steps, ("CANCELLED", self.cancel_at, self.origin, None)]
        steps.append(("PICKUP", self.pickup, self.origin, None))
        steps += [("HUB", t, hub, None) for hub, t in zip(self.hubs, self.hub_times, strict=True)]
        start = self.delivery_start
        if self.kind == "shop":
            steps += [("PARCELSHOP", start, self.shop, None), ("DELIVERED", start + self.shop_wait, self.shop, None)]
        elif self.kind == "not_delivered_first":
            failed = start + self.delivered_after
            next_day = failed + timedelta(hours=18)
            steps += [
                ("IN_DELIVERY", start, self.dest, None),
                ("NOT_DELIVERED", failed, self.dest, self.reason),
                ("PARCELSHOP", next_day, self.shop, None),
                ("DELIVERED", next_day + self.shop_wait, self.shop, None),
            ]
        else:
            steps += [
                ("IN_DELIVERY", start, self.dest, None),
                ("DELIVERED", start + self.delivered_after, self.dest, None),
            ]
        return steps


def build(world: World, shipments: list[ShipmentRecord]) -> None:
    delay_cfg = world.sim.tracking.record_delay_seconds
    for shipment in shipments:
        rng = world.rng("tracking", shipment.message_id)
        route = _Route(world, shipment, rng)
        for parcel in shipment.parcels:
            _parcel_events(world, parcel, route, rng, delay_cfg)


def _parcel_events(world: World, parcel: Parcel, route: _Route, rng, delay_cfg) -> None:
    hex_id = world.unique_id("evt", lambda: f"{int(rng.integers(0, 2**32)):08x}")
    epc = gs1.sscc_to_epc_urn(parcel.sscc)
    for seq, (status, at, depot, reason) in enumerate(route.steps(), start=1):
        event_time = at + timedelta(seconds=int(rng.integers(0, 90))) if seq > 1 else at
        event_time = event_time.replace(microsecond=0)
        delay = min(rng.lognormal(np.log(delay_cfg.median), delay_cfg.sigma), delay_cfg.max)
        record_time = event_time + timedelta(seconds=max(1, round(float(delay))))
        event = EventRecord(
            source=SOURCE,
            key=parcel.track_id,
            source_ts=record_time,
            arrival=world.arrival_after(record_time, rng),
            country=parcel.country,
            business_key=f"{parcel.track_id}|{status}|{depot.gln if depot else ''}",
            event_id=f"evt-{hex_id}-{seq:04d}",
            track_id=parcel.track_id,
            status=status,
            event_time=event_time,
            record_time=record_time,
            gln=depot.gln if depot else None,
            location_name=depot.name if depot else None,
            reason=reason,
            scanner=depot.scanners[int(rng.integers(len(depot.scanners)))] if depot else None,
            firmware=_FIRMWARE[int(rng.integers(len(_FIRMWARE)))] if depot else None,
            epc=epc,
            tz_name=route.tz_name,
        )
        parcel.events.append(world.add(event))
        if status == "PICKUP":
            parcel.picked_up = True
        if status == "DELIVERED":
            parcel.delivered = True
            parcel.delivered_local_date = event.local_date()
