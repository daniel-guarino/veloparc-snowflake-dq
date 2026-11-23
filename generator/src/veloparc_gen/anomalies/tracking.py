"""Anomalies on tracking events (eligible population: parcels, one anomaly per parcel)."""

from __future__ import annotations

import dataclasses
from datetime import timedelta

from veloparc_gen.anomalies.base import Injection, Injector, register
from veloparc_gen.sources.tracking import EventRecord
from veloparc_gen.world import Parcel, World, at_local, local_iso, tz, utc_z


def _clone(world: World, event: EventRecord, **changes) -> EventRecord:
    clone = dataclasses.replace(event, anomaly_ids=[], schema_invalid=False, dropped=False, **changes)
    clone.business_key = f"{clone.track_id}|{clone.status}|{clone.gln or ''}"
    world.add(clone)
    return clone


def _with(parcel: Parcel, status: str) -> list[EventRecord]:
    return [e for e in parcel.events if e.status == status and not e.dropped]


def _ref(event: EventRecord) -> dict:
    return {"eventId": event.event_id, "trackId": event.track_id, "status": event.status}


class _ParcelInjector(Injector):
    def locks(self, item: Parcel):
        return [("track", item.track_id)]


@register
class Gln12Digits(_ParcelInjector):
    """A HUB scan with a 12-digit GLN. Only parcels with 2+ HUB scans, so the quarantined scan does not
    break the R3 sequence in silver."""

    name = "gln_12_digits"

    def eligible(self, world):
        return [p for p in world.parcels.values() if len(_with(p, "HUB")) >= 2]

    def inject(self, world, parcel, rng):
        hubs = _with(parcel, "HUB")
        event = hubs[int(rng.integers(len(hubs)))]
        original = event.gln
        event.gln = original[:12]
        return [
            Injection(
                business_key=event.event_id,
                country=parcel.country,
                event_date=event.local_date(),
                details={"field": "bizLocation.gln", "original": original, "injected": event.gln, **_ref(event)},
                records=[event],
            )
        ]


@register
class StatusDeliverd(_ParcelInjector):
    name = "status_deliverd"

    def eligible(self, world):
        return [p for p in world.parcels.values() if _with(p, "DELIVERED")]

    def inject(self, world, parcel, rng):
        event = _with(parcel, "DELIVERED")[0]
        event.status = "DELIVERD"
        return [
            Injection(
                business_key=event.event_id,
                country=parcel.country,
                event_date=event.local_date(),
                details={"field": "status", "original": "DELIVERED", "injected": "DELIVERD", **_ref(event)},
                records=[event],
                # the scan is quarantined: silver sees the parcel as not delivered
                related_rules=["R3", "R11"],
            )
        ]


@register
class RepeatedEvent(_ParcelInjector):
    """Same trackId, status and GLN scanned twice (new eventId). Only HUB scans of parcels with at most
    2 HUBs, so the extra scan keeps a valid R3 sequence and only R4 fires."""

    name = "repeated_event"

    def eligible(self, world):
        return [p for p in world.parcels.values() if 1 <= len(_with(p, "HUB")) <= 2]

    def inject(self, world, parcel, rng):
        original = _with(parcel, "HUB")[-1]
        later = original.event_time + timedelta(minutes=int(rng.integers(1, 30)))
        record_time = later + (original.record_time - original.event_time)
        clone = _clone(
            world,
            original,
            event_id=original.event_id[:-4] + f"{9000 + int(rng.integers(1, 999)):04d}",
            event_time=later,
            record_time=record_time,
            source_ts=record_time,
            arrival=world.arrival_after(record_time, rng),
        )
        parcel.events.append(clone)
        return [
            Injection(
                business_key=f"{parcel.track_id}|HUB|{original.gln}",
                country=parcel.country,
                event_date=clone.local_date(),
                details={
                    "original_eventId": original.event_id,
                    "repeated_eventId": clone.event_id,
                    "trackId": parcel.track_id,
                    "status": "HUB",
                    "gln": original.gln,
                },
                records=[clone],
            )
        ]


@register
class CancelledAfterPickup(_ParcelInjector):
    name = "cancelled_after_pickup"

    def eligible(self, world):
        return [p for p in world.parcels.values() if p.picked_up and _with(p, "HUB")]

    def inject(self, world, parcel, rng):
        pickup, first_hub = _with(parcel, "PICKUP")[0], _with(parcel, "HUB")[0]
        gap = max(60, int((first_hub.event_time - pickup.event_time).total_seconds()) - 60)
        at = pickup.event_time + timedelta(seconds=int(rng.integers(30, gap)))
        record_time = at + timedelta(seconds=int(rng.integers(5, 120)))
        clone = _clone(
            world,
            pickup,
            event_id=pickup.event_id[:-4] + f"{9000 + int(rng.integers(1, 999)):04d}",
            status="CANCELLED",
            event_time=at,
            record_time=record_time,
            source_ts=record_time,
            arrival=world.arrival_after(record_time, rng),
        )
        parcel.events.append(clone)
        return [
            Injection(
                business_key=parcel.track_id,
                country=parcel.country,
                event_date=clone.local_date(),
                details={
                    "trackId": parcel.track_id,
                    "cancelled_eventId": clone.event_id,
                    "pickup_eventId": pickup.event_id,
                    "sequence": "PICKUP > CANCELLED > HUB",
                },
                records=[clone],
            )
        ]


@register
class DeliveredWithoutInDelivery(_ParcelInjector):
    name = "delivered_without_in_delivery"

    def eligible(self, world):
        return [
            p
            for p in world.parcels.values()
            if _with(p, "IN_DELIVERY") and _with(p, "DELIVERED") and not _with(p, "NOT_DELIVERED")
        ]

    def inject(self, world, parcel, rng):
        removed = _with(parcel, "IN_DELIVERY")[0]
        removed.dropped = True
        parcel.events.remove(removed)
        delivered = _with(parcel, "DELIVERED")[0]
        return [
            Injection(
                business_key=parcel.track_id,
                country=parcel.country,
                event_date=delivered.local_date(),
                details={
                    "trackId": parcel.track_id,
                    "removed_eventId": removed.event_id,
                    "sequence": "HUB > DELIVERED",
                },
                records=[delivered],  # the scan that arrives without its predecessor
            )
        ]


@register
class LateRecordTime(_ParcelInjector):
    name = "late_record_time"

    def eligible(self, world):
        return [p for p in world.parcels.values() if p.picked_up]

    def inject(self, world, parcel, rng):
        scans = [e for e in parcel.events if e.gln is not None and not e.dropped]
        event = scans[int(rng.integers(len(scans)))]
        original = event.record_time
        event.record_time = event.event_time + timedelta(seconds=int(rng.integers(2 * 3600, 12 * 3600)))
        event.source_ts = event.record_time
        event.arrival = world.arrival_after(event.record_time, rng)
        return [
            Injection(
                business_key=event.event_id,
                country=parcel.country,
                event_date=event.local_date(),
                details={
                    "field": "recordTime",
                    "original": utc_z(original),
                    "injected": utc_z(event.record_time),
                    "eventTime": local_iso(event.event_time, event.tz_name),
                    "delay_hours": round((event.record_time - event.event_time).total_seconds() / 3600, 2),
                    **_ref(event),
                },
                records=[event],
            )
        ]


@register
class CountryDayOutage(Injector):
    """Scanner gateway outage: every scan of one country and (week)day is recorded the next morning, so the
    landing has no scans of that country for that day, and they all arrive late (R5)."""

    name = "country_day_outage"

    def eligible(self, world):
        return [(c, d) for c in world.countries for d in world.window_days[:-1] if d.weekday() < 5]

    def locks(self, item):
        return [("outage", item)]

    def inject(self, world, item, rng):
        country, day = item
        zone_name = world.timezone(country)
        local = tz(zone_name)
        affected = [
            e
            for p in world.parcels.values()
            if p.country == country
            for e in p.events
            if not e.dropped and not e.anomaly_ids and e.record_time.astimezone(local).date() == day
        ]
        resume = at_local(day + timedelta(days=1), 6.0, zone_name)
        affected.sort(key=lambda e: e.record_time)
        for n, event in enumerate(affected):
            event.record_time = resume + timedelta(seconds=n // 20)  # backlog replayed at ~20 scans/s
            event.source_ts = event.record_time
            event.arrival = world.arrival_after(event.record_time, rng)
        return [
            Injection(
                business_key=f"{country}|{day.isoformat()}",
                country=country,
                event_date=day,
                details={
                    "affected_events": len(affected),
                    "replayed_from": utc_z(resume),
                    "replayed_until": utc_z(affected[-1].record_time) if affected else None,
                    "event_ids": [e.event_id for e in affected],
                },
                records=affected,
            )
        ]
