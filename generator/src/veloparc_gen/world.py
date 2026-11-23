"""Shared state passed between generation steps, and the record model every source produces."""

from __future__ import annotations

import zlib
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from functools import cache
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
from faker import Faker

from veloparc_gen.config import Config

CENT = Decimal("0.01")
ALNUM_UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
ALNUM_MIXED = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"


# --------------------------------------------------------------------------- helpers


def money(value: Any) -> Decimal:
    """Round half-up to cents (the rounding rule behind R7, R8 and R12)."""
    return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)


def num(value: Decimal) -> float | int:
    """Decimal -> JSON number. Cents-quantized Decimals survive the float round trip exactly as text."""
    return float(value)


@cache
def tz(name: str) -> ZoneInfo:
    return ZoneInfo(name)


def utc_z(ts: datetime) -> str:
    """2026-10-05T15:40:19Z"""
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def local_iso(ts: datetime, zone: str) -> str:
    """2026-10-05T16:40:12+01:00 (local time with offset)."""
    return ts.astimezone(tz(zone)).isoformat(timespec="seconds")


def salesforce_ts(ts: datetime) -> str:
    """2026-08-28T14:03:11.000+0000"""
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000+0000")


def at_local(day: date, hours: float, zone: str) -> datetime:
    """UTC instant of a local wall-clock time (fractional hours) on a given day."""
    base = datetime.combine(day, time(0), tzinfo=tz(zone))
    return (base + timedelta(seconds=round(hours * 3600))).astimezone(UTC)


def at_utc(day: date, hours: float = 0.0) -> datetime:
    return datetime.combine(day, time(0), tzinfo=UTC) + timedelta(seconds=round(hours * 3600))


def period_of(day: date) -> str:
    return f"{day.year:04d}-{day.month:02d}"


def next_month_day(period: str, day: int) -> date:
    year, month = map(int, period.split("-"))
    year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return date(year, month, day)


def days(start: date, end: date) -> list[date]:
    return [start + timedelta(n) for n in range((end - start).days + 1)]


# --------------------------------------------------------------------------- records


@dataclass(slots=True, eq=False)
class Batch:
    """Producer batch: vlp_batch_id / vlp_batch_seq / vlp_batch_count headers (_SOURCE_TXN_* in bronze)."""

    batch_id: str
    seq: int
    count: int


@dataclass(slots=True, eq=False)
class Record:
    """One message for one topic. ``data`` holds the payload for eager sources; lazy sources override
    ``build`` (tracking events and shipments are built on demand to keep memory low)."""

    source: str
    key: str
    source_ts: datetime  # business timestamp of the source (ce_time, _SOURCE_TS)
    arrival: datetime  # broker LogAppendTime: decides the landing folder
    country: str
    business_key: str
    op: str = "I"  # I insert, U update (after-image), D delete, R refresh (initial load)
    data: dict | None = None
    batch: Batch | None = None
    anomaly_ids: list[str] = field(default_factory=list)
    schema_invalid: bool = False  # set by schema anomalies: the payload must fail its JSON Schema
    dropped: bool = False  # removed by an anomaly (never written)

    def build(self, world: World) -> dict:
        if self.data is None:
            raise ValueError(f"{self.source} record {self.key} has no payload")
        return self.data

    def payload(self, world: World) -> dict:
        return self.build(world)


# --------------------------------------------------------------------------- domain objects


@dataclass(slots=True)
class Depot:
    gln: str
    name: str
    type: str
    country: str
    city: str
    postal_code: str
    zone: str
    scanners: list[str]


@dataclass(slots=True)
class Partner:
    partner_id: str
    name: str
    country: str
    zones: list[str]
    base_depot_gln: str
    cost_per_stop: Decimal
    parcels_per_stop: float


@dataclass(slots=True)
class Customer:
    contact_id: str
    group_id: str
    name: str
    country: str
    segment: str
    vat: str
    products: list[str]
    home_depot_gln: str
    volume_weight: float
    first_seen: datetime
    versions: list[dict] = field(default_factory=list)


@dataclass(slots=True)
class Tariff:
    tariff_id: str
    contact_id: str
    product: str
    zone: str
    currency: str
    valid_from: date
    valid_to: date | None
    fuel_pct: Decimal
    bands: list[tuple[Decimal, Decimal, Decimal]]  # (fromKg, toKg, price)

    def covers(self, day: date) -> bool:
        return self.valid_from <= day and (self.valid_to is None or day <= self.valid_to)

    def band_price(self, weight: Decimal) -> Decimal:
        for low, high, price in self.bands:
            if low < weight <= high:
                return price
        raise ValueError(f"no band in {self.tariff_id} for {weight} kg")


@dataclass(slots=True, eq=False)
class Parcel:
    track_id: str
    parcel_number: str
    sscc: str
    weight: Decimal
    message_id: str
    contact_id: str
    product: str
    country: str
    zone: str
    ship_date: date
    cancelled: bool = False
    picked_up: bool = False
    delivered_local_date: date | None = None
    delivered: bool = False
    events: list = field(default_factory=list)


# --------------------------------------------------------------------------- world


class World:
    """Everything generated so far. Each step reads what earlier steps produced and adds its own part."""

    def __init__(self, config: Config):
        self.config = config
        self.sim = config.simulation
        self.countries = list(self.sim.countries)
        self.window_days = days(self.sim.window.start, self.sim.window.end)
        self.window_start_utc = at_utc(self.sim.window.start)
        self.initial_load_at = self.window_start_utc + timedelta(minutes=self.sim.sink.initial_load_offset_minutes)

        self.depots: dict[str, Depot] = {}
        self.partners: dict[str, Partner] = {}
        self.zone_partner: dict[str, str] = {}
        self.customers: dict[str, Customer] = {}
        self.tariffs: dict[tuple[str, str, str], list[Tariff]] = {}
        self.parcels: dict[str, Parcel] = {}
        self.fx: dict[tuple[str, date], Decimal] = {}
        self.postal_zones: list[tuple[str, str, str]] = []  # (country, first digit, zone)
        self.records: dict[str, list[Record]] = {}
        self.reference: dict[str, list[dict]] = {}
        self._used_ids: dict[str, set[str]] = {}

    # ---- randomness: one independent, reproducible stream per named step
    def rng(self, *name: object) -> np.random.Generator:
        key = zlib.crc32("/".join(map(str, name)).encode())
        return np.random.default_rng(np.random.SeedSequence([self.sim.seed, key]))

    def faker(self, country: str, purpose: str) -> Faker:
        fake = Faker(self.sim.countries[country].locale)
        fake.seed_instance(int(self.rng("faker", country, purpose).integers(2**31)))
        return fake

    def unique_id(self, namespace: str, make) -> str:
        used = self._used_ids.setdefault(namespace, set())
        while True:
            value = make()
            if value not in used:
                used.add(value)
                return value

    @staticmethod
    def random_string(rng: np.random.Generator, alphabet: str, length: int) -> str:
        idx = rng.integers(0, len(alphabet), size=length)
        return "".join(alphabet[i] for i in idx)

    @staticmethod
    def uuid4(rng: np.random.Generator) -> str:
        import uuid

        return str(uuid.UUID(bytes=rng.bytes(16), version=4))

    # ---- lookups
    def zone_of_digit(self, digit: int) -> str:
        return f"Z{digit * self.sim.network.zones_per_country // 10 + 1}"

    def zone_digits(self, zone: str) -> list[int]:
        return [d for d in range(10) if self.zone_of_digit(d) == zone]

    def zones(self, country: str) -> list[str]:
        return [f"{country}-Z{n}" for n in range(1, self.sim.network.zones_per_country + 1)]

    def zone_of_postal(self, country: str, postal_code: str) -> str:
        return f"{country}-{self.zone_of_digit(int(postal_code[0]))}"

    def postal_code(self, country: str, zone: str, rng: np.random.Generator) -> str:
        fmt = self.sim.countries[country].postal_format
        digits = self.zone_digits(zone.split("-")[1])
        out, first = [], True
        for ch in fmt:
            if ch == "#":
                out.append(str(digits[rng.integers(len(digits))]) if first else str(rng.integers(10)))
                first = False
            else:
                out.append(ch)
        return "".join(out)

    def depots_in(self, country: str, zone: str | None = None, types: tuple[str, ...] = ()) -> list[Depot]:
        return [
            d
            for d in self.depots.values()
            if d.country == country and (zone is None or d.zone == zone) and (not types or d.type in types)
        ]

    def tariff_for(self, contact_id: str, product: str, zone: str, day: date) -> Tariff:
        matches = [t for t in self.tariffs.get((contact_id, product, zone), []) if t.covers(day)]
        if len(matches) != 1:
            raise LookupError(f"{len(matches)} tariffs for {contact_id} {product} {zone} on {day}")
        return matches[0]

    def fx_rate(self, currency: str, day: date) -> Decimal:
        return Decimal("1") if currency == "EUR" else self.fx[(currency, day)]

    def currency(self, country: str) -> str:
        return self.sim.countries[country].currency

    def timezone(self, country: str) -> str:
        return self.sim.countries[country].timezone

    def arrival_after(self, ts: datetime, rng: np.random.Generator) -> datetime:
        """LogAppendTime: a few seconds after the source timestamp, never before the initial load."""
        delay = self.sim.sink.arrival_delay_seconds
        arrival = ts + timedelta(seconds=int(rng.integers(delay.min, delay.max + 1)))
        return max(arrival, self.initial_load_at)

    def add(self, record: Record) -> Record:
        self.records.setdefault(record.source, []).append(record)
        return record

    def live(self, source: str) -> list[Record]:
        return [r for r in self.records.get(source, []) if not r.dropped]
