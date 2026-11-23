"""Typed configuration loaded from generator/config/*.yaml."""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CONFIG_DIR = REPO_ROOT / "generator" / "config"
DEFAULT_SCHEMA_DIR = REPO_ROOT / "contracts" / "schemas"


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Range(_Model):
    min: float
    max: float


class Country(_Model):
    currency: str
    vat_rate_pct: float
    timezone: str
    locale: str
    gs1_company_prefix: str = Field(pattern=r"^[0-9]{7}$")
    contact_id_prefix: str = Field(pattern=r"^[0-9]{2,3}$")
    postal_format: str
    share: float


class Window(_Model):
    start: date
    end: date


class Network(_Model):
    hubs_per_country: int
    depots_per_country: int
    parcelshops_per_country: int
    locker_share: float
    zones_per_country: int = Field(ge=1, le=9)
    partners_per_country: int


class Customers(_Model):
    count: int
    segments: dict[str, float]
    two_versions_share: float
    volume_pareto_shape: float
    first_seen: date


class Products(_Model):
    mix: dict[str, float]
    second_product_share: float


class Bands(_Model):
    limits_kg: list[float]
    base_price_eur: list[float]


class Tariffs(_Model):
    price_change_share: float
    price_change_date: date
    fuel_surcharge_pct: Range
    bands: dict[str, Bands]
    zone_factor: dict[str, float]
    customer_discount: Range
    remote_area_surcharge_eur: float


class Crm(_Model):
    opportunities: int
    leads: int
    converted_lead_share: float
    closed_won_share: float
    history_start: date


class Peak(_Model):
    date: date
    factor: float


class Weight(_Model):
    median: float
    sigma: float


class Shipments(_Model):
    per_working_day: float
    weekend_factor: float
    peaks: list[Peak]
    parcels_per_shipment: dict[int, float]
    max_weight_kg: float
    weight_kg: dict[str, Weight]
    cancelled_share: float
    shop_delivery_share: float
    business_hours_local: dict[str, int]


class Delay(_Model):
    median: float
    sigma: float
    max: float


class Tracking(_Model):
    delivery_mix: dict[str, float]
    hubs_per_parcel: dict[int, float]
    record_delay_seconds: Delay
    max_normal_delay_minutes: float


class Invoicing(_Model):
    issue_day: int
    max_lines_per_invoice: int
    credit_note_invoice_share: float
    credit_note_line_share: float
    credit_note_delay_days: Range


class FxSeries(_Model):
    start: float
    daily_sigma: float


class Settlements(_Model):
    cost_per_stop: dict[str, Range]
    parcels_per_stop: Range
    issue_delay_days: int


class Gl(_Model):
    issue_delay_days: int


class Sink(_Model):
    flush_size: int
    arrival_delay_seconds: Range
    initial_load_offset_minutes: int
    connector_redelivery_file_rate: float
    producer_retry_rate: float
    dlq_rate: float


class Simulation(_Model):
    seed: int
    window: Window
    scale: float
    countries: dict[str, Country]
    network: Network
    customers: Customers
    products: Products
    tariffs: Tariffs
    crm: Crm
    shipments: Shipments
    tracking: Tracking
    invoicing: Invoicing
    fx: dict[str, FxSeries]
    settlements: Settlements
    gl: Gl
    sink: Sink

    @model_validator(mode="after")
    def _check(self) -> Simulation:
        if self.window.end < self.window.start:
            raise ValueError("window.end must be on or after window.start")
        for code, country in self.countries.items():
            if country.currency != "EUR" and country.currency not in self.fx:
                raise ValueError(f"no fx series for {country.currency} ({code})")
        return self


class Topic(_Model):
    name: str
    transport: Literal["kafka", "file"]
    partitions: int = Field(ge=1)
    landing: Literal["hour", "day"]
    key: str
    schema_file: str | None = Field(alias="schema")
    producer: str
    entity: str


class Anomaly(_Model):
    rule: str
    dq_dimension: str
    source: str
    schema_anomaly: bool
    description: str
    rate: float | None = None
    count: int | None = None

    @model_validator(mode="after")
    def _one_of(self) -> Anomaly:
        if (self.rate is None) == (self.count is None):
            raise ValueError("set exactly one of rate or count")
        return self


class Anomalies(_Model):
    enabled: bool
    anomalies: dict[str, Anomaly]


class Config(_Model):
    simulation: Simulation
    topics: dict[str, Topic]
    anomalies: Anomalies
    schema_dir: Path

    def fingerprint(self) -> str:
        """Stable hash of the effective configuration (goes into run_id and run_manifest)."""
        blob = json.dumps(
            {
                "simulation": self.simulation.model_dump(mode="json"),
                "topics": {k: v.model_dump(mode="json", by_alias=True) for k, v in self.topics.items()},
                "anomalies": self.anomalies.model_dump(mode="json"),
            },
            sort_keys=True,
        )
        return hashlib.sha256(blob.encode()).hexdigest()


def _read(path: Path) -> dict:
    with path.open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def load_config(
    config_dir: Path = DEFAULT_CONFIG_DIR,
    schema_dir: Path = DEFAULT_SCHEMA_DIR,
    *,
    start: date | None = None,
    end: date | None = None,
    countries: list[str] | None = None,
    seed: int | None = None,
    scale: float | None = None,
    anomalies_enabled: bool | None = None,
) -> Config:
    """Load the three YAML files and apply CLI overrides."""
    sim = _read(config_dir / "simulation.yaml")
    if start is not None:
        sim["window"]["start"] = start
    if end is not None:
        sim["window"]["end"] = end
    if seed is not None:
        sim["seed"] = seed
    if scale is not None:
        sim["scale"] = scale
    if countries:
        unknown = sorted(set(countries) - set(sim["countries"]))
        if unknown:
            raise ValueError(f"countries not configured in simulation.yaml: {unknown}")
        sim["countries"] = {c: sim["countries"][c] for c in sim["countries"] if c in countries}
    anomalies = _read(config_dir / "anomalies.yaml")
    if anomalies_enabled is not None:
        anomalies["enabled"] = anomalies_enabled
    return Config(
        simulation=Simulation.model_validate(sim),
        topics={k: Topic.model_validate(v) for k, v in _read(config_dir / "topics.yaml")["topics"].items()},
        anomalies=Anomalies.model_validate(anomalies),
        schema_dir=schema_dir,
    )
