"""Message envelope as the S3 Sink writes it: HoistField(payload) + InsertField(_kafka_*) + headers."""

from __future__ import annotations

import uuid
from datetime import datetime

import numpy as np

from veloparc_gen.config import Topic
from veloparc_gen.world import Record

SPEC_VERSION = "1.0"
_OP_NAME = {"I": "created", "U": "updated", "D": "deleted", "R": "snapshot"}

HEADER_NAMES = (
    "ce_id",
    "ce_source",
    "ce_type",
    "ce_time",
    "ce_specversion",
    "vlp_op",
    "vlp_producer",
    "vlp_entity",
    "vlp_batch_id",
    "vlp_batch_seq",
    "vlp_batch_count",
    "vlp_schema_version",
    "traceparent",
)


def iso_ms(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


def epoch_ms(ts: datetime) -> int:
    return int(ts.timestamp() * 1000)


def entity_of(record: Record, payload: dict, topic: Topic) -> str:
    if record.source == "master_data":
        return payload.get("entity", topic.entity)
    if record.source == "crm_object":
        return str(payload.get("attributes", {}).get("type", topic.entity)).lower()
    return topic.entity


def new_ids(rng: np.random.Generator) -> tuple[str, str]:
    """(ce_id, traceparent), reproducible from the seeded stream."""
    ce_id = str(uuid.UUID(bytes=rng.bytes(16), version=4))
    traceparent = f"00-{rng.bytes(16).hex()}-{rng.bytes(8).hex()}-01"
    return ce_id, traceparent


def headers(record: Record, payload: dict, topic: Topic, ce_id: str, traceparent: str) -> dict[str, str]:
    entity = entity_of(record, payload, topic)
    batch_id, seq, count = (
        (record.batch.batch_id, record.batch.seq, record.batch.count) if record.batch else (ce_id, 1, 1)
    )
    major = topic.name.rsplit(".v", 1)[-1]
    return {
        "ce_id": ce_id,
        "ce_source": f"urn:veloparc:{topic.producer}",
        "ce_type": f"vlp.{entity}.{_OP_NAME[record.op]}",
        "ce_time": iso_ms(record.source_ts),
        "ce_specversion": SPEC_VERSION,
        "vlp_op": record.op,
        "vlp_producer": topic.producer,
        "vlp_entity": entity,
        "vlp_batch_id": batch_id,
        "vlp_batch_seq": str(seq),
        "vlp_batch_count": str(count),
        "vlp_schema_version": f"{major}.0.0",
        "traceparent": traceparent,
    }


def envelope(payload, topic: str, partition: int, offset: int, timestamp_ms: int, hdrs: dict[str, str]) -> dict:
    return {
        "payload": payload,
        "_kafka_topic": topic,
        "_kafka_partition": partition,
        "_kafka_offset": offset,
        "_kafka_timestamp": timestamp_ms,
        "_kafka_headers": hdrs,
    }
