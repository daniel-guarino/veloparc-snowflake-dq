"""Simulated broker + S3 Sink: partitions, offsets, LogAppendTime, rotation, at-least-once and DLQ.

Landing layout (ingestion_kafka_s3_bronze.md):
    landing/kafka/<topic>/year=YYYY/month=MM/day=dd/hour=HH/<topic>+<partition>+<startOffset>.json.gz
    (topics with daily landing have no hour= folder)
The folder is the broker arrival time (LogAppendTime, UTC), not the event time.
"""

from __future__ import annotations

import gzip
import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from veloparc_gen.config import Topic
from veloparc_gen.sink.envelope import envelope, epoch_ms, headers, new_ids
from veloparc_gen.sink.murmur2 import partition_for
from veloparc_gen.sink.offsets import OffsetStore
from veloparc_gen.world import Record, World

_DLQ_ERROR_HEADERS = {
    "__connect.errors.exception.class.name": "org.apache.kafka.connect.errors.DataException",
    "__connect.errors.exception.message": "Converting byte[] to Kafka Connect data failed due to serialization error",
    "__connect.errors.connector.name": "vlp-s3-sink",
}


@dataclass(slots=True)
class _Msg:
    record: Record
    partition: int
    offset: int
    timestamp_ms: int
    ce_id: str
    traceparent: str
    kind: str = "normal"  # normal | retry | corrupt (goes to the DLQ instead of the topic file)


@dataclass
class SinkReport:
    topics: dict[str, dict] = field(default_factory=dict)
    files: int = 0
    redelivered_files: int = 0
    redelivered_messages: int = 0
    producer_retries: int = 0
    dlq_messages: list[dict] = field(default_factory=list)
    locations: dict[int, list[tuple[str, int, int, str]]] = field(default_factory=lambda: defaultdict(list))

    def as_dict(self) -> dict:
        return {
            "files": self.files,
            "topics": self.topics,
            "at_least_once": {
                "redelivered_files": self.redelivered_files,
                "redelivered_messages": self.redelivered_messages,
                "producer_retries": self.producer_retries,
            },
            "dlq": {"messages": len(self.dlq_messages), "items": self.dlq_messages},
        }


class LandingWriter:
    def __init__(self, world: World, out_dir: Path, offsets: OffsetStore):
        self.world = world
        self.out_dir = out_dir
        self.offsets = offsets
        self.sink_cfg = world.sim.sink
        self.report = SinkReport()

    # ------------------------------------------------------------------ public
    def write_all(self) -> SinkReport:
        dlq: list[_Msg] = []
        for source, topic in self.world.config.topics.items():
            if source == "dlq":
                continue
            records = [r for r in self.world.records.get(source, []) if not r.dropped]
            if records:
                dlq += self._write_topic(source, topic, records)
        if dlq:
            self._write_dlq(dlq)
        return self.report

    # ------------------------------------------------------------------ topics
    def _write_topic(self, source: str, topic: Topic, records: list[Record]) -> list[_Msg]:
        rng = self.world.rng("sink", topic.name)
        kafka = topic.transport == "kafka"
        order = sorted(range(len(records)), key=lambda i: (records[i].arrival, i))
        by_partition: dict[int, list[_Msg]] = defaultdict(list)
        last_ts: dict[int, int] = {}
        corrupt: list[_Msg] = []

        def append(record: Record, partition: int, ts_ms: int, ce_id: str, tp: str, kind: str) -> _Msg:
            ts_ms = max(ts_ms, last_ts.get(partition, 0))  # LogAppendTime never goes back in a partition
            last_ts[partition] = ts_ms
            msg = _Msg(record, partition, self.offsets.take(topic.name, partition), ts_ms, ce_id, tp, kind)
            by_partition[partition].append(msg)
            return msg

        for i in order:
            record = records[i]
            partition = partition_for(record.key, topic.partitions)
            ce_id, tp = new_ids(rng)
            ts_ms = epoch_ms(record.arrival)
            if kafka and rng.random() < self.sink_cfg.dlq_rate:
                # a truncated copy is produced first; the connector cannot read it and sends it to the DLQ
                corrupt.append(append(record, partition, ts_ms, ce_id, tp, "corrupt"))
            append(record, partition, ts_ms, ce_id, tp, "normal")
            if kafka and rng.random() < self.sink_cfg.producer_retry_rate:
                append(record, partition, ts_ms + int(rng.integers(50, 800)), ce_id, tp, "retry")
                self.report.producer_retries += 1

        stats = {"name": topic.name, "transport": topic.transport, "messages": 0, "files": 0, "partitions": {}}
        for partition in sorted(by_partition):
            msgs = by_partition[partition]
            written = [m for m in msgs if m.kind != "corrupt"]
            files = self._write_partition(topic, partition, written, rng, kafka)
            stats["messages"] += len(written)
            stats["files"] += files
            stats["partitions"][str(partition)] = {
                "first_offset": msgs[0].offset,
                "last_offset": msgs[-1].offset,
                "messages": len(written),
                "files": files,
            }
        self.report.topics[source] = stats
        return corrupt

    def _write_partition(self, topic: Topic, partition: int, msgs: list[_Msg], rng, kafka: bool) -> int:
        files = 0
        chunk: list[_Msg] = []
        bucket = None
        for msg in msgs:
            msg_bucket = self._bucket(topic, msg.timestamp_ms)
            if chunk and (msg_bucket != bucket or len(chunk) >= self.sink_cfg.flush_size):
                files += self._flush(topic, partition, bucket, chunk, rng, kafka)
                chunk = []
            bucket = msg_bucket
            chunk.append(msg)
        if chunk:
            files += self._flush(topic, partition, bucket, chunk, rng, kafka)
        return files

    def _flush(self, topic: Topic, partition: int, bucket: str, chunk: list[_Msg], rng, kafka: bool) -> int:
        lines = [self._line(topic, m) for m in chunk]
        self._write_file(topic, partition, bucket, chunk[0].offset, lines, chunk)
        files = 1
        if kafka and len(chunk) > 1 and rng.random() < self.sink_cfg.connector_redelivery_file_rate:
            # task restarted before committing: it re-reads from an earlier committed offset and writes the
            # tail again in a new file (same offsets, same ce_id). Bronze keeps the first copy.
            start = int(rng.integers(1, len(chunk)))
            self._write_file(topic, partition, bucket, chunk[start].offset, lines[start:], chunk[start:])
            self.report.redelivered_files += 1
            self.report.redelivered_messages += len(chunk) - start
            files += 1
        return files

    def _line(self, topic: Topic, msg: _Msg) -> str:
        payload = msg.record.payload(self.world)
        hdrs = headers(msg.record, payload, topic, msg.ce_id, msg.traceparent)
        return json.dumps(
            envelope(payload, topic.name, msg.partition, msg.offset, msg.timestamp_ms, hdrs),
            ensure_ascii=False,
            separators=(",", ":"),
        )

    # ------------------------------------------------------------------ DLQ
    def _write_dlq(self, corrupt: list[_Msg]) -> None:
        topic = self.world.config.topics["dlq"]
        rng = self.world.rng("sink", topic.name)
        by_partition: dict[int, list[tuple[_Msg, str, int, int]]] = defaultdict(list)
        for msg in sorted(corrupt, key=lambda m: (m.timestamp_ms, m.record.key)):
            src_topic = self.world.config.topics[msg.record.source]
            partition = partition_for(msg.record.key, topic.partitions)
            dlq_ts = msg.timestamp_ms + int(rng.integers(200, 3000))  # connector reads it, fails, forwards
            by_partition[partition].append((msg, src_topic.name, self.offsets.take(topic.name, partition), dlq_ts))
            self.report.dlq_messages.append(
                {
                    "source_topic": src_topic.name,
                    "source_partition": msg.partition,
                    "source_offset": msg.offset,
                    "ce_id": msg.ce_id,
                    "business_key": msg.record.business_key,
                    "note": "truncated duplicate; the intact message was delivered with the same ce_id",
                }
            )
        stats = {"name": topic.name, "transport": topic.transport, "messages": 0, "files": 0, "partitions": {}}
        for partition, items in sorted(by_partition.items()):
            chunk: list[tuple[str, int]] = []
            bucket, files = None, 0
            for msg, src_name, offset, ts in items:
                src_topic = self.world.config.topics[msg.record.source]
                payload = msg.record.payload(self.world)
                raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
                truncated = raw[: max(10, int(len(raw) * float(rng.uniform(0.2, 0.8))))]
                hdrs = headers(msg.record, payload, src_topic, msg.ce_id, msg.traceparent) | _DLQ_ERROR_HEADERS
                hdrs |= {
                    "__connect.errors.topic": src_name,
                    "__connect.errors.partition": str(msg.partition),
                    "__connect.errors.offset": str(msg.offset),
                }
                line = json.dumps(
                    envelope(truncated, topic.name, partition, offset, ts, hdrs),
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                msg_bucket = self._bucket(topic, ts)
                if chunk and msg_bucket != bucket:
                    self._write_file(topic, partition, bucket, chunk[0][1], [c[0] for c in chunk], [])
                    files += 1
                    chunk = []
                bucket = msg_bucket
                chunk.append((line, offset))
            if chunk:
                self._write_file(topic, partition, bucket, chunk[0][1], [c[0] for c in chunk], [])
                files += 1
            stats["messages"] += len(items)
            stats["files"] += files
            stats["partitions"][str(partition)] = {
                "first_offset": items[0][2],
                "last_offset": items[-1][2],
                "messages": len(items),
                "files": files,
            }
        self.report.topics["dlq"] = stats

    # ------------------------------------------------------------------ files
    @staticmethod
    def _bucket(topic: Topic, timestamp_ms: int) -> str:
        ts = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=timestamp_ms)
        folder = f"year={ts:%Y}/month={ts:%m}/day={ts:%d}"
        return folder + (f"/hour={ts:%H}" if topic.landing == "hour" else "")

    def _write_file(self, topic: Topic, partition: int, bucket: str, start: int, lines, msgs: list[_Msg]) -> None:
        rel = f"landing/kafka/{topic.name}/{bucket}/{topic.name}+{partition}+{start:010d}.json.gz"
        path = self.out_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        data = ("\n".join(lines) + "\n").encode("utf-8")
        path.write_bytes(gzip.compress(data, compresslevel=6, mtime=0))
        self.report.files += 1
        for msg in msgs:
            self.report.locations[id(msg.record)].append((topic.name, partition, msg.offset, rel))


def summarize_locations(locations: list[tuple[str, int, int, str]]) -> list[dict]:
    """Group a record set's landing positions per file: [{topic, partition, file, first/last offset, n}]."""
    per_file: dict[tuple[str, int, str], list[int]] = defaultdict(list)
    for topic, partition, offset, rel in locations:
        per_file[(topic, partition, rel)].append(offset)
    out = []
    for (topic, partition, rel), offsets in sorted(per_file.items()):
        out.append(
            {
                "topic": topic,
                "partition": partition,
                "file": rel,
                "first_offset": min(offsets),
                "last_offset": max(offsets),
                "messages": len(offsets),
            }
        )
    return out
