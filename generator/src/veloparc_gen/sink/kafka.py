"""Real Kafka producer for transport=kafka topics (Aiven), used by `veloparc-gen publish`.

Same key and the same 13 headers as the simulated broker; the value is the source payload (JSON), since the
Snowflake sink connector stores Kafka metadata (topic, partition, offset, timestamp, headers) next to it.
Partitions and offsets come from the broker; LogAppendTime is set by the topic configuration.

At-least-once behaviour is reproduced on purpose, at the rates in simulation.yaml (sink section):
- producer retry: the same message (same ce_id) produced twice -> technical duplicate for bronze;
- DLQ: a truncated copy of a message (not valid JSON) produced before the good one -> the connector routes it to
  vlp.dlq.s3-sink.v1 (errors.tolerance=all), while the intact message still lands.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field

from veloparc_gen.config import Topic
from veloparc_gen.sink.envelope import headers, new_ids
from veloparc_gen.world import Record, World

ProducerFactory = Callable[[dict], object]


def producer_config(connection: dict) -> dict:
    return connection | {
        "acks": "all",
        "enable.idempotence": True,
        "partitioner": "murmur2_random",  # same partition per key as the Java client and the simulated broker
        "compression.type": "zstd",
        "linger.ms": 50,
        "message.max.bytes": 4_194_304,
    }


@dataclass
class PublishReport:
    topics: dict[str, dict] = field(default_factory=dict)
    producer_retries: int = 0
    dlq_copies: int = 0
    failures: list[str] = field(default_factory=list)
    locations: dict[int, list[tuple[str, int, int, str]]] = field(default_factory=lambda: defaultdict(list))

    def as_dict(self) -> dict:
        return {
            "topics": self.topics,
            "at_least_once": {"producer_retries": self.producer_retries, "dlq_copies": self.dlq_copies},
            "failures": len(self.failures),
        }


class KafkaPublisher:
    def __init__(self, world: World, producer):
        self.world = world
        self.producer = producer
        self.report = PublishReport()

    def publish(self, records_by_source: dict[str, list[Record]], flush_timeout: float = 120.0) -> PublishReport:
        for source, records in records_by_source.items():
            topic = self.world.config.topics[source]
            if topic.transport == "kafka" and records:
                self._publish_topic(source, topic, records)
        remaining = self.producer.flush(flush_timeout)
        if remaining:
            self.report.failures.append(f"{remaining} message(s) still in the producer queue after flush")
        return self.report

    def _publish_topic(self, source: str, topic: Topic, records: list[Record]) -> None:
        rng = self.world.rng("publish", topic.name)
        sink = self.world.sim.sink
        stats = {"name": topic.name, "messages": 0, "delivered": 0, "partitions": {}}
        self.report.topics[source] = stats

        def on_delivery(record: Record, kind: str):
            def callback(err, msg):
                if err is not None:
                    self.report.failures.append(f"{topic.name} {record.business_key}: {err}")
                    return
                stats["delivered"] += 1
                part = stats["partitions"].setdefault(str(msg.partition()), {"first": msg.offset(), "last": 0})
                part["first"] = min(part["first"], msg.offset())
                part["last"] = max(part["last"], msg.offset())
                if kind != "dlq-copy":
                    self.report.locations[id(record)].append((topic.name, msg.partition(), msg.offset(), kind))

            return callback

        for record in sorted(records, key=lambda r: r.arrival):
            payload = record.payload(self.world)
            ce_id, traceparent = new_ids(rng)
            hdrs = [(k, v.encode()) for k, v in headers(record, payload, topic, ce_id, traceparent).items()]
            value = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
            key = record.key.encode()
            if rng.random() < sink.dlq_rate:
                cut = max(10, int(len(value) * float(rng.uniform(0.2, 0.8))))
                self._produce(topic.name, key, value[:cut], hdrs, on_delivery(record, "dlq-copy"), stats)
                self.report.dlq_copies += 1
            self._produce(topic.name, key, value, hdrs, on_delivery(record, "message"), stats)
            if rng.random() < sink.producer_retry_rate:
                self._produce(topic.name, key, value, hdrs, on_delivery(record, "retry"), stats)
                self.report.producer_retries += 1

    def _produce(self, topic: str, key: bytes, value: bytes, hdrs, callback, stats: dict) -> None:
        while True:
            try:
                self.producer.produce(topic, key=key, value=value, headers=hdrs, on_delivery=callback)
                break
            except BufferError:
                self.producer.poll(1.0)  # local queue full: let deliveries drain, then retry
        stats["messages"] += 1
        self.producer.poll(0)


def kafka_producer(connection: dict):
    from confluent_kafka import Producer

    return Producer(producer_config(connection))


class DryRunProducer:
    """Stand-in producer for --dry-run and tests: murmur2 partitions and local offsets, nothing leaves the host."""

    class _Msg:
        def __init__(self, topic: str, partition: int, offset: int):
            self._t, self._p, self._o = topic, partition, offset

        def topic(self):
            return self._t

        def partition(self):
            return self._p

        def offset(self):
            return self._o

    def __init__(self, partitions: dict[str, int]):
        from veloparc_gen.sink.murmur2 import partition_for

        self._partition_for = partition_for
        self.partitions = partitions
        self.offsets: dict[tuple[str, int], int] = defaultdict(int)
        self.pending: list = []
        self.sent: list[tuple[str, int, int, bytes, bytes, list]] = []

    def produce(self, topic, key, value, headers, on_delivery):
        partition = self._partition_for(key.decode(), self.partitions[topic])
        offset = self.offsets[(topic, partition)]
        self.offsets[(topic, partition)] += 1
        self.sent.append((topic, partition, offset, key, value, headers))
        self.pending.append((on_delivery, self._Msg(topic, partition, offset)))

    def poll(self, timeout=0):
        pending, self.pending = self.pending, []
        for callback, msg in pending:
            callback(None, msg)
        return len(pending)

    def flush(self, timeout=None):
        self.poll()
        return 0
