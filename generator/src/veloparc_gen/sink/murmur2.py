"""Kafka's murmur2 hash and default partitioner (org.apache.kafka.common.utils.Utils.murmur2)."""

from __future__ import annotations

_M = 0x5BD1E995
_MASK = 0xFFFFFFFF


def murmur2(data: bytes) -> int:
    """32-bit murmur2 with Kafka's seed, returned as a signed int like the Java implementation."""
    length = len(data)
    h = (0x9747B28C ^ length) & _MASK
    for i in range(0, length - length % 4, 4):
        k = data[i] | (data[i + 1] << 8) | (data[i + 2] << 16) | (data[i + 3] << 24)
        k = (k * _M) & _MASK
        k ^= k >> 24
        k = (k * _M) & _MASK
        h = (h * _M) & _MASK
        h ^= k
    tail = length & ~3
    extra = length % 4
    if extra == 3:
        h ^= data[tail + 2] << 16
    if extra >= 2:
        h ^= data[tail + 1] << 8
    if extra >= 1:
        h ^= data[tail]
        h = (h * _M) & _MASK
    h ^= h >> 13
    h = (h * _M) & _MASK
    h ^= h >> 15
    return h - (1 << 32) if h & 0x80000000 else h


def partition_for(key: str, partitions: int) -> int:
    """Default producer partitioner for keyed records: toPositive(murmur2(key)) % partitions."""
    return (murmur2(key.encode("utf-8")) & 0x7FFFFFFF) % partitions
