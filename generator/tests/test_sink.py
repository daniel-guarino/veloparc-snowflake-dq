import pytest

from veloparc_gen.sink.murmur2 import murmur2, partition_for


@pytest.mark.parametrize(
    ("text", "expected"),
    [  # vectors from Kafka's UtilsTest.testMurmur2
        ("21", -973932308),
        ("foobar", -790332482),
        ("a-little-bit-long-string", -985981536),
        ("a-little-bit-longer-string", -1486304829),
        ("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971),
        ("abc", 479470107),
    ],
)
def test_murmur2_matches_kafka(text, expected):
    assert murmur2(text.encode()) == expected


def test_same_key_same_partition():
    assert {partition_for("ZK7Q2M9P", 6) for _ in range(5)} == {partition_for("ZK7Q2M9P", 6)}
    assert all(0 <= partition_for(f"k{i}", 3) < 3 for i in range(100))
