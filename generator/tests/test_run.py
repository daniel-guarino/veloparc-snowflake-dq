"""End-to-end backfill: landing layout, envelope, offsets, manifests and reproducibility."""

import gzip
import hashlib
import json
import re
from collections import defaultdict
from datetime import date
from pathlib import Path

import pytest
from conftest import small_config
from typer.testing import CliRunner

from veloparc_gen.cli import app
from veloparc_gen.run import OutputExists, backfill
from veloparc_gen.sink.envelope import HEADER_NAMES

FILE_RE = re.compile(
    r"landing/kafka/(?P<topic>vlp\.[a-z0-9.-]+)/year=\d{4}/month=\d{2}/day=\d{2}(/hour=\d{2})?/"
    r"(?P=topic)\+(?P<partition>\d+)\+(?P<start>\d{10})\.json\.gz"
)


def _run(tmp: Path, **overrides):
    config = small_config(**overrides)
    return backfill(config, tmp / "out", tmp / "state", log=lambda *_: None)


def _tree_hashes(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("run")
    return tmp, _run(tmp)


def test_landing_layout_and_envelope(run):
    tmp, result = run
    out = tmp / "out"
    files = sorted(out.glob("landing/kafka/**/*.json.gz"))
    assert files
    topics = set()
    for path in files:
        rel = str(path.relative_to(out))
        match = FILE_RE.fullmatch(rel)
        assert match, rel
        topics.add(match["topic"])
        daily = "partner-settlement" in rel or "gl-extract" in rel
        assert ("/hour=" not in rel) == daily, rel
        first = json.loads(gzip.decompress(path.read_bytes()).splitlines()[0])
        assert set(first) == {
            "payload",
            "_kafka_topic",
            "_kafka_partition",
            "_kafka_offset",
            "_kafka_timestamp",
            "_kafka_headers",
        }
        assert first["_kafka_offset"] == int(match["start"])
        assert set(HEADER_NAMES) <= set(first["_kafka_headers"])
    expected = {t["name"] for s, t in result.run_manifest["sink"]["topics"].items()}
    assert topics == expected
    assert {"vlp.ops.tracking-event.v1", "vlp.finance.invoice.v1", "vlp.mdm.master-data.v1"} <= topics


def test_offsets_are_contiguous_per_partition(run):
    """Every offset of a partition is either in a landing file or was sent to the DLQ (gaps are explained)."""
    tmp, result = run
    seen = defaultdict(set)
    for path in (tmp / "out").glob("landing/kafka/**/*.json.gz"):
        for line in gzip.decompress(path.read_bytes()).splitlines():
            msg = json.loads(line)
            seen[(msg["_kafka_topic"], msg["_kafka_partition"])].add(msg["_kafka_offset"])
    for item in result.run_manifest["sink"]["dlq"]["items"]:
        seen[(item["source_topic"], item["source_partition"])].add(item["source_offset"])
    for (topic, partition), offsets in seen.items():
        assert offsets == set(range(min(offsets), max(offsets) + 1)), (topic, partition)
    state = json.loads((tmp / "state" / "offsets.json").read_text())
    for (topic, partition), offsets in seen.items():
        assert state[topic][str(partition)] == max(offsets) + 1


def test_manifests(run):
    tmp, result = run
    manifest = result.run_manifest
    lines = (result.run_dir / "anomaly_manifest.jsonl").read_text().splitlines()
    assert len(lines) == sum(s["manifest_entries"] for s in manifest["anomalies"].values()) > 0
    first = json.loads(lines[0])
    assert first["landing"] and first["landing"][0]["file"].startswith("landing/kafka/")
    assert manifest["rules"]["unexplained"] == []
    assert manifest["rules"]["undetected"] == []
    assert (result.run_dir / "run_manifest.json").exists()
    for table in ("FX_RATE_DAILY", "POSTAL_ZONE", "COUNTRY_ALIAS"):
        assert (tmp / "out" / "reference" / table / f"{table}.csv").exists()


def test_existing_output_needs_reset(run):
    tmp, _ = run
    with pytest.raises(OutputExists):
        _run(tmp)


def test_same_seed_same_files_and_other_seed_differs(tmp_path):
    a = _run(tmp_path / "a", end=date(2026, 9, 2))
    b = _run(tmp_path / "b", end=date(2026, 9, 2))
    assert a.run_id == b.run_id
    hashes_a = _tree_hashes(tmp_path / "a" / "out")
    assert hashes_a == _tree_hashes(tmp_path / "b" / "out")
    assert (tmp_path / "a" / "state" / "offsets.json").read_bytes() == (
        tmp_path / "b" / "state" / "offsets.json"
    ).read_bytes()
    c = _run(tmp_path / "c", end=date(2026, 9, 2), seed=7)
    assert c.run_id != a.run_id
    assert _tree_hashes(tmp_path / "c" / "out") != hashes_a


def test_cli_backfill(tmp_path):
    result = CliRunner().invoke(
        app,
        [
            "backfill",
            "--from",
            "2026-09-01",
            "--to",
            "2026-09-01",
            "--countries",
            "PT",
            "--no-anomalies",
            "--out",
            str(tmp_path / "out"),
            "--state-dir",
            str(tmp_path / "state"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "run backfill-20260901-20260901-" in result.output
    manifests = list((tmp_path / "out" / "_generator" / "runs").glob("*/anomaly_manifest.jsonl"))
    assert len(manifests) == 1 and manifests[0].read_text() == ""
