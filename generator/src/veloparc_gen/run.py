"""Backfill run: world -> anomalies -> schema gate -> rule check -> landing files + manifests.

Output (out_dir), following the S3 layout of implementation_plan.md, Phase 4:
    landing/kafka/<topic>/year=/month=/day=[/hour=]/<topic>+<partition>+<startOffset>.json.gz
    reference/<TABLE>/<TABLE>.csv                      FX_RATE_DAILY, POSTAL_ZONE, COUNTRY_ALIAS
    _generator/runs/<run_id>/run_manifest.json         seed, parameters, counts, sink, validation, rules
    _generator/runs/<run_id>/anomaly_manifest.jsonl    answer key (one line per injected anomaly)
    _generator/runs/<run_id>/rule_violations.jsonl     what the R1-R16 checker found, to trace each anomaly
State (state_dir): offsets.json with the next offset per topic and partition.
"""

from __future__ import annotations

import copy
import csv
import io
import json
import shutil
from collections import Counter
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import timedelta
from pathlib import Path

from veloparc_gen import __version__
from veloparc_gen.anomalies import inject_all
from veloparc_gen.config import Config
from veloparc_gen.pipeline import build_world, check_world_rules
from veloparc_gen.sink.offsets import OffsetStore
from veloparc_gen.sink.writer import LandingWriter, summarize_locations
from veloparc_gen.validate import validate_world
from veloparc_gen.world import Record, World, at_utc

OUTPUT_DIRS = ("landing", "reference", "_generator")


class OutputExists(RuntimeError):
    pass


@dataclass
class RunResult:
    run_id: str
    run_dir: Path
    run_manifest: dict


def run_id_for(config: Config) -> str:
    w = config.simulation.window
    return f"backfill-{w.start:%Y%m%d}-{w.end:%Y%m%d}-{config.fingerprint()[:10]}"


def prepare(out_dir: Path, state_dir: Path, reset: bool, keep_state: bool = False) -> None:
    """Refuses to mix runs in one output folder. A backfill also starts from empty offsets; `publish` keeps the
    offsets (keep_state) so the file-based topics continue where the previous run stopped."""
    offsets = state_dir / "offsets.json"
    existing = [out_dir / d for d in OUTPUT_DIRS if (out_dir / d).exists()]
    if offsets.exists() and not keep_state:
        existing.append(offsets)
    if existing and not reset:
        listed = ", ".join(str(p) for p in existing)
        raise OutputExists(f"backfill output or state already exists ({listed}); use --reset to replace it")
    for path in existing:
        shutil.rmtree(path) if path.is_dir() else path.unlink()


def backfill(config: Config, out_dir: Path, state_dir: Path, reset: bool = False, log=print) -> RunResult:
    prepare(out_dir, state_dir, reset)
    run_id = run_id_for(config)
    run_dir = out_dir / "_generator" / "runs" / run_id

    log(f"[1/6] building the coherent world ({', '.join(config.simulation.countries)})")
    world = build_world(config)
    log("[2/6] injecting anomalies")
    anomalies = inject_all(world)
    log("[3/6] schema gate")
    validation = validate_world(world)
    log("[4/6] checking R1-R16")
    violations = check_world_rules(world)
    log("[5/6] writing landing files")
    offsets = OffsetStore(state_dir / "offsets.json")
    sink = LandingWriter(world, out_dir, offsets).write_all()
    offsets.save()
    log("[6/6] writing reference tables and manifests")
    _write_reference(world, out_dir)

    run_dir.mkdir(parents=True, exist_ok=True)
    entries = []
    for entry in anomalies.entries:
        locations = [loc for r in entry.injection.records for loc in sink.locations.get(id(r), [])]
        entry.landing = summarize_locations(locations)
        entries.append(entry.as_dict())
    _write_jsonl(run_dir / "anomaly_manifest.jsonl", entries)
    _write_jsonl(run_dir / "rule_violations.jsonl", [asdict(v) for v in violations])

    manifest = {
        "run_id": run_id,
        "mode": "backfill",
        "generator_version": __version__,
        "config_fingerprint": config.fingerprint(),
        "seed": config.simulation.seed,
        "window": {"from": str(config.simulation.window.start), "to": str(config.simulation.window.end)},
        "countries": list(config.simulation.countries),
        "scale": config.simulation.scale,
        "anomalies_enabled": config.anomalies.enabled,
        "records": {source: len(world.live(source)) for source in sorted(world.records)},
        "sink": sink.as_dict(),
        "validation": validation.as_dict(),
        "anomalies": anomalies.stats,
        "rules": _reconcile(violations, anomalies.entries),
        "outputs": {
            "landing": "landing/kafka/",
            "reference": sorted(f"reference/{t}/{t}.csv" for t in world.reference),
            "anomaly_manifest": f"_generator/runs/{run_id}/anomaly_manifest.jsonl",
            "rule_violations": f"_generator/runs/{run_id}/rule_violations.jsonl",
            "offsets": "offsets.json in the state directory",
        },
    }
    (run_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", "utf-8")
    return RunResult(run_id, run_dir, manifest)


def _reconcile(violations, entries) -> dict:
    """Per rule: violations found vs anomalies injected for it (primary) or expected to trigger it (related).
    A rule with violations and no anomaly behind it is listed in `unexplained`: a generator or rule bug."""
    found = Counter(v.rule for v in violations)
    primary = Counter(e.spec.rule for e in entries)
    related = Counter(r for e in entries for r in e.injection.related_rules)
    rules = sorted(set(found) | set(primary) | set(related))
    per_rule = {
        r: {"violations": found.get(r, 0), "anomalies": primary.get(r, 0), "related_anomalies": related.get(r, 0)}
        for r in rules
    }
    unexplained = [r for r in rules if found.get(r) and not primary.get(r) and not related.get(r)]
    undetected = [r for r in rules if r != "SCHEMA" and primary.get(r) and not found.get(r)]
    return {"per_rule": per_rule, "unexplained": unexplained, "undetected": undetected}


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=False) + "\n")


def _write_reference(world, out_dir: Path) -> None:
    for table, rows in sorted(world.reference.items()):
        path = out_dir / "reference" / table / f"{table}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
        path.write_text(buffer.getvalue(), encoding="utf-8")


# --------------------------------------------------------------------------- publish (real Kafka)


class PublishFailed(RuntimeError):
    pass


def world_view(world: World, keep: Callable[[Record], bool]) -> World:
    """Shallow copy of the world whose record lists only hold what `keep` accepts."""
    view = copy.copy(world)
    view.records = {source: [r for r in records if keep(r)] for source, records in world.records.items()}
    return view


def publish(config: Config, out_dir: Path, state_dir: Path, producer=None, reset: bool = False, log=print) -> RunResult:
    """Generates the window and publishes what *arrives* in it: transport=kafka topics to Aiven, file topics as
    landing files. Snapshot records (op R, older than the window) are left out: the backfill already has them.

    Intended use: backfill a history as files, then publish the following days through Kafka."""
    from veloparc_gen.kafka_conn import kafka_config
    from veloparc_gen.sink.kafka import KafkaPublisher, kafka_producer

    prepare(out_dir, state_dir, reset, keep_state=True)
    window = config.simulation.window
    run_id = f"publish-{window.start:%Y%m%d}-{window.end:%Y%m%d}-{config.fingerprint()[:10]}"
    run_dir = out_dir / "_generator" / "runs" / run_id

    log(f"[1/5] building the coherent world ({', '.join(config.simulation.countries)})")
    world = build_world(config)
    log("[2/5] injecting anomalies")
    anomalies = inject_all(world)
    log("[3/5] schema gate and R1-R16")
    validation = validate_world(world)
    violations = check_world_rules(world)

    start, end = world.window_start_utc, at_utc(window.end + timedelta(days=1))

    def emitted(record: Record) -> bool:
        return not record.dropped and record.op != "R" and start <= record.arrival < end

    view = world_view(world, emitted)
    kafka_sources = {s for s, t in config.topics.items() if t.transport == "kafka" and s != "dlq"}
    log("[4/5] writing file-based topics")
    offsets = OffsetStore(state_dir / "offsets.json")
    files = LandingWriter(world_view(view, lambda r: r.source not in kafka_sources), out_dir, offsets).write_all()
    offsets.save()
    log("[5/5] producing kafka topics")
    producer = producer or kafka_producer(kafka_config())
    report = KafkaPublisher(world, producer).publish({s: view.records.get(s, []) for s in sorted(kafka_sources)})
    if report.failures:
        raise PublishFailed("delivery failures:\n  " + "\n  ".join(report.failures[:20]))

    run_dir.mkdir(parents=True, exist_ok=True)
    entries = []
    for entry in anomalies.entries:
        records = [r for r in entry.injection.records if emitted(r)]
        if not records:
            continue  # broken record arrives outside the window
        file_locations = [loc for r in records for loc in files.locations.get(id(r), [])]
        entry.landing = summarize_locations(file_locations) + [
            {"topic": t, "partition": p, "offset": o, "kind": kind}
            for r in records
            for t, p, o, kind in report.locations.get(id(r), [])
        ]
        entries.append(entry.as_dict())
    _write_jsonl(run_dir / "anomaly_manifest.jsonl", entries)
    _write_jsonl(run_dir / "rule_violations.jsonl", [asdict(v) for v in violations])
    manifest = {
        "run_id": run_id,
        "mode": "publish",
        "generator_version": __version__,
        "config_fingerprint": config.fingerprint(),
        "seed": config.simulation.seed,
        "window": {"from": str(window.start), "to": str(window.end)},
        "countries": list(config.simulation.countries),
        "scale": config.simulation.scale,
        "anomalies_enabled": config.anomalies.enabled,
        "records_emitted": {s: len(rs) for s, rs in sorted(view.records.items()) if rs},
        "kafka": report.as_dict(),
        "files": files.as_dict(),
        "validation": validation.as_dict(),
        "anomalies": anomalies.stats,
        "anomaly_entries_emitted": len(entries),
        "rules": _reconcile(violations, anomalies.entries),
    }
    (run_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", "utf-8")
    return RunResult(run_id, run_dir, manifest)
