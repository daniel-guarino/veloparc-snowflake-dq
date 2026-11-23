"""Schema gate before anything is written.

- A clean record that fails its JSON Schema is a generator bug: the run fails.
- A record broken by a schema anomaly must fail its schema (it goes to quarantine in bronze).
- A record broken by a business-rule anomaly must still pass its schema (only the DQ rules can catch it).
"""

from __future__ import annotations

import json
import multiprocessing
import os
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field

from jsonschema import Draft202012Validator, FormatChecker

from veloparc_gen.world import World


class ValidationFailed(RuntimeError):
    pass


@dataclass
class ValidationReport:
    checked: Counter = field(default_factory=Counter)
    expected_invalid: Counter = field(default_factory=Counter)
    failures: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "records_checked": dict(sorted(self.checked.items())),
            "schema_invalid_by_design": dict(sorted(self.expected_invalid.items())),
            "failures": len(self.failures),
        }


class SchemaValidator:
    def __init__(self, world: World):
        self.validators: dict[str, Draft202012Validator] = {}
        for source, topic in world.config.topics.items():
            if topic.schema_file:
                schema = json.loads((world.config.schema_dir / topic.schema_file).read_text(encoding="utf-8"))
                self.validators[source] = Draft202012Validator(schema, format_checker=FormatChecker())

    def errors(self, source: str, payload: dict) -> list[str]:
        return [f"{e.json_path}: {e.message}" for e in self.validators[source].iter_errors(payload)]

    def is_valid(self, source: str, payload: dict) -> bool:
        return self.validators[source].is_valid(payload)


_WORLD: World | None = None  # shared with forked workers (copy-on-write)


def _check_slice(task: tuple[str, int, int, int]) -> tuple[str, int, int, list[str]]:
    source, start, stop, max_failures = task
    world = _WORLD
    validator = SchemaValidator(world)
    checked = expected_invalid = 0
    failures: list[str] = []
    for record in world.records[source][start:stop]:
        if record.dropped:
            continue
        payload = record.payload(world)
        valid = validator.is_valid(source, payload)
        checked += 1
        if record.schema_invalid:
            expected_invalid += 1
            if valid:
                failures.append(
                    f"{source} {record.business_key}: schema anomaly {record.anomaly_ids} still passes the schema"
                )
        elif not valid:
            kind = f"anomaly {record.anomaly_ids}" if record.anomaly_ids else "clean record"
            errors = "; ".join(validator.errors(source, payload)[:3])
            failures.append(f"{source} {record.business_key}: {kind} fails the schema: {errors}")
        if len(failures) >= max_failures:
            break
    return source, checked, expected_invalid, failures


PARALLEL_THRESHOLD = 200_000  # below this many records, forking costs more than it saves


def validate_world(world: World, max_failures: int = 20, workers: int | None = None) -> ValidationReport:
    """Validates every live record. Large worlds are split across forked worker processes (POSIX only); a worker
    that dies raises BrokenProcessPool instead of hanging the run."""
    global _WORLD
    sources = [s for s in world.records if s in SchemaValidator(world).validators]
    chunk = 50_000
    tasks = [
        (source, start, start + chunk, max_failures)
        for source in sources
        for start in range(0, len(world.records[source]), chunk)
    ]
    total = sum(len(world.records[source]) for source in sources)
    if workers is None:
        workers = min(8, os.cpu_count() or 1) if total >= PARALLEL_THRESHOLD else 1
    _WORLD = world
    try:
        if workers > 1 and len(tasks) > 1 and "fork" in multiprocessing.get_all_start_methods():
            context = multiprocessing.get_context("fork")
            with ProcessPoolExecutor(max_workers=workers, mp_context=context) as pool:
                results = list(pool.map(_check_slice, tasks))
        else:
            results = [_check_slice(task) for task in tasks]
    finally:
        _WORLD = None
    report = ValidationReport()
    for source, checked, expected_invalid, failures in results:
        report.checked[source] += checked
        if expected_invalid:
            report.expected_invalid[source] += expected_invalid
        report.failures += failures
    if report.failures:
        raise ValidationFailed("schema gate failed:\n  " + "\n  ".join(report.failures[:max_failures]))
    return report
