"""Schema gate before anything is written.

- A clean record that fails its JSON Schema is a generator bug: the run fails.
- A record broken by a schema anomaly must fail its schema (it goes to quarantine in bronze).
- A record broken by a business-rule anomaly must still pass its schema (only the DQ rules can catch it).
"""

from __future__ import annotations

import json
from collections import Counter
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


def validate_world(world: World, max_failures: int = 20) -> ValidationReport:
    """Validates every live record against its schema, in a single process (the full backfill takes a few
    minutes; simple and predictable beats parallel here)."""
    validator = SchemaValidator(world)
    report = ValidationReport()
    for source, records in world.records.items():
        if source not in validator.validators:
            continue
        for record in records:
            if record.dropped:
                continue
            payload = record.payload(world)
            valid = validator.is_valid(source, payload)
            report.checked[source] += 1
            if record.schema_invalid:
                report.expected_invalid[source] += 1
                if valid:
                    report.failures.append(
                        f"{source} {record.business_key}: schema anomaly {record.anomaly_ids} still passes the schema"
                    )
            elif not valid:
                kind = f"anomaly {record.anomaly_ids}" if record.anomaly_ids else "clean record"
                errors = "; ".join(validator.errors(source, payload)[:3])
                report.failures.append(f"{source} {record.business_key}: {kind} fails the schema: {errors}")
            if len(report.failures) >= max_failures:
                break
    if report.failures:
        raise ValidationFailed("schema gate failed:\n  " + "\n  ".join(report.failures[:max_failures]))
    return report
