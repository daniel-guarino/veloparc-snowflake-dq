"""The contract examples must stay valid against the schemas and cross-source rules."""

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _load_validator():
    spec = importlib.util.spec_from_file_location("validate_examples", ROOT / "contracts/validate_examples.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_examples_pass_schemas():
    assert _load_validator().schema_errors() == []


def test_examples_pass_cross_source_rules():
    assert _load_validator().rule_errors() == []
