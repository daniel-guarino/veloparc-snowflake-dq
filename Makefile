# Veloparc · developer and CI entry points. Requires uv (https://docs.astral.sh/uv/).
.DEFAULT_GOAL := help
.PHONY: help setup lint format test validate-contracts validate-powerbi generate clean dbt-parse

UV ?= uv
FROM ?= 2026-09-01
TO ?= 2026-10-31
COUNTRIES ?= PT,ES,DE,PL
OUT ?= out/

help: ## List targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

setup: ## Install pinned dependencies and the git hooks
	$(UV) sync --frozen
	$(UV) run pre-commit install

lint: ## ruff (lint + format check) and all pre-commit hooks
	$(UV) run ruff check .
	$(UV) run ruff format --check .
	$(UV) run pre-commit run --all-files --show-diff-on-failure
	$(MAKE) dbt-parse

dbt-parse: ## Parse the dbt project offline (no Snowflake connection; placeholder profile)
	mkdir -p dbt/veloparc/.ci && cp dbt/veloparc/profiles.example.yml dbt/veloparc/.ci/profiles.yml
	$(UV) run dbt parse --project-dir dbt/veloparc --profiles-dir dbt/veloparc/.ci --no-version-check

format: ## Auto-fix lint and formatting
	$(UV) run ruff check --fix .
	$(UV) run ruff format .

test: ## Unit and integration tests with coverage
	$(UV) run pytest --cov=veloparc_gen --cov-report=term-missing:skip-covered

validate-contracts: ## Contract examples against schemas and rules R1, R2, R6-R9, R12, R14, R15
	$(UV) run python contracts/validate_examples.py

validate-powerbi: ## Power BI project: JSON, TMDL, columns vs dbt, DAX and report fields, relationships, no account ids
	$(UV) run python powerbi/tools/validate_pbip.py

generate: ## Backfill (replaces previous output): landing files, run and anomaly manifests into $(OUT)
	$(UV) run veloparc-gen backfill --from $(FROM) --to $(TO) --countries $(COUNTRIES) --out $(OUT) --reset

clean: ## Remove generator output, state and caches
	rm -rf out/ state/ .pytest_cache .ruff_cache .coverage
