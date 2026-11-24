# CLAUDE.md

Portfolio case: a data quality platform for the Sales & Finance data products of **Veloparc**, a fictitious European parcel carrier. Python generator → Kafka (Aiven) → Snowflake via the Aiven Snowflake sink, and files → Snowflake internal stage → bronze → dbt (silver, gold, DQ). No AWS account (ADR 0003).

## Hard rules

- **Public repository.** Never commit secrets: no `.env`, private keys (`*.p8`, `*.pem`), Snowflake/AWS/Aiven credentials, account identifiers or tokens. Use `.env` (git-ignored) and `.env.example` with placeholders only. Run `detect-secrets scan` (or at least `git diff --cached` review) before every commit.
- **Fictitious company only.** Veloparc and all data are invented. Never reference real carriers or employers by name in code, comments, commit messages, docs, bucket names or screenshots.
- **The JSON Schemas in `contracts/schemas/` are the source of truth** for what each source sends. The generator must produce data that validates against them (except deliberately injected anomalies), and bronze validates against them.
- Commits follow Conventional Commits. Work on branches and merge via PR.

## Where things are

| Path | What |
| --- | --- |
| `docs/architecture/implementation_plan.md` | Phase-by-phase plan (0–11) and minimum path. Start here. |
| `docs/architecture/ingestion_kafka_s3_bronze.md` | Topics, headers, S3 landing layout, the 25 bronze metadata columns, bronze DQ |
| `contracts/schemas/` | 8 JSON Schemas (draft 2020-12), one per source |
| `contracts/examples/`, `contracts/validate_examples.py` | Valid example payloads and their validator |
| `contracts/README.md` | Generation order, cross-source rules **R1–R16**, volumes, anomalies to inject |
| `docs/data-model/silver_model.dbml`, `gold_star_schema.dbml` | Silver and gold (star schema) models |
| `docs/data-model/source_to_silver_mapping.md` | Column-by-column source → silver mapping with Snowflake expressions, gaps and decisions |
| `docs/data-model/silver_to_gold_mapping.md` | Silver → gold mapping, bus matrix, SCD rules, unknown members, gaps and decisions |
| `generator/` | Data generator (Phase 2): `config/` (simulation, anomalies, topics), `src/veloparc_gen/`, `tests/`, README with how to run and trace anomalies |
| `connect/` | Snowflake sink connector template, deployed by the `connector-deploy` workflow (key pair rotated on each run) |
| `infra/snowflake/` | One-time bootstrap SQL (run by a human) and schemachange migrations applied by the `snowflake-deploy` workflow |
| `dbt/veloparc/` | dbt project, deployed to Snowflake as `VLP_DP.DBT.VELOPARC` and executed there by the `dbt` workflow (dbt Projects on Snowflake, dbt Core 1.12.3, no Hub packages) |
| `docs/adr/` | Architecture decision records (0001: CI/CD pipeline; 0002: managed Kafka Connect and execution in GitHub Actions; 0003: no AWS, internal stage + Aiven Snowflake sink; 0004: bronze loaders, dedup flags, validation in dbt; Phase 0 decisions still to be written) |
| `Makefile`, `pyproject.toml`, `.pre-commit-config.yaml`, `.github/workflows/` | Tooling: `make setup lint test validate-contracts generate`; CI on PRs, merge on approval, deploy from main |

Planned layout for code (create as phases progress): `apps/streamlit/`.

## Key decisions already taken

- **Region:** Snowflake on AWS `eu-west-3` (Paris), in Snowflake's own cloud account. Aiven trial in the UK (DigitalOcean, London). No AWS account of ours.
- **Kafka:** Aiven, trial plan without a credit card (when the trial ends the service is powered off, never billed). Kept within the free-tier limits (5 topics × 2 partitions) so a downgrade keeps the topics working. On Kafka: shipments, tracking events, invoices, CRM, DLQ. Master data, tariffs, partner settlements and GL arrive as files in the same landing layout and envelope. `topics.yaml` has `transport: kafka | file` per topic.
- **Kafka → Snowflake:** managed Kafka Connect on Aiven running the Snowflake sink connector (Snowpipe Streaming) into one connector table per topic; configuration versioned in `connect/` and applied through the Aiven API by GitHub Actions. ADR 0002, ADR 0003.
- **Files → Snowflake:** the 2-month backfill (all topics) and the 4 file-based topics are uploaded by GitHub Actions to the internal stage `@BRONZE.LANDING`, same `landing/kafka/<topic>/year=/month=/day=[/hour=]/` layout; generator manifests and state go to `@BRONZE.GENERATOR`. ADR 0003.
- **Execution:** nothing runs on the maintainer's computer. Everything that touches a cloud service (generator runs, Kafka setup, Terraform, Snowflake deploys, dbt) runs in GitHub Actions, with credentials as secrets of the GitHub Environment `dev`. Claude cloud sessions write and test code and trigger workflows, but hold no credentials (Kafka is not reachable through the session's HTTPS proxy). ADR 0002.
- **Bronze:** outside dbt; one `RAW_*` table per business topic (8, no DLQ), append-only, 25 metadata columns + `RAW_PAYLOAD VARIANT` + `BK_*`, fed by two loaders over the data already in Snowflake (COPY from `@LANDING` run by a workflow; stream + triggered task from the connector tables). Dedup is flagged in `V_RAW_*` views (technical: topic, partition, offset, `ce_id`; resend: topic, `ce_id`) and applied by the dbt `stg_` models. No validation at load: JSON Schema rules are checked in dbt. Reference CSVs load into `REF`, generator manifests into `DQ`, in the same `bronze-load` run. ADR 0004.
- **dbt:** runs inside Snowflake (dbt Projects on Snowflake), deployed and executed by the `dbt` workflow; writes straight to `SILVER`, `GOLD`, `DQ`. `stg_` views → incremental `silver` (merge) → `gold` star schema → `dq` models. Hash surrogate keys with the project macro `surrogate_key()` (no Hub packages). Unknown members = -1.
- **Data quality:** each rule R1–R16 is a dbt test with `meta: {rule_id, dimension, owner}`; failures stored; `dq_issue`, `dq_scorecard_daily`, and `dq_detection_eval` comparing detections with the generator's `anomaly_manifest.jsonl` (precision/recall).

## Open items

- `contracts/examples/` has 12 coherent examples validated by `contracts/validate_examples.py` (schemas + R1, R2, R6, R7, R8, R9, R12, R14, R15). It runs in CI (`make validate-contracts`); keep it green whenever a schema changes.
- Open gaps listed in both mapping docs (e.g. `related_invoice_number`, FX reference table, postal-code → zone reference).
