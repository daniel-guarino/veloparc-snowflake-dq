# CLAUDE.md

Portfolio case: a data quality platform for the Sales & Finance data products of **Veloparc**, a fictitious European parcel carrier. Python generator → Kafka (Aiven) and files → S3 landing → Snowflake (bronze via Snowpipe) → dbt (silver, gold, DQ).

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
| `contracts/README.md` | Generation order, cross-source rules **R1–R16**, volumes, anomalies to inject |
| `docs/data-model/silver_model.dbml`, `gold_star_schema.dbml` | Silver and gold (star schema) models |
| `docs/data-model/source_to_silver_mapping.md` | Column-by-column source → silver mapping with Snowflake expressions, gaps and decisions |
| `docs/data-model/silver_to_gold_mapping.md` | Silver → gold mapping, bus matrix, SCD rules, unknown members, gaps and decisions |
| `docs/adr/` | Architecture decision records (to be written, see Phase 0) |

Planned layout for code (create as phases progress): `generator/`, `connect/`, `infra/terraform/aws/`, `infra/snowflake/`, `dbt/veloparc/`, `apps/streamlit/`.

## Key decisions already taken

- **Region:** AWS `eu-west-3` (Paris) for S3 and Snowflake.
- **Kafka:** Aiven free tier (max 5 topics × 2 partitions, 125 KiB/s, no managed Kafka Connect). On Kafka: shipments, tracking events, invoices, CRM, DLQ. Master data, tariffs, partner settlements and GL arrive as files in the same landing layout and envelope. `topics.yaml` has `transport: kafka | file` per topic.
- **Kafka → S3:** self-hosted Kafka Connect (Docker on WSL) with Aiven's open-source S3 sink. 2-month backfill goes straight to S3 as files.
- **Bronze:** outside dbt; one `RAW_*` table and one Snowpipe per topic, append-only, 25 metadata columns + `RAW_PAYLOAD VARIANT`. Invalid payloads go to `BRONZE.DQ_QUARANTINE`.
- **dbt:** `stg_` views → incremental `silver` (merge) → `gold` star schema → `dq` models. Hash surrogate keys (`dbt_utils.generate_surrogate_key`). Unknown members = -1.
- **Data quality:** each rule R1–R16 is a dbt test with `meta: {rule_id, dimension, owner}`; failures stored; `dq_issue`, `dq_scorecard_daily`, and `dq_detection_eval` comparing detections with the generator's `anomaly_manifest.jsonl` (precision/recall).

## Open items

- `contracts/examples/` (11 valid example payloads, one per object type) is referenced in `contracts/README.md` but not committed yet; create it in Phase 2 and validate it in CI.
- Open gaps listed in both mapping docs (e.g. `related_invoice_number`, FX reference table, postal-code → zone reference).
