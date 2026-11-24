# Veloparc · Step-by-step implementation plan

As of 2026-11-23 · Daniel Guarino

## Overview

The path is: Python generator → Aiven Kafka (continuous sources) and files in the exact S3 Sink format (history and batch sources) → S3 landing → Snowpipe into bronze → dbt for silver, gold and data quality. Building in this order puts real data in bronze by the end of the first week and adds layers without redoing anything upstream.

**Repository name (suggestion): `veloparc-snowflake-dq`.** It names the fictitious company, the platform and the focus (data quality). Alternatives: `veloparc-sales-finance-dataproducts` (more descriptive) or `parcel-lakehouse-dq-case` (neutral, no brand).

Principles that guide every phase:

- **Real broker only where it adds value.** What the case must prove starts at the file the S3 Sink leaves in the landing zone. Continuous sources go through a real managed Kafka (Aiven); history and batch sources are written by the generator byte for byte in the same format (envelope, headers, partition, offset).
- **Anomalies with an answer key.** Every injected anomaly is logged in a manifest. At the end, you measure how many the DQ rules caught (recall) and how many false alarms they raised (precision). This turns data quality from a claim into a measured result.
- **Everything as code and reproducible.** Fixed seed in the generator, infrastructure in Terraform and versioned SQL, dbt for transformations and tests, CI in GitHub Actions. Anyone who clones the repo can recreate everything.
- **Cost close to zero.** XS warehouses with 60 s auto-suspend, a resource monitor, S3 lifecycle rules. The Snowflake trial lasts 30 days or until the free credits run out, whichever comes first, so open the account only once the generator works.
- **Fictitious data only** in code, commits, bucket names, docs and screenshots.

## Phase 0 · Accounts and decisions before starting

Settle four decisions before writing code: region, Snowflake edition, when to open the trial, and the open architecture decisions.

1. **Region: AWS `eu-west-3` (Paris)** for the bucket and the Snowflake account. The same region avoids transfer costs, and Snowpipe auto-ingest requires the bucket on the same cloud. The available Snowflake account is in Paris, so the bucket and the rest of AWS also go in eu-west-3.
2. **Snowflake: Enterprise edition on the trial.** It gives masking policies (consignee PII) and extended Time Travel, both worth showing in the demo.
3. **Open the trial only in Phase 5.** Phases 1 to 4 don't need Snowflake. That way the 30 days cover bronze, dbt, DQ and the demo.
4. **AWS:** personal account, IAM user without root access, MFA on, and a 5 USD monthly budget alert.
5. **Close the open decisions** from the ingestion page and the mappings. Each becomes a short ADR in `docs/adr/`:

| Open decision | Proposal for the case |
| --- | --- |
| CRM in one topic or two | One topic, routed by `attributes.type` (already in the mapping) |
| GL via Kafka or direct file | Direct file (extract), since it doesn't fit in the Aiven free-tier topic limit; the batch control (`_SOURCE_TXN_COUNT`) stays the same |
| HeaderToField on the connector | Check on the Aiven S3 sink; for file-based topics the generator writes `_kafka_headers` directly |
| Landing retention | 90 days in S3 Standard, then Glacier; bronze is the permanent record |
| `related_invoice_number` in silver and gold | Add it (gap #1 of the source → silver mapping) |
| FX rates for settlements and CRM | `REF.FX_RATE_DAILY` seed produced by the generator |
| Parcel delivery zone | `REF.POSTAL_ZONE` seed derived from master data |
| Surrogate keys | Hash (`dbt_utils.generate_surrogate_key`) instead of sequences: idempotent on full refresh |

## Phase 1 · Public GitHub repository

Create the empty repo with an MIT licence, the structure below and the code-quality tooling before the first script. The documents already in the project go in with the first commit.

1. Create `veloparc-snowflake-dq` as public, with README, Python `.gitignore` and MIT licence.
2. Configure git with an email that is not the client's (`daniel@guarinoit.com` or the GitHub noreply address).
3. Protect the `main` branch: changes only via PR, with the CI checks required (Phase 10).
4. Python with `uv` (`pyproject.toml` + lockfile), `pre-commit` with `ruff`, `sqlfluff` (snowflake dialect, dbt templater), `check-jsonschema` and `detect-secrets`.
5. `Makefile` with the targets the demo will use: `make generate`, `make upload`, `make stream`, `make dbt-build`, `make dq-report`.
6. `.env.example` with the variable names (Snowflake account, user, private key path, bucket, Aiven credentials). The real `.env` never enters the repo.
7. Copy the project docs into the repo: schemas, examples, DBML, mappings and the ingestion page.

```text
veloparc-snowflake-dq/
├── README.md                  # architecture, how to run, DQ results
├── Makefile
├── pyproject.toml
├── .github/workflows/         # ci.yml, deploy.yml, generate.yml, stream.yml
├── docs/
│   ├── architecture/          # ingestion_kafka_s3_bronze.md, diagrams
│   ├── data-model/            # *.dbml, source→silver→gold mappings
│   └── adr/                   # 0001-crm-single-topic.md, ...
├── contracts/
│   ├── schemas/               # 8 JSON Schemas (source of truth)
│   └── examples/              # 11 valid examples
├── generator/
│   ├── config/                # simulation.yaml, anomalies.yaml, topics.yaml
│   ├── src/veloparc_gen/      # master, tariffs, crm, shipments, events, invoices, ...
│   │   ├── anomalies/         # one injector per anomaly
│   │   └── sink/              # Kafka producer + simulated broker: partitioner, envelope, file writer
│   └── tests/
├── connect/                   # S3 sink connector config for the managed Kafka Connect on Aiven
├── infra/
│   ├── terraform/aws/         # bucket, IAM role, notifications
│   └── snowflake/             # V001__rbac.sql, V002__storage.sql, ... (schemachange)
├── dbt/veloparc/
│   ├── models/{staging,silver,gold,dq}/
│   ├── macros/  tests/  seeds/  snapshots/
│   └── dbt_project.yml
└── apps/streamlit/            # DQ scorecard (Streamlit in Snowflake)
```

Suggested first commit: `chore: scaffold repo, contracts and docs`. Use Conventional Commits from then on; the repo history is evaluated too.

## Phase 2 · Data generator

The generator first builds a coherent world in memory, then injects the anomalies, and only at the end serializes. Keeping these three steps apart is what gives an exact answer key of what was broken.

**Stack:** Python 3.12, `numpy` (distributions), `Faker` with pt_PT, es_ES, de_DE and pl_PL locales (names and addresses), `jsonschema` (validation), `pydantic` (configuration), `typer` (CLI). A global seed in `simulation.yaml` makes every run reproducible.

1. **Configuration.** `simulation.yaml` with the parameters from the volumes table in the schemas README: countries PT/ES/DE/PL, 300 customers, ~3,000 shipments per working day, weekends at 20%, Black Friday ×2.5, log-normal weight with a 1.5 kg median. `anomalies.yaml` with the rate of each anomaly (1 to 2%).
2. **GS1 utilities first.** Functions for GLN and SSCC check digits, SSCC ↔ EPC URN conversion, VAT number with country prefix. Unit tests for each: they underpin rule R15 and the `GS1_SSCC_FROM_URN` UDF.
3. **Generation in dependency order** (one module per step, each receiving the state of the previous ones):
    1. Master data: depots with GLN, partners and zones (one per zone, R16), customers with 10% in two versions.
    2. Tariffs per customer × product × zone, contiguous bands and increasing price (R14).
    3. Opportunities and leads; "Closed Won" linked to customers, 25% of leads converted.
    4. VeloShip shipments with 1 to 5 parcels and one TrackID per parcel (R1).
    5. Tracking events per TrackID, following the R3 state machine, with `recordTime` seconds to minutes after `eventTime` (R5).
    6. Monthly invoices per customer, one line per collected TrackID, correct band price and FUEL surcharge (R7, R8), negative credit notes (R9), daily PLN rate (R10).
    7. Partner settlements per zone and month, counted from DELIVERED events (R11, R12).
    8. General ledger extract per entity and month equal to the sum of invoices (R13).
    9. Reference tables: `FX_RATE_DAILY`, `POSTAL_ZONE`, `COUNTRY_ALIAS` (CSV output for dbt seeds).
4. **Anomaly injection.** One injector per row of the anomalies table (12-digit GLN, `DELIVERD`, repeated event, `CANCELLED` after `PICKUP`, positive credit note, overlapping tariff, etc.). Each injector alters the record and writes a line to `anomaly_manifest.jsonl`: `anomaly_id`, `rule`, `dq_dimension`, `source`, `business_key`, `country`, `event_date`, `description`.
5. **Validation.** Before writing, every "clean" record is validated against its schema. An invalid clean record is a generator bug and fails the run. Deliberately broken records must fail the schema when the anomaly is a schema anomaly, and pass when it is a business-rule anomaly.
6. **Tests (`pytest`).** The 11 examples validate; a small run (1 country, 3 days) meets R1 to R16 with anomalies turned off; the same seed produces identical files.
7. **Two run modes.** `backfill` generates the 2 months of history in one go. `stream` generates the next hour from saved state (`state/` with the last offset per partition and the TrackIDs in transit), to show incremental ingestion live.

Reference volume: ~130 thousand shipments and close to 1 million events over 2 months. Enough to show performance and cost, and the generator runs on a laptop in minutes. The scale can be raised in the YAML to discuss production-scale volumes (tens of millions of records per day).

## Phase 3 · Kafka on Aiven + files

> **Update (ADR 0003):** no AWS account. Kafka topics reach Snowflake through the Aiven-managed **Snowflake sink connector**; files (backfill and file-based topics) go to the Snowflake internal stage `@BRONZE.LANDING`. Where this section mentions S3 or the S3 sink, read the internal stage and the Snowflake sink. New order: 3a producer, `stream` and Kafka setup through the Aiven API → 3b Snowflake foundation (Phase 5, moved earlier) → 3c Snowflake sink connector → 3d backfill upload to the internal stage.

Decision: real managed Kafka on Aiven for the continuous sources, and files written straight to S3 for the history and for the sources that don't fit within the limits. The generator has code for every topic; only those the plan allows are connected to the broker. The S3 landing and everything downstream are the same on both paths.

**Plan.** The service runs on Aiven's trial plan, without a credit card: managed Kafka Connect is available, and when the trial ends the service is powered off (never billed). The topology stays within the free-tier limits below, so a downgrade keeps the topics working; only Kafka Connect would be lost (fallback: self-hosted Connect, see alternatives). See ADR 0002.

**Free-tier limits and how the case adapts** ([Aiven docs](https://aiven.io/docs/products/kafka/free-tier/kafka-free-tier), [changelog](https://aiven.io/changelog/6d8b5811-882e-4a87-a346-66fbca7ed709)):

| Limit | Adaptation |
| --- | --- |
| Up to 5 topics, 2 partitions each | On Kafka: `vlp.shipping.veloship-shipment.v1`, `vlp.ops.tracking-event.v1`, `vlp.finance.invoice.v1`, `vlp.sales.crm-object.v1` and `vlp.dlq.s3-sink.v1`, all with 2 partitions |
| 125 KiB/s produce and consume | 2-month backfill via files; Kafka only receives stream mode (one hour at a time, a few hundred KB) |
| No Kafka Connect or managed integrations | Trial plan: managed Kafka Connect on Aiven with the Aiven S3 sink connector |
| Fixed retention | Irrelevant: the S3 landing is the durable record |
| One free service per organization; Schema Registry (Karapace) included | Register the 4 JSON Schemas with BACKWARD compatibility |

**Topics that stay off Kafka** (master data, tariffs, settlements, GL) arrive as file extracts, written by the generator with the same layout and envelope. This is realistic for ERP and MDM, and closes the open decision "GL via Kafka or file". If there is ever a paid plan, these topics only need to be switched on in `topics.yaml`.

**Steps**

1. Aiven account and Kafka service in a European region (done). SASL enabled, a `vlp-generator` service user, Schema Registry (Karapace) enabled, Kafka Connect enabled with "Set up Kafka Connect". Credentials and CA go to the secrets of the GitHub Environment `dev`, never to a local `.env`.
2. `veloparc-gen kafka-setup`, run by a GitHub Actions workflow, creates the 5 topics with 2 partitions (`message.timestamp.type=LogAppendTime`) and registers the schemas in Karapace.
3. `topics.yaml` with a `transport: kafka | file` flag per topic. The generator gets `--sink kafka` using `confluent-kafka`: same key, same 13 headers, stable `ce_id`. For Kafka topics, partitions and offsets now come from the broker.
4. Aiven S3 sink connector on the managed Kafka Connect, configured in `connect/s3_sink.json` with the `year=/month=/day=/hour=` layout, gzip, the envelope (HoistField, InsertField and headers) and the DLQ pointing to `vlp.dlq.s3-sink.v1`. It needs the S3 bucket and the connector's IAM credentials from Phase 4. Its default output (CSV, Base64 key and value, `<topic>-<partition>-<offset>` names) must be configured or the generator adapted (step 5).
5. Confirm that the files written by the connector are loaded by the same pipes. If the envelope differs from what the generator writes in the backfill, adapt the generator to the connector, never the other way round.

**Alternatives considered, not implemented:**

- Openflow Kafka connector (on the Snowflake side), reading from Aiven via Snowpipe Streaming. It removes the local Kafka Connect, but skips the S3 landing, loses the 25 metadata columns and, on the trial, Openflow is limited to about 10 credits per day without a card.
- Self-hosted Kafka Connect (Docker) consuming from Aiven: works on the free tier, but needs a machine running during the demo. Kept as the fallback if the trial ends.
- Local Kafka: full topology (9 topics, designed partitions) and no throughput limit, but no managed broker.
- Generator inside Snowflake (Snowpark stored procedure + internal stage): fallback plan if the AWS account is delayed.

The generator's `sink/` module keeps simulating the broker for the backfill and the file-based topics, and must reproduce, in line with the ingestion page:

1. **Topics.** `topics.yaml` with the 9 `vlp.<domain>.<dataset>.v1` topics, key, number of partitions and landing granularity (hour or day).
2. **Partitioner.** `partition = murmur2(key) % n_partitions`, like the default Java producer. The same key always lands on the same partition, preserving order per TrackID.
3. **Offsets.** Monotonic counter per topic and partition, kept in `state/offsets.json` between runs.
4. **Timestamps.** `_kafka_timestamp` = broker arrival time (LogAppendTime), a few seconds after the event. This time defines the folder, not the event time: late events (anomaly R5) land in the folder of the hour they arrived.
5. **Envelope.** One JSON line per message: `payload` (HoistField), `_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp` and `_kafka_headers` with the 13 mandatory headers (`ce_id`, `ce_time`, `vlp_op`, `vlp_batch_id`, `vlp_batch_count`, `traceparent`, ...).
6. **File rotation.** One file per topic, partition and hour (or per `flush.size`), named `<topic>+<partition>+<startOffset>.json.gz`.
7. **At-least-once.** With a small probability, rewrite a file with already-sent offsets and resend messages with the same `ce_id`. These are the technical duplicates bronze must remove.
8. **DLQ.** Messages that are not even valid JSON (e.g. truncated) go to `vlp.dlq.s3-sink.v1`, as the connector would with `errors.tolerance=all`.

## Phase 4 · S3 landing

> **Dropped (ADR 0003):** no AWS account. The landing layout below lives in the Snowflake internal stage `@BRONZE.LANDING`, and the generator outputs in `@BRONZE.GENERATOR`. Kept for reference.

One bucket per environment, with prefixes by function and the Hive layout (`year=/month=/day=/hour=`) defined on the ingestion page. All AWS infrastructure lives in Terraform, in `infra/terraform/aws`.

Bucket names are global on AWS: `vlp-dp-landing-prd` may already be taken. Add a short random suffix (e.g. `vlp-dp-landing-dev-7k3q`) and keep it in a variable.

```text
s3://vlp-dp-landing-dev-<suffix>/
├── landing/kafka/<topic>/year=YYYY/month=MM/day=dd/hour=HH/<topic>+<partition>+<startOffset>.json.gz
│       (monthly topics: .../day=dd/ without hour=)
├── landing/kafka/vlp.dlq.s3-sink.v1/...      # messages rejected by the sink
├── reference/<table>/<table>.csv             # FX_RATE_DAILY, POSTAL_ZONE, COUNTRY_ALIAS (optional; or dbt seeds)
└── _generator/runs/<run_id>/
        ├── run_manifest.json                 # seed, parameters, counts per topic
        └── anomaly_manifest.jsonl            # answer key of injected anomalies
```

1. **Bucket:** public access blocked, SSE-S3 encryption, versioning off (the landing is immutable by convention), tags `project=veloparc`.
2. **Lifecycle:** `landing/` moves to Glacier Instant Retrieval after 90 days; `_generator/` expires after 30 days.
3. **IAM role for Snowflake:** only `s3:GetObject`, `s3:GetObjectVersion` and `s3:ListBucket` on the `landing/` prefix, with a trust policy for the IAM user and the external ID Snowflake returns in `DESC INTEGRATION` (Phase 5). This step goes back and forth between Terraform and Snowflake: create the role with a temporary trust and update it later.
4. **Notifications:** `s3:ObjectCreated:*` event on the `landing/kafka/` prefix to the SQS queue Snowflake shows in `SHOW PIPES` (`notification_channel`). One notification serves all pipes on the bucket.
5. **Upload:** the `generate.yml` workflow (GitHub Actions, manual trigger with seed, window and countries) runs the backfill and `aws s3 sync out/landing s3://.../landing --size-only`, plus the manifests to `_generator/runs/<run_id>/`. In `stream` mode, Kafka Connect writes the Kafka topics; the generator sends the file-based topics with `boto3` as soon as each file is closed. Generator state (`offsets.json`, parcels in transit) lives in `s3://.../_generator/state/`, since runners are ephemeral.
6. **Writer credentials:** separate credentials for the generator and for Kafka Connect, only `s3:PutObject` on `landing/*` (and `_generator/*` for the generator).

Why store the anomaly manifest in S3: Phase 9 loads it into `DQ.ANOMALY_GROUND_TRUTH` and compares it with what the tests detected. It stays outside `landing/` so no pipe loads it as business data.

## Phase 5 · Snowflake foundation

> **Update (ADR 0003):** moved before the Snowflake sink connector (Phase 3b). Adds the internal stages `@BRONZE.LANDING` and `@BRONZE.GENERATOR` and a key-pair service user for the Aiven connector; no storage integration or S3 notification.

Open the trial in this phase and create everything through versioned SQL scripts in `infra/snowflake/`, applied with `schemachange`. Nothing is created by hand in Snowsight, so the repo can rebuild the account from scratch.

1. **Trial** Enterprise on AWS `eu-west-3` (Paris). First script: resource monitor with a monthly quota and suspension at 90%, applied to the account.
2. **Warehouses** XS, `AUTO_SUSPEND = 60`, `AUTO_RESUME = TRUE`: `WH_LOAD` (validation tasks), `WH_TRANSFORM` (dbt), `WH_ANALYTICS` (Streamlit and queries). Snowpipe uses serverless compute and doesn't need a warehouse.
3. **Database and schemas:** `VLP_DP` with `LANDING` (stages, file formats, pipes), `BRONZE`, `SILVER`, `GOLD`, `DQ`, `REF` and `UTIL` (UDFs). For development, `VLP_DP_DEV` as a zero-copy clone, or `DBT_<user>` schemas generated by dbt.
4. **Role-based access control**, with per-schema access roles (`*_R`, `*_RW`) granted to functional roles:

| Functional role | For | Access |
| --- | --- | --- |
| `VLP_LOADER` | pipes, bronze tasks | writes `LANDING`, `BRONZE`; uses `WH_LOAD` |
| `VLP_TRANSFORMER` | dbt (local, CI, dbt Projects) | reads `BRONZE`, `REF`; writes `SILVER`, `GOLD`, `DQ`; uses `WH_TRANSFORM` |
| `VLP_DQ_ANALYST` | data quality role | reads everything, including quarantine and test results |
| `VLP_ANALYST` | Sales & Finance consumers | reads `GOLD` and the `DQ` scorecard; PII masked |

5. **Service user** `SVC_VELOPARC_CI` (`TYPE = SERVICE`) with RSA key-pair authentication, no password. The private key lives in GitHub secrets.
6. **Storage integration** `SI_VLP_LANDING` for the Phase 4 IAM role. Run `DESC INTEGRATION`, copy `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` into Terraform and re-apply the trust policy.
7. **File format** `FF_JSONL_GZ` (`TYPE = JSON`, `COMPRESSION = GZIP`, `STRIP_OUTER_ARRAY = FALSE`) and **external stage** `STG_LANDING` pointing to `s3://.../landing/kafka/`.
8. **Smoke test:** `LIST @STG_LANDING` shows the backfill files and `SELECT $1 FROM @STG_LANDING/vlp.ops.tracking-event.v1/ (FILE_FORMAT => 'FF_JSONL_GZ') LIMIT 10` returns the envelope.
9. **Governance to show in the demo:** masking policy on consignee name and address (visible only to `VLP_DQ_ANALYST`), `PII` and `DATA_PRODUCT` tags on columns and tables, `QUERY_TAG` per layer.

## Phase 6 · Bronze

Bronze stays outside dbt: it is ingestion, not transformation. One `RAW_*` table per business topic (8; the DLQ
stays out for now), append-only, with the 25 metadata columns, the payload intact in `VARIANT` and `BK_*` keys.
It is built on the data already in Snowflake (no new ingestion, connector unchanged): the backfill files in
`@BRONZE.LANDING` and the connector tables `KAFKA_*`. Decisions in ADR 0004; columns and sources per path in
`ingestion_kafka_s3_bronze.md`.

1. **Inspect first** (`snowflake-inspect` workflow, read-only): connector tables, header types, landing envelopes,
   generator stage, UDF packages. Done on 2026-11-24; its findings are in ADR 0004.
2. **One column spec** (Python) generates the DDL of the 8 tables and the SELECT of both loaders, so the 25 columns
   are identical everywhere; the generated SQL is committed and CI fails if it is stale. No clustering keys yet:
   the tables are small (largest: tracking events, about 140 MB of gzip JSON) and automatic clustering is billed
   as serverless compute; add `CLUSTER BY (TO_DATE(_KAFKA_TS))` on shipments and tracking events if volumes grow.
   `bronze-preview` (read-only) runs both SELECTs on real data before the migration is applied.
3. **Migrations** (schemachange): `RAW_*` (V1.4.0, generated), `LOAD_AUDIT` (V1.5.0), `REF` and `DQ` answer-key
   tables (V1.6.0); repeatable, generated `R__` scripts for the connector streams and tasks and for the views.
4. **File loader:** `veloparc-gen bronze-load` (workflow `bronze-load`) runs one `COPY INTO` per topic with
   transformation (`METADATA$FILENAME`, `FILE_ROW_NUMBER`, `FILE_CONTENT_KEY`, `FILE_LAST_MODIFIED`,
   `START_SCAN_TIME`), `ON_ERROR = CONTINUE`, results in `LOAD_AUDIT`. COPY load metadata skips files already
   loaded. No pipe: there is no auto-ingest on internal stages. Done on 2026-11-24: 5,225 files, 0 errors.
5. **Connector loader:** change tracking on `KAFKA_*`, an append-only stream per table and a triggered task
   (`WHEN SYSTEM$STREAM_HAS_DATA`, `WH_LOAD`, UTC) that maps `RECORD_METADATA` to the same columns (file columns
   NULL). Done on 2026-11-24: 558 shipments, 815 tracking events, 8 CRM objects.
6. **Views dbt reads as sources:** `V_RAW_*` with `_IS_TECH_DUP` (topic, partition, offset, `ce_id`) and
   `_IS_RESEND` (topic, `ce_id` first seen elsewhere), filtered by the `stg_` models; `V_GL_BATCH_COMPLETENESS`.
7. **Reference and answer key**, in the same `bronze-load` run: the generator's reference CSVs into
   `REF.FX_RATE_DAILY`, `REF.POSTAL_ZONE`, `REF.COUNTRY_ALIAS` (replaced); `run_manifest.json`,
   `anomaly_manifest.jsonl` and `rule_violations.jsonl` into `DQ.GENERATOR_RUNS`, `DQ.ANOMALY_GROUND_TRUTH` and
   `DQ.GENERATOR_RULE_VIOLATIONS` (appended, keyed by `run_id`).
8. **Schema validation moves to dbt** (ADR 0004, decision 6): no UDF, quarantine or validation task in bronze.
   GS1 check-digit and SSCC-from-URN logic become dbt macros in Phase 7/9.
9. **Phase check**, done as dbt tests on the sources in Phase 7 (counts per topic vs `DQ.GENERATOR_RUNS`, GL batches
   complete) instead of a separate workflow. Known gaps: the file-based topics of 2026-11-01 and the manifests of
   that publish are not on the stage.

## Phase 7 · dbt and silver

dbt starts at the bronze views and implements the source → silver mapping column by column. Each source goes through a `stg_` model (parsing, dedup, quarantine exclusion) and one or more incremental `silver` models.

**Setup**

1. `dbt-core` + `dbt-snowflake` in `pyproject.toml`, pinned versions. `profiles.yml` with `env_var()` only and private-key authentication; targets `dev` (schema `DBT_DANIEL`), `ci` (schema per PR) and `prd`.
2. Packages: `dbt_utils`, `dbt_expectations` (domain, range and regex tests) and `elementary` (test result history and volume and freshness anomalies).
3. `sources.yml` with the bronze views, `loaded_at_field: _LOAD_TS` and `freshness` (warn at 2 h, error at 6 h) for hourly topics.
4. Sources for `REF.COUNTRY_ALIAS`, `REF.POSTAL_ZONE`, `REF.FX_RATE_DAILY` (generated in Phase 2, loaded in Phase 6); seed only for `unknown_members`.
5. Conventions: `stg_<source>` as views, `silver` as `incremental` with `merge` and `unique_key`, `gold` as table or incremental. Macros for repeated patterns: `parse_ts_utc()`, `latest_version()` (QUALIFY per key), `not_quarantined()`.

**Build order for silver models** (the same as the generator, so each step can test references to the previous ones):

| # | Silver models | Mapping detail to watch |
| --- | --- | --- |
| 1 | `customer`, `depot`, `partner`, `partner_zone` | Customer keeps every version; depot and partner only the latest (ordered by `_SOURCE_TS`, `_KAFKA_OFFSET`); zones replaced on every version |
| 2 | `tariff`, `tariff_weight_band` | Compacted topic: latest version per `tariffId`; bands replaced |
| 3 | `crm_opportunity`, `crm_lead` | Routed by `attributes.type`; Salesforce dates without a colon in the offset; country normalized with `country_alias`, `country_raw` preserved |
| 4 | `veloship_shipment`, `veloship_parcel` | Two FLATTENs zipped by index with a FULL OUTER JOIN, so R1 violations show up as rows with nulls |
| 5 | `tracking_event` | UTC + original offset; `GS1_SSCC_FROM_URN`; business duplicates (R4) kept and flagged with `is_duplicate` |
| 6 | `invoice`, `invoice_line`, `invoice_surcharge` | Nested FLATTEN; add `related_invoice_number` (gap #1) |
| 7 | `partner_settlement` | Key on `settlementId` |
| 8 | `gl_balance` | Only the latest complete batch per (company_code, period) |

In each model, document in YAML the origin of every column (copied from the mapping) and the basic tests: `unique` and `not_null` on PKs, `accepted_values` on enums, `relationships` for FKs. Business-rule tests come in Phase 9.

Add `_loaded_at` to every silver table (decision #12 of the mapping): it is the watermark for incremental models and for gold.

## Phase 8 · Gold

Gold follows the silver → gold mapping: 5 dimensions and 7 facts. Build the dimensions and unknown members first, then the transaction facts and, last, the accumulating ones that depend on the others.

1. **Unknown members** from the `unknown_members` seed: `customer_sk = -1`, `partner_zone_sk = -1`, `tariff_sk = -1`, `depot_gln = '0000000000000'`, `date_key = -1`. They enter each dimension via `UNION ALL`.
2. **`dim_date`** with `dbt_utils.date_spine` from the minimum to the maximum business date + 2 years; `period` and `is_weekend` columns.
3. **`dim_customer` SCD2** derived from the versions already kept in silver: `valid_from = updated_at`, `valid_to = LEAD(updated_at)`, `is_current`. No `dbt snapshot` needed, because history comes from the source.
4. **`dim_depot`, `dim_partner_zone`, `dim_tariff`** as SCD1, hash surrogate key. Uniqueness test on `zone_code` in `dim_partner_zone` (R16).
5. **Macro `scd2_lookup(dim, natural_key, business_date)`** with the rule `valid_from <= date < COALESCE(valid_to, '9999-12-31')` and `COALESCE(..., -1)`. Used by `fct_parcel` (shipping date), `fct_invoice_line` (issue date) and `dim_tariff` (validity start).
6. **Transaction facts**, incremental on `_loaded_at`: `fct_parcel_event`, `fct_invoice_line` (with `amount_eur` and `fuel_surcharge_eur`).
7. **Periodic snapshots:** `fct_partner_cost` (EUR using `fx_rate_daily` monthly average) and `fct_gl_revenue` with `delete+insert` per (company_code, period).
8. **Accumulating facts:** `fct_parcel` with `pickup_ts`, `delivered_ts`, `is_delivered` and `is_inferred` (orphan TrackIDs, decision #5); `fct_opportunity` and `fct_lead` with the latest state.
9. **Data products on top of gold**, as small views consumers use: revenue by country and month, delivery lead time by zone, partner cost per delivered parcel, lead → opportunity conversion. Each with `contract: enforced: true` in YAML, which is the promise made to Sales & Finance.

Phase check: with anomalies off, revenue in `fct_invoice_line` by country and month matches `fct_gl_revenue` within ±0.5% (R13). With anomalies on, the country with positive credit notes must come out 2 to 3% off.

## Phase 9 · Data quality

This is the core of the project: rules and controls, automated tests, scorecards and root-cause analysis. Each rule R1 to R16 becomes a dbt test with an owner, a quality dimension and a severity, and the result of every run is stored to measure trends.

1. **Rule catalogue** in `dbt/veloparc/seeds/dq_rule_catalog.csv`: `rule_id`, description, sources, dimension (Validity, Completeness, Uniqueness, Consistency, Timeliness, Accuracy), severity (`warn`/`error`), data owner (e.g. "Finance PT") and acceptable threshold.
2. **Custom generic tests** in `tests/generic/`, reusable and parameterized, with `meta: {rule_id, dimension, owner}` on every use:
    - `gs1_check_digit` (R15), `vat_prefix_matches_country` (R15)
    - `event_sequence_valid` (R3), `no_repeated_scan` (R4), `record_after_event` (R5)
    - `amounts_reconcile` (R7, R12), `price_matches_tariff_band` (R8), `credit_note_negative` (R9)
    - `contiguous_bands_no_overlap` (R14), `settlement_matches_events` (R11)
    - `revenue_reconciles_with_gl` (R13, ±0.5% tolerance), `country_has_events_every_day` (Timeliness)
3. **Store failures:** `store_failures: true` with `+schema: dq_failures`, so each test has a table of the failing rows with the business key. `elementary` keeps the run history and adds volume and freshness tests per topic.
4. **`dq_issue` model** (one record per business key × violated rule), combining the failure tables, the bronze quarantine and the violations flagged in silver (`is_duplicate`, R1 with nulls). It is the base for everything that follows.
5. **Scorecard** `dq_scorecard_daily`: per data product × country × dimension × day, `% valid records = 1 − records with issues / records evaluated`, compared with the catalogue threshold. A `dq_scorecard_current` view with a traffic light.
6. **Root cause:** every `dq_issue` carries `_FILE_NAME`, `_KAFKA_OFFSET`, `_SOURCE_SYSTEM` and `_SOURCE_TXN_ID` from bronze. A view aggregates issues by source system, batch and arrival hour, to show for example that all positive credit notes come from a single country and a single billing batch.
7. **Answer key:** load `anomaly_manifest.jsonl` into `DQ.ANOMALY_GROUND_TRUTH` (seed or `COPY`) and build `dq_detection_eval` with, per rule, anomalies injected, detected, missed and false positives, and from these precision and recall. A rule with recall below 100% is a finding to document, not a problem to hide.
8. **Visible scorecard:** Streamlit in Snowflake app in `apps/streamlit/` with three pages: scorecard by data product and country, daily trend by dimension, rule detail with the records and the probable cause.
9. **Alerts:** `error` tests stop the pipeline before gold; `warn` tests only feed the scorecard. Optional: a Snowflake Alert that sends an email when a data product drops below its threshold.

## Phase 10 · Orchestration, streaming and CI/CD

Orchestration stays inside Snowflake: Snowpipe loads bronze, one task validates and another runs dbt as a native project. GitHub Actions only tests and deploys the code.

**Orchestration**

1. Deploy the dbt project as a `DBT PROJECT` object with `snow dbt deploy` (dbt Projects on Snowflake, GA since 6 Nov 2025). Fallback if you hit limitations: run `dbt build` in GitHub Actions.
2. Task graph: `TASK_VALIDATE_BRONZE` (every 5 min, when the streams have data) → `TASK_DBT_BUILD` (`EXECUTE DBT PROJECT ... ARGS = 'build --select silver+ gold+ dq+'`, hourly).
3. **Stream mode for the demo:** `stream.yml` workflow in GitHub Actions (cron every 15 minutes, switched on only on demo days) runs `make stream`. The generator publishes the next hour to the Aiven topics, the managed Kafka Connect writes to S3, Snowpipe loads and the next task refreshes silver, gold and the scorecard. Nothing runs on the maintainer's computer.
4. Reprocessing: `make backfill FROM=... TO=...` rewrites a window and dbt runs with `--full-refresh` on the affected models.

**CI/CD (GitHub Actions)**

| Workflow | When | What it does |
| --- | --- | --- |
| `ci.yml` | every PR | `ruff`, `sqlfluff lint`, generator `pytest`, validation of the examples against the schemas, `dbt build --select state:modified+` in a `CI_PR_<n>` schema with sample data, PR comment with the test summary |
| `deploy.yml` | merge to `main` | `schemachange deploy` of the Snowflake scripts, `terraform apply` (with manual approval), `snow dbt deploy`, publishing `dbt docs` to GitHub Pages |
| `generate.yml` | manual (`workflow_dispatch`) | backfill with the given seed, window and countries; uploads the landing and the manifests to S3 |
| `kafka-setup.yml` | manual | creates the topics and registers the schemas in Karapace |
| `stream.yml` | cron, off by default | generates and sends the next hour |
| `cleanup.yml` | PR closed | `DROP SCHEMA CI_PR_<n>` |

GitHub secrets, in the Environment `dev` (only for workflows on `main` or manual triggers, never for PRs from forks): the `SVC_VELOPARC_CI` private key, the Snowflake account, the Aiven credentials and CA, and an AWS role assumed via OIDC (no stored access keys). For cheap CI, use `dbt build --empty` or a 1-day sample instead of the full backfill.

## Phase 11 · Documentation and demo

The README is what a reviewer opens first: within 30 seconds it must show what the project proves and the data quality result, before any installation instructions.

**README, in this order**

1. One sentence: fictitious European parcel carrier, 8 sources, Kafka → S3 → Snowflake → dbt, focused on data quality for the Sales & Finance data products.
2. Architecture diagram (Mermaid in the README itself).
3. Result: precision and recall table per rule and a screenshot of the scorecard.
4. Design decisions linking to the ADRs (bronze with 25 metadata columns, at-least-once and dedup by offset, SCD2 from the source, anomaly answer key).
5. How to run: prerequisites, `.env`, `make` per phase, estimated cost.
6. Limitations and next steps (scaling to 30 million records per day, data contracts at the producer).

Also publish `dbt docs` on GitHub Pages, with lineage from bronze to gold.

**Demo script (10 minutes)**

1. Architecture and why (1 min): diagram, 8 sources, where each control sits.
2. Data coming in (2 min): `stream.yml` triggered by hand or the cron switched on; message passing through Aiven, file appearing in S3, the pipe's `COPY_HISTORY`.
3. Bronze (1 min): one row with its metadata, the quarantine with a 12-digit GLN and the schema error.
4. One rule end to end (3 min): positive credit notes → R9 test fails → R13 out of tolerance in one country → root cause in the billing batch → how you would talk to that country's data owner.
5. Scorecard (2 min): data product × country × dimension, trend, precision and recall against the answer key.
6. Engineering (1 min): PR with green CI, `state:modified+`, deploy via `snow dbt deploy`, account cost.

**Before making the repo public:** `git log` and `git grep` with no references to real companies, no credentials (`detect-secrets scan`), no Snowflake or AWS account IDs in screenshots.

## Minimum path and schedule

The full plan is about 17 days of focused work; a minimum path that already proves the core fits in about 9. Estimates assume full days of work.

| Phase | Effort (days) | Minimum path |
| --- | --- | --- |
| 0 · Accounts and decisions | 0.5 | Same |
| 1 · Repository | 0.5 | Same |
| 2 · Generator | 3 | Backfill mode only; anomalies for the sources used below |
| 3 · Kafka on Aiven + files | 1 + 1 | Backfill via files + Aiven with tracking events only |
| 4 · S3 | 0 | Dropped (ADR 0003): internal stage instead |
| 5 · Snowflake foundation | 0.5 | Same (RBAC simplified to two roles) |
| 6 · Bronze | 1.5 | Pipes yes; JSON validation in dbt instead of stream + task |
| 7 · dbt and silver | 2 | 4 sources: master data, shipments, events, invoices (+ tariffs for R8) |
| 8 · Gold | 1.5 | `dim_customer`, `dim_depot`, `fct_parcel`, `fct_invoice_line` |
| 9 · Data quality | 2.5 | R1 to R9, R13 and R15, `dq_issue`, scorecard and answer key; no Streamlit |
| 10 · Orchestration and CI/CD | 1.5 | Only `ci.yml` with lint and pytest |
| 11 · Documentation and demo | 1 | README and demo script |

Order to avoid getting blocked: the Kafka producer and topic setup (3a) don't need Snowflake; the connector (3c) and the backfill upload (3d) do, so the Snowflake trial opens at 3b; Phase 9 depends on 7 and doesn't need 8 complete, so gold can be cut first if time gets tight. The other Aiven topics and Streamlit are the best extras to add after the minimum path.
