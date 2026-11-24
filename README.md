# veloparc-snowflake-dq

Data quality platform for the Sales & Finance data products of **Veloparc**, a fictitious European parcel carrier.
A Python generator plays 8 source systems. Their data flows through Kafka (Aiven) and Snowflake stages into a bronze
layer, and dbt, running inside Snowflake, builds silver, a gold star schema and data quality models. Every DQ rule is
then measured against an **answer key** of anomalies the generator injected on purpose (precision and recall per rule).

> All company names and data in this repository are fictitious.

## Architecture

```mermaid
flowchart LR
  G[Python generator] -->|shipments, scans, invoices, CRM| K[(Kafka · Aiven)]
  K -->|Snowflake sink connector · Snowpipe Streaming| KT[KAFKA_* tables]
  G -->|backfill + file sources · GitHub Actions| ST[(Internal stage @BRONZE.LANDING)]
  G -->|answer key + reference CSVs| GS[(@BRONZE.GENERATOR)]
  KT -->|stream + triggered task| B[Bronze · 8 RAW_* tables · 25 metadata columns]
  ST -->|COPY INTO| B
  GS -->|COPY INTO| REF[REF · DQ.ANOMALY_GROUND_TRUTH]
  B -->|dbt stg_ · dedup| SV[Silver · incremental]
  SV -->|dbt| GD[Gold · star schema]
  SV --> DQ[DQ · one model per rule R1–R16]
  REF --> DQ
  DQ --> SC[dq_issue · dq_scorecard_daily · dq_detection_eval]
```

- **No AWS account and nothing on a laptop.** Everything that touches a cloud service runs in GitHub Actions, with
  credentials in the GitHub Environment `dev` ([ADR 0002](docs/adr/0002-managed-kafka-connect-and-ci-execution.md),
  [ADR 0003](docs/adr/0003-snowflake-internal-stage-and-aiven-snowflake-sink.md)).
- **Bronze keeps every row.** Each row carries 25 metadata columns (Kafka topic, partition, offset, headers, source
  file, load times). Duplicates are flagged in `V_RAW_*` views and removed at the entry to silver. JSON Schema rules
  are checked in dbt, not at load ([ADR 0004](docs/adr/0004-bronze-loaders-dedup-and-validation.md)).
- **dbt runs in Snowflake** as the project object `VLP_DP.DBT.VELOPARC` (dbt Projects on Snowflake, dbt Core 1.12),
  deployed and executed by the `dbt` workflow. It uses no Hub packages: surrogate keys, GS1 check digits and generic
  tests are project macros.

## Data quality, measured

Each cross-source rule in [`contracts/README.md`](contracts/README.md) (R1–R16, plus contract checks) is a dbt model
`DQ.dq_rule_*`. It returns one row per violation, with the bronze lineage of the offending record (file, offset,
event id) for root cause, and a `no_violations` test with `meta: {rule_id, dimension, owner}`. Rules warn, they do
not stop the pipeline: the generator injects anomalies on purpose.

| Model | What it holds |
| --- | --- |
| `dq_rule_catalog` (seed) | Rule, description, sources, DQ dimension, owner, acceptable threshold |
| `dq_issue` | Every violation of every rule, with dimension, owner and bronze lineage |
| `dq_scorecard_daily` | Data product × source × country × dimension × day: `pct_valid` against the threshold, green/amber/red |
| `dq_detection_eval` | Per rule: detections vs injected anomalies and vs the generator's own checker, with precision and recall |

The first evaluation found a design lesson worth reading: once schema-invalid records reach silver flagged instead of
quarantined, one malformed GLN was counted by three rules. See
[`docs/data-quality/detection_evaluation.md`](docs/data-quality/detection_evaluation.md) for the results per rule and
the findings.

## Gold data model (star schema)

Seven facts share five conformed dimensions. `dim_customer` is SCD2 (facts point to the customer version valid at the business date); the others are SCD1. Every lookup that fails points to an unknown member (`-1`, or GLN `0000000000000`), so no fact row is dropped.

```mermaid
erDiagram
  dim_date ||--o{ fct_parcel : "shipping_date_key"
  dim_date ||--o{ fct_parcel_event : "event_date_key"
  dim_date ||--o{ fct_invoice_line : "invoice_date_key"
  dim_date ||--o{ fct_opportunity : "close_date_key"
  dim_date ||--o{ fct_lead : "created_date_key"
  dim_customer ||--o{ fct_parcel : "customer_sk"
  dim_customer ||--o{ fct_invoice_line : "customer_sk"
  dim_customer ||--o{ fct_opportunity : "customer_sk"
  dim_customer ||--o{ dim_tariff : "customer_sk"
  dim_tariff ||--o{ fct_invoice_line : "tariff_sk"
  dim_depot ||--o{ fct_parcel_event : "depot_gln"
  dim_depot ||--o{ dim_partner_zone : "base_depot_gln"
  dim_partner_zone ||--o{ fct_parcel : "partner_zone_sk"
  dim_partner_zone ||--o{ fct_partner_cost : "partner_zone_sk"
  fct_parcel ||--o{ fct_parcel_event : "track_id"
  fct_parcel ||--o{ fct_invoice_line : "track_id"
  fct_opportunity |o--o| fct_lead : "converted_opportunity_id"

  dim_date {
    int date_key PK "YYYYMMDD"
    date full_date
    varchar period "YYYY-MM"
    int iso_week
    boolean is_weekend
  }
  dim_customer {
    int customer_sk PK "SCD2"
    varchar contact_id "natural key"
    varchar segment
    char country_code
    timestamp valid_from
    timestamp valid_to
    boolean is_current
  }
  dim_depot {
    char depot_gln PK "GS1 GLN"
    varchar depot_type "HUB DEPOT PARCELSHOP LOCKER"
    char country_code
    varchar city
  }
  dim_partner_zone {
    int partner_zone_sk PK
    varchar partner_id
    varchar zone_code "one partner per zone"
    char base_depot_gln FK
  }
  dim_tariff {
    int tariff_sk PK "tariff x weight band"
    varchar tariff_id
    int customer_sk FK
    decimal weight_from_kg
    decimal weight_to_kg
    decimal price
    decimal fuel_surcharge_pct
  }
  fct_parcel {
    varchar track_id PK "accumulating snapshot"
    int customer_sk FK
    int partner_zone_sk FK
    int shipping_date_key FK
    decimal weight_kg
    timestamp pickup_ts
    timestamp delivered_ts
    boolean is_delivered
  }
  fct_parcel_event {
    varchar event_id PK "one row per scan"
    varchar track_id FK
    char depot_gln FK
    int event_date_key FK
    varchar status
    int capture_delay_min
  }
  fct_invoice_line {
    varchar invoice_number PK
    int line_no PK
    int customer_sk FK
    int tariff_sk FK
    varchar track_id FK
    int invoice_date_key FK
    varchar doc_type
    decimal amount_eur
    decimal fuel_surcharge_eur
  }
  fct_partner_cost {
    varchar settlement_id PK "partner x zone x month"
    int partner_zone_sk FK
    varchar period
    int stops
    int delivered_parcels
    decimal total_cost_eur
  }
  fct_gl_revenue {
    varchar company_code PK
    varchar period PK
    varchar gl_account PK
    char country_code
    decimal amount_eur
  }
  fct_opportunity {
    varchar opportunity_id PK
    int customer_sk FK
    int close_date_key FK
    varchar stage
    decimal amount_eur
  }
  fct_lead {
    varchar lead_id PK
    int created_date_key FK
    varchar converted_opportunity_id FK
    varchar lead_source
    varchar status
  }
```

| Fact | Grain | Type | What it answers |
| --- | --- | --- | --- |
| `fct_parcel` | one row per parcel | accumulating snapshot | Lead time pickup → delivery, delivery rate by zone and customer |
| `fct_parcel_event` | one row per scan | transaction | Network flow per depot, capture delays (timeliness) |
| `fct_invoice_line` | one row per invoice line | transaction | Revenue in EUR by customer, product and month, fuel surcharge |
| `fct_partner_cost` | partner × zone × month | periodic snapshot | Last-mile cost per delivered parcel |
| `fct_gl_revenue` | company × month × GL account | periodic snapshot | Ledger revenue, reconciled with invoices (R13, ±0.5%) |
| `fct_opportunity` | one row per opportunity | accumulating snapshot | Pipeline and won volume by country |
| `fct_lead` | one row per lead | accumulating snapshot | Lead → opportunity conversion by source |

`fct_gl_revenue` and `fct_partner_cost` join `dim_date` on `period` (month grain). The full model is in [`docs/data-model/gold_star_schema.dbml`](docs/data-model/gold_star_schema.dbml) and the column-level lineage in [`docs/data-model/silver_to_gold_mapping.md`](docs/data-model/silver_to_gold_mapping.md).

## Repository layout

| Path | Content |
| --- | --- |
| `contracts/` | JSON Schemas of the 8 sources, coherent example payloads and their validator, cross-source rules R1–R16 |
| `generator/` | Synthetic data generator: coherent world, anomalies with an answer key, publish to Kafka and upload to the stages ([README](generator/README.md)) |
| `connect/` | Aiven Snowflake sink connector template |
| `infra/snowflake/` | One-time bootstrap SQL and schemachange migrations (stages, bronze tables, loaders, views, roles) |
| `dbt/veloparc/` | dbt project: `staging` → `silver` → `gold` → `dq`, with column-level documentation |
| `docs/architecture/` | Ingestion design and the phase-by-phase implementation plan |
| `docs/data-model/` | Silver and gold models (DBML) and column-level mappings |
| `docs/data-quality/` | Detection evaluation results and findings |
| `docs/adr/` | Architecture decision records |
| `.github/workflows/` | CI, merge on approval and the manual workflows below |

## How it runs

Everything runs in GitHub Actions. The manual workflows, in the order a fresh environment needs them:

| Workflow | What it does |
| --- | --- |
| `snowflake-deploy` | Applies the Snowflake migrations with schemachange (schemas, stages, bronze tables, loader tasks, views) |
| `kafka-setup` | Creates the Kafka topics, the producer ACL and the JSON Schemas on Aiven |
| `connector-deploy` | Rotates the connector's key pair and creates or updates the Aiven Snowflake sink connector |
| `generate` | Generates the 2-month backfill and uploads it, the reference CSVs and the answer key to the stages |
| `publish` | Generates a window and publishes it: Kafka topics to Aiven, file topics to the stage |
| `bronze-load` | `COPY INTO` the `RAW_*`, `REF` and generator manifest tables from the stages |
| `dbt` | Deploys the dbt project to Snowflake and runs a dbt command there (`build`, `show --select ...`) |
| `snowflake-inspect`, `bronze-preview`, `kafka-check` | Read-only checks |

Local development needs only [uv](https://docs.astral.sh/uv/) (it installs Python 3.12 and the pinned dependencies)
and no credentials:

```bash
make setup                 # uv sync + git hooks
make lint test validate-contracts   # ruff, pytest, contract examples, dbt parse
make generate              # 2-month backfill into out/ (see generator/README.md)
```

Every PR runs the same checks in CI. Approving a PR (or labelling it `ready-to-merge`) re-verifies it against the
latest `main` and merges it: see [ADR 0001](docs/adr/0001-ci-cd-pipeline.md).

## Limitations and next steps

- **Orchestration.** dbt runs on demand through the `dbt` workflow. The next step is a Snowflake task running
  `EXECUTE DBT PROJECT` after the bronze loaders.
- **Known data gaps.** The file-based topics of the 2026-11-01 publish are not on the stage, so the answer key covers
  the backfill only. The open modelling gaps are listed in both mapping documents.
- **Reporting.** A Power BI report on the gold star schema and the DQ models is planned.

## Licence

MIT
