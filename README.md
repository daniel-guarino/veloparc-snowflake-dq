# veloparc-snowflake-dq

Data quality platform for the Sales & Finance data products of **Veloparc**, a fictitious European parcel carrier: 8 sources, Kafka → S3 → Snowflake → dbt, with every data quality rule measured against an answer key of injected anomalies.

> Work in progress. All company names and data in this repository are fictitious.

## Architecture

```mermaid
flowchart LR
  G[Python generator] -->|continuous sources| K[(Kafka · Aiven)]
  G -->|backfill + batch sources| S3
  K --> C[Kafka Connect S3 sink]
  C --> S3[(S3 landing)]
  S3 -->|Snowpipe| B[Bronze · RAW_* + 25 metadata cols]
  B --> Q[DQ_QUARANTINE]
  B -->|dbt| SV[Silver]
  SV -->|dbt| GD[Gold · star schema]
  SV --> DQ[DQ tests · R1–R16]
  GD --> DQ
  DQ --> SC[Scorecard · precision/recall vs answer key]
```

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

## What is here today

| Path | Content |
| --- | --- |
| `contracts/schemas/` | JSON Schemas for the 8 sources |
| `contracts/examples/` | 12 valid example payloads that describe one coherent story (customer → shipment → scan → invoice → credit note) |
| `contracts/validate_examples.py` | Validates the examples against the schemas and the cross-source rules R1, R2, R6, R7, R8, R9, R12, R14, R15 |
| `contracts/README.md` | Generation order, cross-source rules R1–R16, anomalies to inject |
| `docs/architecture/` | Ingestion design (Kafka → S3 → bronze) and the phase-by-phase implementation plan |
| `docs/data-model/` | Silver and gold models (DBML) and column-level mappings |
| `generator/` | Synthetic data generator: coherent world, anomalies with an answer key, landing files in the S3 Sink format ([README](generator/README.md)) |
| `docs/adr/` | Architecture decision records |
| `Makefile`, `pyproject.toml`, `.github/workflows/` | Tooling (uv, ruff, pytest, pre-commit) and CI/CD |

## Development

Requires [uv](https://docs.astral.sh/uv/) (it installs Python 3.12 and the pinned dependencies).

```bash
make setup                 # uv sync + git hooks
make lint test validate-contracts
make generate              # 2-month backfill into out/ (see generator/README.md)
```

Every PR runs the same checks in CI. Approving a PR (or labelling it `ready-to-merge`) re-verifies it against the latest `main`, merges it and starts the deploy pipeline: see [ADR 0001](docs/adr/0001-ci-cd-pipeline.md).

Next: Kafka on Aiven (`connect/`), infrastructure (`infra/`) and the dbt project (`dbt/`). See [`docs/architecture/implementation_plan.md`](docs/architecture/implementation_plan.md).

## Licence

MIT
