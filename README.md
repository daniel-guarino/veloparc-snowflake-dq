# veloparc-snowflake-dq

Data quality platform for the Sales & Finance data products of **Veloparc**, a fictitious European parcel carrier: 8 sources, Kafka → S3 → Snowflake → dbt, with every data quality rule measured against an answer key of injected anomalies.

> Work in progress. All company names and data in this repository are fictitious.

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

## What is here today

| Path | Content |
| --- | --- |
| `contracts/` | JSON Schemas for the 8 sources, cross-source rules R1–R16, anomalies to inject |
| `docs/architecture/` | Ingestion design (Kafka → S3 → bronze) and the phase-by-phase implementation plan |
| `docs/data-model/` | Silver and gold models (DBML) and column-level mappings |

Next: the data generator (`generator/`), infrastructure (`infra/`) and the dbt project (`dbt/`). See [`docs/architecture/implementation_plan.md`](docs/architecture/implementation_plan.md).

## Licence

MIT
