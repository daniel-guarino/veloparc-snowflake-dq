# ADR 0004 · Bronze: two loaders over the existing data, dedup flags, validation in Snowflake

- Status: accepted
- Date: 2026-11-24
- Refines: ADR 0003 (decisions 1, 2 and 5) and the implementation plan, Phase 6

## Context

Phase 6 builds bronze on what is already in Snowflake (`snowflake-inspect`, run of 2026-11-24):

- `@BRONZE.LANDING`: the backfill (2026-09-01 → 2026-10-31, with late arrivals up to 2026-12-03 in the landing
  folders), 5,494 files in 9 topics. Every line is an envelope `payload` + `_kafka_*` + `_kafka_headers` (13 headers,
  all strings).
- `KAFKA_VELOSHIP_SHIPMENTS` (558 rows), `KAFKA_TRACKING_EVENTS` (815), `KAFKA_CRM_OBJECTS` (8) and
  `KAFKA_INVOICES` (0), written by the connector (Snowpipe Streaming), owned by `VLP_CONNECTOR`, change tracking off.
- `@BRONZE.GENERATOR`: one backfill run (`run_manifest.json`, `anomaly_manifest.jsonl`, `rule_violations.jsonl`),
  the 3 reference tables as CSV, `state/offsets.json`.
- Python UDF packages: `jsonschema` 4.26.0 (the version pinned in `pyproject.toml`) and `rfc3339-validator` exist for
  runtime 3.12.

There will be **no new ingestion of files and no change to the connector**: bronze maps the data as it is.

What the connector path does differently from the files (seen in the inspection):

| Header | Files | `RECORD_METADATA:headers` |
| --- | --- | --- |
| `ce_time` | ISO-8601 string (`2026-11-01T06:00:44.000Z`) | epoch milliseconds as a string (`"1793512844000"`): the default Kafka Connect header converter parses the ISO value as a timestamp |
| `ce_specversion` | `"1.0"` | integer `1` |
| `vlp_batch_seq`, `vlp_batch_count` | strings | integers |
| others | strings | strings |

`RECORD_METADATA:CreateTime` is the time the publish workflow sent the message (the Developer plan has no
LogAppendTime), not the simulated broker time that `_kafka_timestamp` carries in the files.
`RECORD_METADATA:key` is present; the file envelope has no key.

## Decision

1. **Eight `RAW_*` tables** (one per business topic), append-only, created by schemachange: the **25 metadata
   columns**, `RAW_PAYLOAD VARIANT` and `BK_*` business keys. The column list, types and the expression for each
   path are defined once (a Python spec that generates the DDL and both SELECTs), so both loaders produce the same
   columns. The 25 columns and their sources are in `docs/architecture/ingestion_kafka_s3_bronze.md`. Compared with
   the earlier list, `_EVENT_TYPE` (`ce_type`) and `_KAFKA_KEY` (`RECORD_METADATA:key`, NULL for file rows) are
   added, and `_SOURCE_USER` is `vlp_producer`.
2. **Header values are normalized in the bronze columns, not in `_KAFKA_HEADERS`.** `_SOURCE_TXN_SEQ` and
   `_SOURCE_TXN_COUNT` are cast to numbers on both paths; a `ce_time` is read as epoch milliseconds when it is all
   digits and as ISO-8601 otherwise. `_KAFKA_HEADERS` keeps the object exactly as each path delivered it.
3. **Files are loaded by `COPY INTO` run from a workflow** (`bronze-load`), with `METADATA$FILENAME`,
   `FILE_ROW_NUMBER`, `FILE_CONTENT_KEY`, `FILE_LAST_MODIFIED` and `START_SCAN_TIME`. COPY's load metadata makes a
   re-run skip files already loaded. No pipe: Snowpipe auto-ingest does not exist for internal stages, the REST
   API needs key-pair authentication, and directory-table auto refresh would mean recreating `@LANDING`.
4. **Connector tables are loaded by a stream + triggered task** per table (`WHEN SYSTEM$STREAM_HAS_DATA`, warehouse
   `WH_LOAD`) into the same `RAW_*` table; the first run picks up the rows already there. The migration enables
   change tracking on the `KAFKA_*` tables (`VLP_DEPLOYER` inherits `VLP_CONNECTOR`, their owner).
5. **Deduplication is flagged in bronze and applied at the entry to silver.** Bronze keeps every row. A view
   `V_RAW_<topic>` per table adds `_IS_TECH_DUP` (same topic, partition, offset and `ce_id`: the same message
   loaded twice) and `_IS_RESEND` (same topic and `ce_id` at another offset: a producer retry); the first row by
   `_INSERTED_AT` is kept. `ce_id` is part of the technical key because backfill and broker offsets overlap. The dbt
   `stg_` models filter on both flags, so silver has no technical duplicates; business duplicates (R4) are kept and
   flagged in silver.
6. **Schema validation runs in Snowflake**: a Python UDF `BRONZE.VALIDATE_JSON(schema_id, payload)` with
   `jsonschema` 4.26.0 (runtime 3.12) and the 8 schemas of `contracts/schemas/` embedded at deploy time, applied by a
   triggered task on an append-only stream per `RAW_*` table; invalid rows go to `BRONZE.DQ_QUARANTINE`. The
   handler is a Python file in the repository, tested with pytest against the same examples and anomalies as the
   generator.
7. **GL batch completeness** is a view: per `_SOURCE_TXN_ID`, distinct `_SOURCE_TXN_SEQ` = `_SOURCE_TXN_COUNT`.
8. **Reference tables stay CSV** (as the generator writes them) and are loaded by COPY into
   `REF.FX_RATE_DAILY`, `REF.POSTAL_ZONE`, `REF.COUNTRY_ALIAS`; dbt reads them as sources instead of seeds.
   **Generator manifests** are loaded into `DQ.GENERATOR_RUNS`, `DQ.ANOMALY_GROUND_TRUTH` and
   `DQ.GENERATOR_RULE_VIOLATIONS`, keyed by `run_id`.
9. **The DLQ stays out of bronze for now**: no `RAW_DLQ`; the DLQ files of the backfill stay on the stage.

## Consequences

- The two paths differ in `_KAFKA_HEADERS` and in what `_KAFKA_TS` means (simulated broker time vs publish time);
  the Phase 6 check compares the normalized columns, not the header objects. Business time is always `_SOURCE_TS`.
- Known gaps in the existing data, accepted: the file-based topics of 2026-11-01 are not on the stage, and the
  manifests of the 2026-11-01 publish were not uploaded, so the answer key covers the backfill only.
- Tasks only wake `WH_LOAD` when a stream has data; with no new publishes they cost nothing after the first run.
- If the connector is redeployed one day with `header.converter=StringConverter`, new rows get string headers; the
  normalization in decision 2 already handles both forms.
