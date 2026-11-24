# Ingestão: Kafka → S3 landing → Snowflake bronze

## Tópicos

Convenção `vlp.<domínio>.<dataset>.v<major>`. Todos: RF=3, min.insync.replicas=2, zstd, `message.timestamp.type=LogAppendTime`.

| Tópico | Chave | Part. | Limpeza · retenção | Landing | Bronze | _SOURCE_TS |
|---|---|---|---|---|---|---|
| vlp.shipping.veloship-shipment.v1 | messageId | 6 | delete · 7 d | hora | RAW_VELOSHIP_SHIPMENTS | receivedAt |
| vlp.ops.tracking-event.v1 | trackId | 6 | delete · 7 d | hora | RAW_TRACKING_EVENTS | recordTime |
| vlp.finance.invoice.v1 | invoiceNumber | 3 | delete · 30 d (max.message.bytes 4 MB) | hora | RAW_INVOICES | issueDate |
| vlp.sales.crm-object.v1 | Id | 3 | compact,delete · 30 d | hora | RAW_CRM_OBJECTS | LastModifiedDate / CreatedDate |
| vlp.ops.partner-settlement.v1 | settlementId | 1 | delete · 30 d | dia | RAW_PARTNER_SETTLEMENTS | ce_time |
| vlp.pricing.tariff.v1 | tariffId | 3 | compact | hora | RAW_TARIFFS | ce_time |
| vlp.finance.gl-extract.v1 | company_code\|period\|gl_account | 1 | delete · 30 d | dia | RAW_GL_EXTRACT | ce_time do lote |
| vlp.mdm.master-data.v1 | entity:chave natural | 3 | compact | hora | RAW_MASTER_DATA | updatedAt |
| vlp.dlq.s3-sink.v1 | original | 1 | delete · 14 d | hora | RAW_DLQ | — |

Schema Registry com compatibilidade BACKWARD mas sem validação estrita no produtor: as anomalias do case têm de chegar à bronze.

> Implementação no case: dentro dos limites do free tier da Aiven, só os tópicos de shipments, tracking events, invoices, CRM e a DLQ passam pelo Kafka (2 partições cada). Master data, tarifas, liquidações e GL chegam como ficheiros no mesmo layout e envelope. O Kafka Connect é o gerido pela Aiven (plano trial), com o S3 sink da Aiven. Ver `implementation_plan.md`, Phase 3, e ADR 0002.

## Headers obrigatórios

`ce_id`, `ce_source`, `ce_type`, `ce_time`, `ce_specversion`, `vlp_op` (I/U/D/R), `vlp_producer`, `vlp_entity`, `vlp_batch_id`, `vlp_batch_seq`, `vlp_batch_count`, `vlp_schema_version`, `traceparent`.

## Landing S3

> **Atualização (ADR 0003):** sem conta AWS. O mesmo layout vive no stage interno do Snowflake `@BRONZE.LANDING` (backfill e tópicos de ficheiro, enviados por GitHub Actions com `PUT`); os tópicos Kafka chegam ao Snowflake pelo conector Snowflake gerido da Aiven (Snowpipe Streaming), com `RECORD_METADATA` (topic, partition, offset, timestamp, key, headers) a alimentar as mesmas colunas de metadados.

```
s3://vlp-dp-landing-prd/landing/kafka/<topic>/year=YYYY/month=MM/day=dd/hour=HH/<topic>+<partition>+<startOffset>.json.gz
```

- S3 Sink, TimeBasedPartitioner, `timestamp.extractor=Record`, UTC: a pasta é a hora de chegada ao broker, não a hora do evento.
- JSON lines gzip; SMTs HoistField (`payload`) + InsertField (`_kafka_topic/partition/offset/timestamp`) + headers → `_kafka_headers`.
- Fontes mensais: pasta diária e `rotate.schedule.interval.ms` (at-least-once; dedup por offset na bronze).

## Bronze: 25 metadata columns (inspired by the IBM CDC journal control fields)

> Section in English (project rule for docs); the rest of this file is older and in Portuguese. Decisions: ADR 0004.

Eight `RAW_*` tables (no `RAW_DLQ` for now), append-only: the 25 columns below, `RAW_PAYLOAD VARIANT` and `BK_*`
(business keys per topic). Both loaders fill the same columns: **file** = COPY from `@BRONZE.LANDING`
(`$1` is the envelope, `h` = `$1:_kafka_headers`), **connector** = stream + task from `KAFKA_*`
(`h` = `RECORD_METADATA:headers`).

| # | Column | Type | File | Connector |
| --- | --- | --- | --- | --- |
| A1 | `_OP_TYPE` (&ENTTYP) | VARCHAR(1) | `h:vlp_op` | `h:vlp_op` |
| A2 | `_SOURCE_TS` (&TIMSTAMP) | TIMESTAMP_TZ | payload field per topic (table above); `ce_time` for settlements, tariffs, GL | payload field per topic |
| A3 | `_SOURCE_SYSTEM` (&SYSTEM) | VARCHAR | `h:ce_source` | `h:ce_source` |
| A4 | `_SOURCE_USER` (&USER) | VARCHAR | `h:vlp_producer` | `h:vlp_producer` |
| A5 | `_SOURCE_TXN_ID` (&CCID) | VARCHAR | `h:vlp_batch_id` | `h:vlp_batch_id` |
| A6 | `_SOURCE_TXN_SEQ` | NUMBER | `h:vlp_batch_seq` (string) | `h:vlp_batch_seq` (integer) |
| A7 | `_SOURCE_TXN_COUNT` | NUMBER | `h:vlp_batch_count` (string) | `h:vlp_batch_count` (integer) |
| A8 | `_EVENT_ID` | VARCHAR | `h:ce_id` | `h:ce_id` |
| A9 | `_EVENT_TYPE` | VARCHAR | `h:ce_type` | `h:ce_type` |
| A10 | `_ENTITY_TYPE` | VARCHAR | `h:vlp_entity` | `h:vlp_entity` |
| B1 | `_KAFKA_TOPIC` | VARCHAR | `$1:_kafka_topic` | `RECORD_METADATA:topic` |
| B2 | `_KAFKA_PARTITION` | NUMBER | `$1:_kafka_partition` | `RECORD_METADATA:partition` |
| B3 | `_KAFKA_OFFSET` (&SEQNO) | NUMBER | `$1:_kafka_offset` (simulated) | `RECORD_METADATA:offset` (broker) |
| B4 | `_KAFKA_KEY` | VARCHAR | NULL (the envelope has no key) | `RECORD_METADATA:key` |
| B5 | `_KAFKA_TS` | TIMESTAMP_TZ | `$1:_kafka_timestamp` (ms, simulated broker time) | `RECORD_METADATA:CreateTime` (ms, publish time) |
| B6 | `_KAFKA_HEADERS` | OBJECT | `$1:_kafka_headers`, as delivered | `RECORD_METADATA:headers`, as delivered |
| C1 | `_FILE_NAME` | VARCHAR | `METADATA$FILENAME` | NULL |
| C2 | `_FILE_ROW_NUMBER` | NUMBER | `METADATA$FILE_ROW_NUMBER` | NULL |
| C3 | `_FILE_CONTENT_KEY` | VARCHAR | `METADATA$FILE_CONTENT_KEY` | NULL |
| C4 | `_FILE_LAST_MODIFIED` | TIMESTAMP_NTZ | `METADATA$FILE_LAST_MODIFIED` | NULL |
| D1 | `_SCHEMA_ID` | VARCHAR | `$id` of the topic's JSON Schema | `$id` of the topic's JSON Schema |
| D2 | `_SCHEMA_VERSION` | VARCHAR | `h:vlp_schema_version` | `h:vlp_schema_version` |
| D3 | `_TRACE_ID` | VARCHAR | trace-id of `h:traceparent` | trace-id of `h:traceparent` |
| D4 | `_LOAD_TS` | TIMESTAMP_TZ | `METADATA$START_SCAN_TIME` | `RECORD_METADATA:SnowflakeConnectorPushTime` |
| D5 | `_INSERTED_AT` | TIMESTAMP_TZ | insert into `RAW_*` | insert into `RAW_*` |

`_OP_TYPE`: I = PT, U = UP (after-image only), D = DL, R = RR.

**What the connector path delivers differently** (`snowflake-inspect`, 2026-11-24). The default Kafka Connect
header converter parses some header strings: `ce_time` arrives as epoch milliseconds in a string
(`"1793512844000"` instead of `2026-11-01T06:00:44.000Z`), `ce_specversion` as integer `1` instead of `"1.0"`,
`vlp_batch_seq` / `vlp_batch_count` as integers. The bronze columns normalize this (numbers cast on both paths;
`ce_time` read as epoch ms when all digits, ISO-8601 otherwise); `_KAFKA_HEADERS` keeps each path's object as is.

**Kafka timestamp.** The Aiven Developer plan does not allow `message.timestamp.type=LogAppendTime` (neither per
topic nor at service level), so broker messages carry **CreateTime**, set by the producer when the publish workflow
sends them. In the files, `_kafka_timestamp` is the simulated broker time. Business time is always `_SOURCE_TS`.

## Quality

- Bronze append-only, no validation at load: the JSON Schema rules are checked in dbt (`stg_` models and Phase 9
  tests); rows that break them are flagged in silver, never dropped (ADR 0004, decision 6).
- Duplicates are flagged in the `V_RAW_*` views and removed by the dbt `stg_` models (entry to silver): technical
  duplicate (same topic + partition + offset + `ce_id`, the same message loaded twice) and producer resend (same
  topic + `ce_id`, another offset); the first by `_INSERTED_AT` is kept. `ce_id` is in the technical key because
  backfill offsets and broker offsets overlap. Business duplicate (R4): kept and flagged as an anomaly in silver.
- GL completeness: view `BRONZE.V_GL_BATCH_COMPLETENESS`, per `_SOURCE_TXN_ID`, distinct `_SOURCE_TXN_SEQ` =
  `_SOURCE_TXN_COUNT`.

## Decisões em aberto

HeaderToField disponível no conector? · CRM num tópico ou dois · GL via Kafka ou ficheiro direto · retenção da landing (obrigações fiscais).

As propostas para cada uma estão em `implementation_plan.md`, Phase 0.
