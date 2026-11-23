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

> Implementação no case: no free tier da Aiven só os tópicos de shipments, tracking events, invoices, CRM e a DLQ passam pelo Kafka (2 partições cada). Master data, tarifas, liquidações e GL chegam como ficheiros no mesmo layout e envelope. Ver `implementation_plan.md`, Phase 3.

## Headers obrigatórios

`ce_id`, `ce_source`, `ce_type`, `ce_time`, `ce_specversion`, `vlp_op` (I/U/D/R), `vlp_producer`, `vlp_entity`, `vlp_batch_id`, `vlp_batch_seq`, `vlp_batch_count`, `vlp_schema_version`, `traceparent`.

## Landing S3

```
s3://vlp-dp-landing-prd/landing/kafka/<topic>/year=YYYY/month=MM/day=dd/hour=HH/<topic>+<partition>+<startOffset>.json.gz
```

- S3 Sink, TimeBasedPartitioner, `timestamp.extractor=Record`, UTC: a pasta é a hora de chegada ao broker, não a hora do evento.
- JSON lines gzip; SMTs HoistField (`payload`) + InsertField (`_kafka_topic/partition/offset/timestamp`) + headers → `_kafka_headers`.
- Fontes mensais: pasta diária e `rotate.schedule.interval.ms` (at-least-once; dedup por offset na bronze).

## Bronze: 25 colunas de metadados (inspiradas nos journal control fields do IBM CDC)

- A · Journal da origem: `_OP_TYPE` (&ENTTYP), `_SOURCE_TS` (&TIMSTAMP), `_SOURCE_SYSTEM` (&SYSTEM/&JOB), `_SOURCE_USER` (&USER), `_SOURCE_TXN_ID` (&CCID), `_SOURCE_TXN_SEQ`, `_SOURCE_TXN_COUNT`, `_EVENT_ID`, `_ENTITY_TYPE`
- B · Kafka: `_KAFKA_TOPIC`, `_KAFKA_PARTITION`, `_KAFKA_OFFSET` (&SEQNO), `_KAFKA_TS`, `_KAFKA_HEADERS`
- C · Ficheiro: `_FILE_NAME`, `_FILE_ROW_NUMBER`, `_FILE_CONTENT_KEY`, `_FILE_LAST_MODIFIED` (METADATA$)
- D · Carga e lineage: `_SCHEMA_ID`, `_SCHEMA_VERSION`, `_TRACE_ID`, `_LOAD_TS`, `_INSERTED_AT`
- Mais `RAW_PAYLOAD VARIANT` e `BK_*` (chaves de negócio).

`_OP_TYPE`: I = PT, U = UP (só after-image), D = DL, R = RR.

## Qualidade

- Bronze append-only. Stream + Task com UDF `VALIDATE_JSON(_SCHEMA_ID, RAW_PAYLOAD)`; os inválidos vão para `BRONZE.DQ_QUARANTINE`.
- Duplicado técnico (mesmo topic+partition+offset): fica o primeiro. Reenvio do produtor (mesmo ce_id): fica o primeiro. Duplicado de negócio (R4): mantém-se e é marcado como anomalia.
- Completude do GL: COUNT(*) por `_SOURCE_TXN_ID` = `_SOURCE_TXN_COUNT`.

## Decisões em aberto

HeaderToField disponível no conector? · CRM num tópico ou dois · GL via Kafka ou ficheiro direto · retenção da landing (obrigações fiscais).

As propostas para cada uma estão em `implementation_plan.md`, Phase 0.
