# Kafka Connect (Aiven, managed)

| File | Connector |
| --- | --- |
| [`snowflake_sink.json`](snowflake_sink.json) | Snowflake sink 3.x, Snowpipe Streaming, from the 4 Kafka topics to `VLP_DP.BRONZE.KAFKA_*` (ADR 0003) |

The file is a template without secrets: `${SNOWFLAKE_URL}` and `${SNOWFLAKE_PRIVATE_KEY}` are filled in by
`veloparc-gen connector-deploy`, run by the **connector-deploy** workflow. Each run:

1. generates a new RSA key pair in memory;
2. sets the public key on the Snowflake user `SVC_AIVEN_CONNECTOR` (`ALTER USER ... SET RSA_PUBLIC_KEY`);
3. creates or updates the connector on the Kafka Connect service through the Aiven API;
4. waits until the connector and its tasks are `RUNNING`, or fails with the task's error.

The private key is never written to disk, printed or stored; re-running the workflow rotates it.

## Choices

- **Connector 3.x** (`SnowflakeSinkConnector`, `snowflake.ingestion.method=SNOWPIPE_STREAMING`): stable; 4.x is
  in early availability on Aiven.
- **Schematization off:** one table per topic with `RECORD_METADATA` (topic, partition, offset, CreateTime, key,
  headers) and `RECORD_CONTENT` (the JSON payload). Bronze maps them to the `RAW_*` tables (Phase 6).
- **Converters:** key as string, value as schemaless JSON. A message that is not valid JSON (the generator's
  truncated DLQ copies) fails the converter and goes to `vlp.dlq.s3-sink.v1` (`errors.tolerance=all`).
- On the Aiven Developer tier, Kafka Connect is a separate service: set its name in the Environment `dev`
  variable `AIVEN_CONNECT_SERVICE` (falls back to `AIVEN_SERVICE`).
