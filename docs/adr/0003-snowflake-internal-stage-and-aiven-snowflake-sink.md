# ADR 0003 · No AWS: Snowflake internal stage for files, Aiven Snowflake sink for Kafka topics

- Status: accepted
- Date: 2026-11-23
- Supersedes: the S3 landing (implementation plan, Phase 4) and the S3 sink connector (ADR 0002, decision 1)

## Context

The maintainer does not want an AWS account. S3 was only the landing zone between the generator / Kafka Connect
and Snowflake. The Aiven trial runs in the UK (DigitalOcean, London); Snowflake stays on AWS `eu-west-3` (Paris),
in Snowflake's own cloud account, so no AWS account of ours is involved.

Options considered:

- **A. Snowflake internal stage + Aiven Snowflake sink connector** (chosen).
- B. Snowflake internal stage + our own consumer job in GitHub Actions writing S3-Sink-style files. Single format
  and simpler, but Kafka Connect would no longer be used.
- C. Another S3-compatible store (R2, GCS, Azure): another account, usually a card, and no Snowpipe auto-ingest
  from Snowflake's S3-compatible stages. No gain over AWS.

## Decision

1. **Files land in a Snowflake internal stage**, `@BRONZE.LANDING`, with the same layout as before:
   `landing/kafka/<topic>/year=YYYY/month=MM/day=dd[/hour=HH]/<topic>+<partition>+<startOffset>.json.gz`.
   They are the 2-month backfill of every topic and, in stream mode, the 4 file-based topics (master data, tariffs,
   settlements, GL). GitHub Actions uploads them with `PUT`; `COPY INTO` (task or pipe via REST) loads them, and
   `METADATA$FILENAME` / `METADATA$FILE_ROW_NUMBER` / `METADATA$FILE_CONTENT_KEY` /
   `METADATA$FILE_LAST_MODIFIED` still fill the file metadata columns.
2. **Kafka topics reach Snowflake through the Aiven-managed Snowflake sink connector** (Snowpipe Streaming), one
   connector table per topic (`RECORD_METADATA`, `RECORD_CONTENT`). `RECORD_METADATA` carries topic, partition,
   offset, timestamp, key and headers, so the Kafka and source-journal metadata columns are filled as before; the
   file columns are NULL for streamed rows. A stream + task moves the connector rows into the same `RAW_*` table
   as the files, so each topic still has one bronze table, fed by two paths.
3. **Generator outputs** go to `@BRONZE.GENERATOR`: `runs/<run_id>/run_manifest.json`, `anomaly_manifest.jsonl`,
   `rule_violations.jsonl`, and `state/` for stream mode (offsets of file topics, parcels in transit). Not loaded
   by any pipe as business data.
4. **Configuration as code through APIs, run by GitHub Actions:** topics, ACLs and the connector are created
   through the Aiven API (`AIVEN_API_TOKEN`); the connector's Snowflake private key never leaves GitHub secrets.
   Snowflake objects are versioned SQL applied by the deploy pipeline.
5. **Technical deduplication in bronze is by (topic, partition, offset, `ce_id`)**: backfill files carry
   simulated offsets that overlap the broker's real offsets on the same topics.

## Consequences

- Phase 4 (S3, Terraform for AWS) is dropped. The Snowflake foundation (Phase 5) moves before the connector,
  so the Snowflake trial (30 days) starts during Phase 3.
- Bronze has two loaders per Kafka topic (COPY from files, task from connector tables) that must produce
  identical metadata columns; this is tested in Phase 6.
- The landing is no longer an object store outside the warehouse: retention follows the stage (no Glacier
  lifecycle). Bronze remains the permanent record.
- Snowpipe Streaming and the tasks consume a few credits per day; the resource monitor from Phase 5 covers them.
