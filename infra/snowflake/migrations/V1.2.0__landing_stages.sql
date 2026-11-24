-- Landing zone inside Snowflake (ADR 0003): same layout as the S3 Sink,
--   @BRONZE.LANDING/landing/kafka/<topic>/year=YYYY/month=MM/day=dd[/hour=HH]/<topic>+<partition>+<offset>.json.gz
-- Generator manifests and state: @BRONZE.GENERATOR/runs/<run_id>/, @BRONZE.GENERATOR/state/.
CREATE FILE FORMAT IF NOT EXISTS VLP_DP.BRONZE.FF_LANDING_JSONL
  TYPE = JSON COMPRESSION = GZIP STRIP_OUTER_ARRAY = FALSE
  COMMENT = 'One envelope per line: payload + _kafka_* + _kafka_headers';

CREATE STAGE IF NOT EXISTS VLP_DP.BRONZE.LANDING
  DIRECTORY = (ENABLE = TRUE)
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  FILE_FORMAT = VLP_DP.BRONZE.FF_LANDING_JSONL
  COMMENT = 'Landing files (backfill and file-based topics), uploaded by GitHub Actions';

CREATE STAGE IF NOT EXISTS VLP_DP.BRONZE.GENERATOR
  DIRECTORY = (ENABLE = TRUE)
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  COMMENT = 'Generator run manifests, anomaly answer key and stream state (not business data)';
