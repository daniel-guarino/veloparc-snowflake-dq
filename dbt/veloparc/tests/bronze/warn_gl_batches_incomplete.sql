-- GL completeness (ADR 0004): batches with fewer rows than announced in vlp_batch_count. A warning, not an error:
-- an incomplete batch is a data quality finding to report, and silver keeps only the latest complete batch.
{{ config(severity='warn') }}
select BATCH_ID, COMPANY_CODE, PERIOD, ROWS_RECEIVED, ROWS_EXPECTED
from {{ source('bronze', 'V_GL_BATCH_COMPLETENESS') }}
where not IS_COMPLETE
