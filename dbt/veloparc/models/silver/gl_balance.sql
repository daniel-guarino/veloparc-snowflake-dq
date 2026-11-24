-- GL balances from the latest complete batch per (company_code, period) (mapping decision #13). Rebuilt on every
-- run: a few rows per month, and a new batch replaces the previous one.
{{ config(materialized='table') }}

with complete_batches as (
    select BATCH_ID as batch_id, COMPANY_CODE as company_code, PERIOD as period, LAST_ROW_TS as last_row_ts
    from {{ source('bronze', 'V_GL_BATCH_COMPLETENESS') }}
    where IS_COMPLETE
    qualify row_number() over (partition by COMPANY_CODE, PERIOD order by LAST_ROW_TS desc, BATCH_ID desc) = 1
)
select
    g.company_code, g.country_code, g.period, g.gl_account, g.account_name, g.amount_eur,
    g._source_txn_id as batch_id,
    g._event_id, g._source_ts, g._kafka_ts, g._kafka_offset, g._file_name, g._loaded_at, g._inserted_at
from {{ ref('stg_gl_extract') }} g
join complete_batches b on b.batch_id = g._source_txn_id
qualify row_number() over (partition by g.company_code, g.period, g.gl_account order by g._kafka_ts desc, g._kafka_offset desc) = 1
