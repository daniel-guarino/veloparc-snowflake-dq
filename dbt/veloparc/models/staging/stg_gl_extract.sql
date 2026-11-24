-- GL extract rows without technical duplicates or resends; _source_txn_id identifies the batch.
select
    RAW_PAYLOAD:company_code::varchar as company_code,
    RAW_PAYLOAD:country_code::varchar as country_code,
    RAW_PAYLOAD:period::varchar as period,
    RAW_PAYLOAD:gl_account::varchar as gl_account,
    RAW_PAYLOAD:account_name::varchar as account_name,
    try_cast(RAW_PAYLOAD:amount_eur::varchar as number(14, 2)) as amount_eur,
    {{ bronze_meta() }}
from {{ source('bronze', 'V_RAW_GL_EXTRACT') }}
where {{ not_duplicate() }}
