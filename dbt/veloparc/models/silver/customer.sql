-- Every customer version received (contact_id, updated_at): gold builds SCD2 from these (valid_from = updated_at).
{{ config(unique_key=['contact_id', 'updated_at'], incremental_strategy='merge') }}

select
    contact_id,
    updated_at,
    group_customer_id,
    name as customer_name,
    country_code,
    segment,
    vat_number,
    _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
from {{ ref('stg_master_data') }}
where entity = 'customer' {{ new_rows_only() }}
qualify row_number() over (partition by contact_id, updated_at order by {{ latest_first() }}) = 1
