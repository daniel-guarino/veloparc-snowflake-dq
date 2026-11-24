-- Latest version per opportunity, ordered by LastModifiedDate (mapping: crm_object Opportunity).
{{ config(unique_key='opportunity_id', incremental_strategy='merge') }}

with candidates as (
    select
        crm_id as opportunity_id, account_id, contact_id, opportunity_name, stage, amount, currency, close_date,
        opportunity_country_code as country_code, expected_parcels_month, last_modified_at,
        _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
    from {{ ref('stg_crm_objects') }}
    where object_type = 'Opportunity' {{ new_rows_only() }}
    {% if is_incremental() %}
    union all
    select
        opportunity_id, account_id, contact_id, opportunity_name, stage, amount, currency, close_date,
        country_code, expected_parcels_month, last_modified_at,
        _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
    from {{ this }}
    {% endif %}
)
select * from candidates
qualify row_number() over (
    partition by opportunity_id order by last_modified_at desc nulls last, {{ latest_first() }}
) = 1
