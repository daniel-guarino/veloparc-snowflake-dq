-- Latest version per partner (ordered by bronze metadata, mapping decision #11). Zones: partner_zone.
{{ config(unique_key='partner_id', incremental_strategy='merge') }}

with candidates as (
    select
        partner_id, name as partner_name, country_code, base_depot_gln,
        _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
    from {{ ref('stg_master_data') }}
    where entity = 'partner' {{ new_rows_only() }}
    {% if is_incremental() %}
    union all
    select
        partner_id, partner_name, country_code, base_depot_gln,
        _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
    from {{ this }}
    {% endif %}
)
select * from candidates
qualify row_number() over (partition by partner_id order by {{ latest_first() }}) = 1
