-- Latest version per depot GLN (no updatedAt in the schema: ordered by bronze metadata, mapping decision #11).
{{ config(unique_key='gln', incremental_strategy='merge') }}

with candidates as (
    select
        gln, name as depot_name, depot_type, country_code, city, postal_code,
        _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
    from {{ ref('stg_master_data') }}
    where entity = 'depot' {{ new_rows_only() }}
    {% if is_incremental() %}
    union all
    select
        gln, depot_name, depot_type, country_code, city, postal_code,
        _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
    from {{ this }}
    {% endif %}
)
select * from candidates
qualify row_number() over (partition by gln order by {{ latest_first() }}) = 1
