-- Latest version per tariff (compacted topic, mapping decision #10). Bands: tariff_weight_band.
{{ config(unique_key='tariff_id', incremental_strategy='merge') }}

with candidates as (
    select
        tariff_id, contact_id, product, zone_code, currency, fuel_surcharge_pct, valid_from, valid_to,
        _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
    from {{ ref('stg_tariffs') }}
    where true {{ new_rows_only() }}
    {% if is_incremental() %}
    union all
    select
        tariff_id, contact_id, product, zone_code, currency, fuel_surcharge_pct, valid_from, valid_to,
        _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
    from {{ this }}
    {% endif %}
)
select * from candidates
qualify row_number() over (partition by tariff_id order by {{ latest_first() }}) = 1
