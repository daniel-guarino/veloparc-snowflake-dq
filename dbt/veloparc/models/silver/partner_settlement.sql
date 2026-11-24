-- One row per partner settlement (latest version). partnerName stays in bronze (mapping decision #4).
{{ config(unique_key='settlement_id', incremental_strategy='merge') }}

select
    settlement_id, partner_id, zone_code, period, currency, stops, delivered_parcels, failed_attempts,
    cost_per_stop, total_cost,
    _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
from {{ ref('stg_partner_settlements') }}
where true {{ new_rows_only() }}
qualify row_number() over (partition by settlement_id order by {{ latest_first() }}) = 1
