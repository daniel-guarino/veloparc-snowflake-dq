-- One row per scan event (event_id). Business duplicates (R4), late records (R5) and invalid statuses are kept:
-- the Phase 9 tests count them.
{{ config(unique_key='event_id', incremental_strategy='merge') }}

select
    event_id, event_type, track_id, sscc, status, biz_step, event_time_utc, event_tz_offset, record_time_utc,
    location_gln, location_name, reason_code, scanner_id,
    _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
from {{ ref('stg_tracking_events') }}
where true {{ new_rows_only() }}
qualify row_number() over (partition by event_id order by {{ latest_first() }}) = 1
