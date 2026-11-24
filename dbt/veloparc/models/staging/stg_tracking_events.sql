-- Scan events without technical duplicates or resends. Business duplicates (R4) are kept: they are DQ findings.
with parsed as (
select
    RAW_PAYLOAD:eventId::varchar as event_id,
    RAW_PAYLOAD:eventType::varchar as event_type,
    RAW_PAYLOAD:trackId::varchar as track_id,
    {{ sscc_body_from_urn('RAW_PAYLOAD:epcList[0]::varchar') }} as _sscc_body,
    upper(trim(RAW_PAYLOAD:status::varchar)) as status,
    replace(RAW_PAYLOAD:bizStep::varchar, 'urn:epcglobal:cbv:bizstep:', '') as biz_step,
    {{ ts_utc('RAW_PAYLOAD:eventTime::varchar') }} as event_time_utc,
    iff(endswith(RAW_PAYLOAD:eventTime::varchar, 'Z'), 'Z', right(RAW_PAYLOAD:eventTime::varchar, 6)) as event_tz_offset,
    {{ ts_utc('RAW_PAYLOAD:recordTime::varchar') }} as record_time_utc,
    RAW_PAYLOAD:bizLocation:gln::varchar as location_gln,
    trim(RAW_PAYLOAD:bizLocation:name::varchar) as location_name,
    RAW_PAYLOAD:reasonCode::varchar as reason_code,
    RAW_PAYLOAD:device:scannerId::varchar as scanner_id,
    {{ bronze_meta() }}
from {{ source('bronze', 'V_RAW_TRACKING_EVENTS') }}
where {{ not_duplicate() }}
)
select
    * exclude (_sscc_body),
    {{ sscc_from_body('_sscc_body') }} as sscc
from parsed
