-- One row per scan event. event_date_key is the local business date (UTC shifted by the original offset, decision
-- #7). is_duplicate marks repeated (track_id, status, GLN) scans after the first (R4, decision #9).
select
    e.event_id,
    e.track_id,
    case
        when e.location_gln is null then null
        when dd.depot_gln is null then '0000000000000'
        else e.location_gln
    end as depot_gln,
    {{ date_key("dateadd(minute, iff(e.event_tz_offset = 'Z' or e.event_tz_offset is null, 0,
        iff(left(e.event_tz_offset, 1) = '-', -1, 1)
        * (try_to_number(substr(e.event_tz_offset, 2, 2)) * 60 + try_to_number(substr(e.event_tz_offset, 5, 2)))),
        e.event_time_utc)::date") }} as event_date_key,
    e.status,
    e.event_time_utc as event_ts,
    e.reason_code,
    datediff(minute, e.event_time_utc, e.record_time_utc) as capture_delay_min,
    row_number() over (
        partition by e.track_id, e.status, e.location_gln order by e.event_time_utc, e.event_id
    ) > 1 as is_duplicate
from {{ ref('tracking_event') }} e
left join {{ ref('dim_depot') }} dd on dd.depot_gln = e.location_gln
