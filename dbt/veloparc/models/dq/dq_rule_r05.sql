-- R5: recordTime >= eventTime and at most 15 minutes later (max_normal_delay_minutes of the simulation); and
-- every country with scans on a day also records scans that day (a gateway outage shifts them to the next day).
with events as ({{ events_with_country() }}),
days as (
    select consignee_country as country, event_time_utc::date as day, 'event' as kind from events
    union
    select consignee_country, record_time_utc::date, 'record' from events
)
select {{ dq_violation('R5', 'event_id', 'tracking_event', 'consignee_country', 'event_time_utc',
        "iff(record_time_utc < event_time_utc, 'recordTime before eventTime',
             'recorded ' || datediff(minute, event_time_utc, record_time_utc) || ' minutes late')",
        '_event_id', '_source_txn_id', '_file_name', '_kafka_offset') }}
from events
where record_time_utc < event_time_utc or datediff(second, event_time_utc, record_time_utc) > 15 * 60
union all
select {{ dq_violation('R5', "d.country || '|' || to_char(d.day, 'YYYY-MM-DD')", 'tracking_event', 'd.country', 'd.day',
        "'no scans recorded on a day with activity'") }}
from days d
where d.kind = 'event' and d.country is not null
  and not exists (select 1 from days r where r.kind = 'record' and r.country = d.country and r.day = d.day)
