-- R3: per parcel, the scan statuses in time order follow DATA_RECEIVED > PICKUP > HUB (1-3) > IN_DELIVERY >
-- DELIVERED, with NOT_DELIVERED / PARCELSHOP as detours and CANCELLED only right after DATA_RECEIVED.
-- The rule is a grammar over one letter per status (same as the generator's checker): a regex is the rule itself.
with tokens as (
    select
        track_id,
        any_value(consignee_country) as country,
        min(event_time_utc)::date as first_day,
        listagg(decode(status, 'DATA_RECEIVED', 'D', 'PICKUP', 'P', 'HUB', 'H', 'IN_DELIVERY', 'I', 'DELIVERED', 'V',
                       'NOT_DELIVERED', 'N', 'PARCELSHOP', 'S', 'CANCELLED', 'C', '?'), '')
            within group (order by event_time_utc, event_id) as sequence,
        listagg(status, ' > ') within group (order by event_time_utc, event_id) as statuses
    from ({{ events_with_country() }})
    group by 1
)
select {{ dq_violation('R3', 'track_id', 'tracking_event', 'country', 'first_day', 'statuses') }}
from tokens
where not regexp_like(sequence, 'D(C|PH{1,3}(IV|INSV|SV))')
