-- R4: no repeated scan with the same (TrackID, status, GLN) (UPU repeated-event rule).
select {{ dq_violation('R4', "track_id || '|' || status || '|' || coalesce(location_gln, 'None')", 'tracking_event',
        'any_value(consignee_country)', 'min(event_time_utc)', "'repeated ' || count(*) || ' times'") }}
from ({{ events_with_country() }})
group by track_id, status, location_gln
having count(*) > 1
