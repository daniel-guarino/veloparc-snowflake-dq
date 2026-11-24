-- R6: the scan GLN is a depot; the last scan, when DELIVERED, is at a depot in the consignee's zone
-- (zone = REF.POSTAL_ZONE of the country and the first character of the postal code, as in the generator).
with events as ({{ events_with_country() }}),
last_scan as (
    select * from events
    qualify row_number() over (partition by track_id order by event_time_utc desc, event_id desc) = 1
)
select {{ dq_violation('R6', 'e.event_id', 'tracking_event', 'e.consignee_country', 'e.event_time_utc',
        "'GLN ' || e.location_gln || ' is not a depot'", 'e._event_id', 'e._source_txn_id', 'e._file_name', 'e._kafka_offset') }}
from events e
where e.location_gln is not null and e.location_gln not in (select gln from {{ ref('depot') }})
union all
select {{ dq_violation('R6', 'l.track_id', 'tracking_event', 'l.consignee_country', 'l.event_time_utc',
        "'last scan at ' || l.location_gln || ' outside consignee zone'", 'l._event_id', 'l._source_txn_id', 'l._file_name', 'l._kafka_offset') }}
from last_scan l
join {{ ref('depot') }} d on d.gln = l.location_gln
left join {{ source('ref', 'POSTAL_ZONE') }} dz on dz.COUNTRY_CODE = d.country_code and dz.POSTAL_PREFIX = left(d.postal_code, 1)
left join {{ source('ref', 'POSTAL_ZONE') }} cz on cz.COUNTRY_CODE = l.consignee_country and cz.POSTAL_PREFIX = left(l.consignee_zip, 1)
where l.status = 'DELIVERED' and not equal_null(dz.ZONE_CODE, cz.ZONE_CODE)
