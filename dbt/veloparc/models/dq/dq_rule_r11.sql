-- R11: a settlement's deliveredParcels = DELIVERED scans in the partner's zone and month. Zone of a parcel =
-- REF.POSTAL_ZONE of the consignee country and first postal character; month = local month of the scan (as the
-- generator counts it: every DELIVERED scan of a parcel with a shipment).
with delivered as (
    select z.ZONE_CODE as zone_code,
           to_char(dateadd(minute, iff(e.event_tz_offset = 'Z', 0,
               iff(left(e.event_tz_offset, 1) = '-', -1, 1)
               * (try_to_number(substr(e.event_tz_offset, 2, 2)) * 60 + try_to_number(substr(e.event_tz_offset, 5, 2)))),
               e.event_time_utc), 'YYYY-MM') as period,
           count(*) as scans
    from ({{ events_with_country() }}) e
    join {{ source('ref', 'POSTAL_ZONE') }} z
        on z.COUNTRY_CODE = e.consignee_country and z.POSTAL_PREFIX = left(e.consignee_zip, 1)
    where e.status = 'DELIVERED'
    group by 1, 2
)
select {{ dq_violation('R11', 'ps.settlement_id', 'partner_settlement', 'left(ps.zone_code, 2)', "to_date(ps.period || '-01')",
        "'deliveredParcels ' || ps.delivered_parcels || ' != ' || coalesce(d.scans, 0)",
        'ps._event_id', 'ps._source_txn_id', 'ps._file_name', 'ps._kafka_offset') }}
from {{ ref('partner_settlement') }} ps
left join delivered d on d.zone_code = ps.zone_code and d.period = ps.period
where ps.delivered_parcels <> coalesce(d.scans, 0)
