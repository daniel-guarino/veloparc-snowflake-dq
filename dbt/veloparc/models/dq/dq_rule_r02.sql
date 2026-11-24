-- R2: TrackIDs of events and invoices exist in a shipment; invoiced parcels were picked up; delivered parcels are
-- invoiced; invoiced customers exist in master data. Keys follow the generator's answer key.
with parcels as (select track_id from {{ ref('veloship_parcel') }} where track_id is not null),
lines as (
    select l.invoice_number, l.line_no, l.track_id, i.doc_type, i.contact_id, i.issue_date,
           i._event_id, i._source_txn_id, i._file_name, i._kafka_offset
    from {{ ref('invoice_line') }} l
    join {{ ref('invoice') }} i on i.invoice_number = l.invoice_number
),
picked_up as (select distinct track_id from {{ ref('tracking_event') }} where status = 'PICKUP'),
delivered as (
    select e.track_id, min(e.event_time_utc) as delivered_at, any_value(e.consignee_country) as country
    from ({{ events_with_country() }}) e
    where e.status = 'DELIVERED'
    group by 1
)
select {{ dq_violation('R2', 'e.event_id', 'tracking_event', 'null', 'e.event_time_utc',
        "'trackId ' || e.track_id || ' not in any shipment'", 'e._event_id', 'e._source_txn_id', 'e._file_name', 'e._kafka_offset') }}
from {{ ref('tracking_event') }} e
where e.track_id not in (select track_id from parcels)
union all
select {{ dq_violation('R2', "l.invoice_number || '#' || l.line_no", 'invoice', 'left(l.invoice_number, 2)', 'l.issue_date',
        "'trackId ' || l.track_id || ' not in any shipment'", 'l._event_id', 'l._source_txn_id', 'l._file_name', 'l._kafka_offset') }}
from lines l
where l.track_id not in (select track_id from parcels)
union all
select {{ dq_violation('R2', "l.invoice_number || '|' || l.track_id", 'invoice', 'left(l.invoice_number, 2)', 'l.issue_date',
        "'trackId ' || l.track_id || ' billed without PICKUP'", 'l._event_id', 'l._source_txn_id', 'l._file_name', 'l._kafka_offset') }}
from lines l
where l.doc_type = 'INVOICE'
  and l.track_id in (select track_id from parcels)
  and l.track_id not in (select track_id from picked_up)
union all
select {{ dq_violation('R2', 'd.track_id', 'invoice', 'd.country', 'd.delivered_at', "'delivered parcel never invoiced'") }}
from delivered d
where d.track_id not in (select track_id from lines where doc_type = 'INVOICE' and track_id is not null)
union all
select {{ dq_violation('R2', 'i.invoice_number', 'invoice', 'left(i.invoice_number, 2)', 'i.issue_date',
        "'customer ' || i.contact_id || ' not in master data'", 'i._event_id', 'i._source_txn_id', 'i._file_name', 'i._kafka_offset') }}
from {{ ref('invoice') }} i
where i.contact_id not in (select contact_id from {{ ref('customer') }})
