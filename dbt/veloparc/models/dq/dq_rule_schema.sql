-- SCHEMA: contract checks the bronze load does not enforce (ADR 0004, decision 6): enum and pattern of
-- tracking_event, and the credit note if/then (relatedInvoiceNumber required).
select {{ dq_violation('SCHEMA', 'e.event_id', 'tracking_event', 'e.consignee_country', 'e.event_time_utc',
        "'status ' || e.status || ' not in the contract enum'", 'e._event_id', 'e._source_txn_id', 'e._file_name', 'e._kafka_offset') }}
from ({{ events_with_country() }}) e
where e.status not in ('DATA_RECEIVED', 'PICKUP', 'HUB', 'IN_DELIVERY', 'DELIVERED', 'CANCELLED', 'NOT_DELIVERED', 'PARCELSHOP')
union all
select {{ dq_violation('SCHEMA', 'e.event_id', 'tracking_event', 'e.consignee_country', 'e.event_time_utc',
        "'bizLocation.gln ' || e.location_gln || ' is not 13 digits'", 'e._event_id', 'e._source_txn_id', 'e._file_name', 'e._kafka_offset') }}
from ({{ events_with_country() }}) e
where e.location_gln is not null and (length(e.location_gln) <> 13 or try_to_number(e.location_gln) is null)
union all
select {{ dq_violation('SCHEMA', 'i.invoice_number', 'invoice', 'left(i.invoice_number, 2)', 'i.issue_date',
        "'credit note without relatedInvoiceNumber'", 'i._event_id', 'i._source_txn_id', 'i._file_name', 'i._kafka_offset') }}
from {{ ref('invoice') }} i
where i.doc_type = 'CREDIT_NOTE' and i.related_invoice_number is null
