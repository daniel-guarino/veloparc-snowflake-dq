-- R15: valid GS1 check digit in SSCCs (shipments), depot GLNs, partner base depots and scan GLNs; the customer's
-- vatNumber starts with its country code (latest version).
select {{ dq_violation('R15', 'p.track_id', 'veloship_shipment', 's.consignee_country', 's.shipping_date',
        "'invalid SSCC ' || p.sscc", 's._event_id', 's._source_txn_id', 's._file_name', 's._kafka_offset') }}
from {{ ref('veloship_parcel') }} p
join {{ ref('veloship_shipment') }} s on s.message_id = p.message_id
where p.sscc is not null and not coalesce({{ gs1_is_valid('p.sscc', 18) }}, false)
union all
select {{ dq_violation('R15', 'gln', 'master_data', 'country_code', '_source_ts', "'invalid GLN check digit'",
        '_event_id', '_source_txn_id', '_file_name', '_kafka_offset') }}
from {{ ref('depot') }}
where not coalesce({{ gs1_is_valid('gln', 13) }}, false)
union all
select {{ dq_violation('R15', 'partner_id', 'master_data', 'country_code', '_source_ts', "'invalid baseDepotGln'",
        '_event_id', '_source_txn_id', '_file_name', '_kafka_offset') }}
from {{ ref('partner') }}
where not coalesce({{ gs1_is_valid('base_depot_gln', 13) }}, false)
union all
select {{ dq_violation('R15', 'contact_id', 'master_data', 'country_code', 'updated_at', "'vatNumber prefix != countryCode'",
        '_event_id', '_source_txn_id', '_file_name', '_kafka_offset') }}
from (
    select * from {{ ref('customer') }}
    qualify row_number() over (partition by contact_id order by updated_at desc) = 1
)
where left(vat_number, 2) <> country_code
union all
select {{ dq_violation('R15', 'e.event_id', 'tracking_event', 'e.consignee_country', 'e.event_time_utc',
        "'invalid GLN ' || e.location_gln", 'e._event_id', 'e._source_txn_id', 'e._file_name', 'e._kafka_offset') }}
from ({{ events_with_country() }}) e
where e.location_gln is not null and not coalesce({{ gs1_is_valid('e.location_gln', 13) }}, false)
