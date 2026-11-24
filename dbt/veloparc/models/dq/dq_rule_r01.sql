-- R1: every ShipmentUnit of a VeloShip message gets a TrackID (len(ParcelData) = len(ShipmentUnit)).
with counts as (
    select message_id, count_if(track_id is not null) as track_ids, count_if(is_matched or track_id is null) as units
    from {{ ref('veloship_parcel') }}
    group by 1
    having count_if(not is_matched) > 0
)
select {{ dq_violation('R1', 's.message_id', 'veloship_shipment', 's.consignee_country', 's.shipping_date',
        "c.units || ' units, ' || c.track_ids || ' TrackIDs'", 's._event_id', 's._source_txn_id', 's._file_name', 's._kafka_offset') }}
from counts c
join {{ ref('veloship_shipment') }} s on s.message_id = c.message_id
