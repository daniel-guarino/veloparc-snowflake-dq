-- Request units zipped with response parcels by position (mapping decision #6). FULL OUTER join: an extra unit
-- without a TrackID, or a parcel without a unit, stays visible as a row with NULLs (R1, Completeness).
{{ config(unique_key=['message_id', 'unit_index'], incremental_strategy='delete+insert') }}

with messages as (
    select message_id, shipment_units, parcel_data, _event_id, _inserted_at
    from {{ ref('stg_veloship_shipments') }}
    where true {{ new_rows_only() }}
    qualify row_number() over (partition by message_id order by {{ latest_first() }}) = 1
),
units as (
    select m.message_id, u.index as idx, u.value as unit
    from messages m, lateral flatten(input => m.shipment_units) u
),
parcels as (
    select m.message_id, p.index as idx, p.value as parcel
    from messages m, lateral flatten(input => m.parcel_data) p
)
select
    coalesce(u.message_id, p.message_id) as message_id,
    coalesce(u.idx, p.idx) + 1 as unit_index,
    p.parcel:TrackID::varchar as track_id,
    array_to_string(u.unit:ShipmentUnitReference, '|') as shipment_unit_reference,
    p.parcel:ParcelNumber::varchar as parcel_number,
    p.parcel:SSCC::varchar as sscc,
    round(try_cast(u.unit:Weight::varchar as number(10, 3)), 2)::number(8, 2) as weight_kg,
    nullif(transform(u.unit:Service, s -> s:Service:ServiceName), array_construct()) as services,
    u.unit is not null and p.parcel is not null as is_matched,
    m._event_id,
    m._inserted_at
from units u
full outer join parcels p on p.message_id = u.message_id and p.idx = u.idx
join messages m on m.message_id = coalesce(u.message_id, p.message_id)
