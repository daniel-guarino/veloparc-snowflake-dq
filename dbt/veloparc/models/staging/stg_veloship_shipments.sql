-- VeloShip shipment messages without technical duplicates or resends; units and parcels kept as arrays.
select
    RAW_PAYLOAD:messageId::varchar as message_id,
    {{ ts_utc('RAW_PAYLOAD:receivedAt::varchar') }} as received_at,
    array_to_string(RAW_PAYLOAD:request:Shipment:ShipmentReference, '|') as shipment_reference,
    try_to_date(RAW_PAYLOAD:request:Shipment:ShippingDate::varchar) as shipping_date,
    RAW_PAYLOAD:request:Shipment:IncotermCode::varchar as incoterm_code,
    upper(trim(RAW_PAYLOAD:request:Shipment:Product::varchar)) as product,
    trim(RAW_PAYLOAD:request:Shipment:Middleware::varchar) as middleware,
    RAW_PAYLOAD:request:Shipment:Shipper:ContactID::varchar as contact_id,
    RAW_PAYLOAD:request:Shipment:Consignee:ConsigneeID::varchar as consignee_id,
    trim(RAW_PAYLOAD:request:Shipment:Consignee:Address:Name1::varchar) as consignee_name,
    upper(RAW_PAYLOAD:request:Shipment:Consignee:Address:CountryCode::varchar) as consignee_country,
    trim(RAW_PAYLOAD:request:Shipment:Consignee:Address:ZIPCode::varchar) as consignee_zip,
    trim(RAW_PAYLOAD:request:Shipment:Consignee:Address:City::varchar) as consignee_city,
    RAW_PAYLOAD:request:Shipment:ShipmentUnit as shipment_units,
    RAW_PAYLOAD:response:CreatedShipment:ParcelData as parcel_data,
    {{ bronze_meta() }}
from {{ source('bronze', 'V_RAW_VELOSHIP_SHIPMENTS') }}
where {{ not_duplicate() }}
