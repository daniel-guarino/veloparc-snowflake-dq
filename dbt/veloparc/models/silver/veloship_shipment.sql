-- One row per VeloShip shipment message. Consignee street and number stay in bronze (mapping gap #2, PII).
{{ config(unique_key='message_id', incremental_strategy='merge') }}

select
    message_id, shipment_reference, shipping_date, incoterm_code, product, middleware, contact_id,
    consignee_id, consignee_name, consignee_country, consignee_zip, consignee_city, received_at,
    _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
from {{ ref('stg_veloship_shipments') }}
where true {{ new_rows_only() }}
qualify row_number() over (partition by message_id order by {{ latest_first() }}) = 1
