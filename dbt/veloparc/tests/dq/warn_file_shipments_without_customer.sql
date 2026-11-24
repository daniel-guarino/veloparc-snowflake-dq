-- Diagnosis of the 462 shipments whose customer is not in master data (silver relationships warning): the same
-- check restricted to shipments loaded from landing files. 0 rows = they all came through the Kafka connector on
-- 2026-11-01, the day whose master data files were never uploaded (known gap, ADR 0004), not a data defect.
{{ config(severity='warn') }}
select s.message_id, s.contact_id, s.shipping_date
from {{ ref('veloship_shipment') }} s
where s._file_name is not null
  and s.contact_id not in (select contact_id from {{ ref('customer') }})
