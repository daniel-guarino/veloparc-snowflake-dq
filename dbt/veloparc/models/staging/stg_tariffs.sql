-- Customer tariffs from bronze, without technical duplicates or resends; weight bands kept as an array.
select
    RAW_PAYLOAD:tariffId::varchar as tariff_id,
    RAW_PAYLOAD:contactId::varchar as contact_id,
    RAW_PAYLOAD:product::varchar as product,
    RAW_PAYLOAD:zone::varchar as zone_code,
    RAW_PAYLOAD:currency::varchar as currency,
    try_cast(RAW_PAYLOAD:fuelSurchargePct::varchar as number(5, 2)) as fuel_surcharge_pct,
    try_to_date(RAW_PAYLOAD:validFrom::varchar) as valid_from,
    try_to_date(RAW_PAYLOAD:validTo::varchar) as valid_to,
    RAW_PAYLOAD:weightBands as weight_bands,
    {{ bronze_meta() }}
from {{ source('bronze', 'V_RAW_TARIFFS') }}
where {{ not_duplicate() }}
