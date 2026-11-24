-- Monthly partner settlements without technical duplicates or resends.
select
    RAW_PAYLOAD:settlementId::varchar as settlement_id,
    RAW_PAYLOAD:partnerId::varchar as partner_id,
    RAW_PAYLOAD:zone::varchar as zone_code,
    RAW_PAYLOAD:period::varchar as period,
    RAW_PAYLOAD:currency::varchar as currency,
    try_cast(RAW_PAYLOAD:stops::varchar as integer) as stops,
    try_cast(RAW_PAYLOAD:deliveredParcels::varchar as integer) as delivered_parcels,
    try_cast(RAW_PAYLOAD:failedAttempts::varchar as integer) as failed_attempts,
    try_cast(RAW_PAYLOAD:costPerStop::varchar as number(8, 2)) as cost_per_stop,
    try_cast(RAW_PAYLOAD:totalCost::varchar as number(12, 2)) as total_cost,
    {{ bronze_meta() }}
from {{ source('bronze', 'V_RAW_PARTNER_SETTLEMENTS') }}
where {{ not_duplicate() }}
