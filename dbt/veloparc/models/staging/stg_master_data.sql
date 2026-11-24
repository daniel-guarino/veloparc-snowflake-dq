-- Master data (customers, depots, partners) from bronze, without technical duplicates or resends.
-- One row per message; entity-specific fields are NULL for the other entities.
select
    RAW_PAYLOAD:entity::varchar as entity,
    -- customer
    RAW_PAYLOAD:contactId::varchar as contact_id,
    RAW_PAYLOAD:groupCustomerId::varchar as group_customer_id,
    RAW_PAYLOAD:segment::varchar as segment,
    upper(replace(RAW_PAYLOAD:vatNumber::varchar, ' ', '')) as vat_number,
    {{ ts_utc('RAW_PAYLOAD:updatedAt::varchar') }} as updated_at,
    -- depot
    RAW_PAYLOAD:gln::varchar as gln,
    RAW_PAYLOAD:type::varchar as depot_type,
    trim(RAW_PAYLOAD:city::varchar) as city,
    trim(RAW_PAYLOAD:postalCode::varchar) as postal_code,
    -- partner
    RAW_PAYLOAD:partnerId::varchar as partner_id,
    RAW_PAYLOAD:baseDepotGln::varchar as base_depot_gln,
    RAW_PAYLOAD:zones as zones,
    -- common
    trim(RAW_PAYLOAD:name::varchar) as name,
    RAW_PAYLOAD:countryCode::varchar as country_code,
    {{ bronze_meta() }}
from {{ source('bronze', 'V_RAW_MASTER_DATA') }}
where {{ not_duplicate() }}
