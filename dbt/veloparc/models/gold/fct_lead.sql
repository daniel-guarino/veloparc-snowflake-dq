-- One row per lead (latest state); country_code is the ISO-normalised value from silver.
select
    lead_id,
    {{ date_key('created_at::date') }} as created_date_key,
    converted_opportunity_id,
    country_code,
    lead_source,
    status
from {{ ref('crm_lead') }}
