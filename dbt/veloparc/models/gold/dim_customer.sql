-- SCD2 from the versions kept in silver.customer: valid_from = updated_at, valid_to = next version's updated_at.
select
    {{ surrogate_key(['contact_id', 'updated_at']) }} as customer_sk,
    contact_id,
    group_customer_id,
    customer_name,
    country_code,
    segment,
    vat_number,
    updated_at as valid_from,
    lead(updated_at) over (partition by contact_id order by updated_at) as valid_to,
    valid_to is null as is_current,
    row_number() over (partition by contact_id order by updated_at) as version_number
from {{ ref('customer') }}
union all
select {{ unknown_sk() }}, 'UNKNOWN', null, 'Unknown customer', null, null, null,
       '1900-01-01'::timestamp_ntz, null, true, 1
