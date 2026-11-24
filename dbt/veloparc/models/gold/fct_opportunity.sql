-- One row per opportunity (latest state). customer_sk: current version; NULL while the opportunity has no customer.
-- amount_eur: rate of the close date, or the last rate available before it (close dates can be in the future).
with fx_last as (
    select CURRENCY as currency, max(RATE_DATE) as last_rate_date
    from {{ source('ref', 'FX_RATE_DAILY') }}
    group by 1
)
select
    o.opportunity_id,
    case when o.contact_id is null then null else coalesce(dc.customer_sk, {{ unknown_sk() }}) end as customer_sk,
    {{ date_key('o.close_date') }} as close_date_key,
    o.stage,
    o.currency,
    o.amount as amount_local,
    iff(o.currency = 'EUR' or o.currency is null, o.amount, round(o.amount * fx.RATE_TO_EUR, 2)) as amount_eur,
    o.country_code,
    o.expected_parcels_month
from {{ ref('crm_opportunity') }} o
left join {{ ref('dim_customer') }} dc on dc.contact_id = o.contact_id and dc.is_current
left join fx_last fl on fl.currency = o.currency
left join {{ source('ref', 'FX_RATE_DAILY') }} fx
    on fx.CURRENCY = o.currency and fx.RATE_DATE = least(o.close_date, fl.last_rate_date)
