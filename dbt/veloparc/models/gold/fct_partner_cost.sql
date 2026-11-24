-- One row per partner x zone x month. total_cost_eur uses the month's average rate from REF.FX_RATE_DAILY
-- (gap #1); EUR settlements as they are.
with fx_monthly as (
    select CURRENCY as currency, to_char(RATE_DATE, 'YYYY-MM') as period, avg(RATE_TO_EUR) as rate_to_eur
    from {{ source('ref', 'FX_RATE_DAILY') }}
    group by 1, 2
)
select
    ps.settlement_id,
    coalesce(dpz.partner_zone_sk, {{ unknown_sk() }}) as partner_zone_sk,
    ps.period,
    ps.stops,
    ps.delivered_parcels,
    ps.failed_attempts,
    ps.currency,
    ps.total_cost as total_cost_local,
    iff(ps.currency = 'EUR', ps.total_cost, round(ps.total_cost * fx.rate_to_eur, 2)) as total_cost_eur
from {{ ref('partner_settlement') }} ps
left join {{ ref('dim_partner_zone') }} dpz on dpz.partner_id = ps.partner_id and dpz.zone_code = ps.zone_code
left join fx_monthly fx on fx.currency = ps.currency and fx.period = ps.period
