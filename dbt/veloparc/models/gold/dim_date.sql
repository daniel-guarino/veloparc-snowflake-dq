-- One row per calendar day, from the first business date in silver to two years after the last; plus the unknown
-- member (date_key -1, 1900-01-01).
with bounds as (
    select min(d) as first_day, dateadd(year, 2, max(d)) as last_day
    from (
        select min(shipping_date) as d from {{ ref('veloship_shipment') }}
        union all select max(shipping_date) from {{ ref('veloship_shipment') }}
        union all select min(issue_date) from {{ ref('invoice') }}
        union all select max(issue_date) from {{ ref('invoice') }}
        union all select min(event_time_utc)::date from {{ ref('tracking_event') }}
        union all select max(event_time_utc)::date from {{ ref('tracking_event') }}
        union all select min(close_date) from {{ ref('crm_opportunity') }}
        union all select min(created_at)::date from {{ ref('crm_lead') }}
    )
),
numbers as (
    select row_number() over (order by seq4()) - 1 as n
    from table(generator(rowcount => 4000))
),
days as (
    select dateadd(day, n.n, b.first_day) as full_date
    from bounds b
    join numbers n on dateadd(day, n.n, b.first_day) <= b.last_day
)
select
    {{ date_key('full_date') }} as date_key,
    full_date,
    year(full_date) as year,
    month(full_date) as month,
    to_char(full_date, 'YYYY-MM') as period,
    weekiso(full_date) as iso_week,
    dayofweekiso(full_date) in (6, 7) as is_weekend
from days
union all
select -1, '1900-01-01'::date, 1900, 1, '1900-01', 1, false
