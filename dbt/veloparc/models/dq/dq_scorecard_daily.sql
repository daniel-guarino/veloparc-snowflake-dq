-- Scorecard: per data product, source, country, DQ dimension and day, the share of records without an issue
-- (1 - records with an issue / records evaluated), against the catalog threshold of that dimension.
-- Records evaluated = the source's silver records of that country and day; records with an issue = distinct
-- business keys in dq_issue (capped at the records evaluated: some rules work on line or group keys). Rules with no
-- event date (R16) are not in the daily scorecard; they are in dq_issue.
with evaluated as (
    select 'tracking_event' as source, consignee_country as country_code, event_time_utc::date as day, count(*) as records
    from ({{ events_with_country() }}) group by 1, 2, 3
    union all
    select 'veloship_shipment', consignee_country, shipping_date, count(*) from {{ ref('veloship_shipment') }} group by 1, 2, 3
    union all
    select 'invoice', left(invoice_number, 2), issue_date, count(*) from {{ ref('invoice') }} group by 1, 2, 3
    union all
    select 'partner_settlement', left(zone_code, 2), to_date(period || '-01'), count(*) from {{ ref('partner_settlement') }} group by 1, 2, 3
    union all
    select 'tariff', left(zone_code, 2), valid_from, count(*) from {{ ref('tariff') }} group by 1, 2, 3
    union all
    select 'crm_object', country_code, created_at::date, count(*) from {{ ref('crm_lead') }} group by 1, 2, 3
    union all
    select 'gl_extract', country_code, to_date(period || '-01'), count(distinct period) from {{ ref('gl_balance') }} group by 1, 2, 3
    union all
    select 'master_data', country_code, day, count(*) from (
        select country_code, updated_at::date as day from {{ ref('customer') }}
        union all select country_code, _source_ts::date from {{ ref('depot') }}
        union all select country_code, _source_ts::date from {{ ref('partner') }}
    ) group by 1, 2, 3
),
dimensions as (
    select dimension, min(threshold_pct) as threshold_pct from {{ ref('dq_rule_catalog') }} group by 1
),
issues as (
    select source, country_code, event_date as day, dimension, count(distinct business_key) as records_with_issue
    from {{ ref('dq_issue') }}
    group by 1, 2, 3, 4
),
grid as (
    -- every source x day evaluated, for every dimension that has a rule on that source
    select e.source, e.country_code, e.day, e.records, d.dimension, d.threshold_pct
    from evaluated e
    join (select distinct trim(f.value::varchar) as source, c.dimension
          from {{ ref('dq_rule_catalog') }} c, lateral flatten(input => split(c.sources, ';')) f) s on s.source = e.source
    join dimensions d on d.dimension = s.dimension
)
select
    case g.source
        when 'tracking_event' then 'Parcel operations'
        when 'veloship_shipment' then 'Parcel operations'
        when 'invoice' then 'Revenue and billing'
        when 'gl_extract' then 'Revenue and billing'
        when 'partner_settlement' then 'Partner costs'
        when 'tariff' then 'Customer pricing'
        when 'master_data' then 'Customer pricing'
        when 'crm_object' then 'Sales pipeline'
    end as data_product,
    g.source,
    g.country_code,
    g.day,
    g.dimension,
    g.records as records_evaluated,
    least(coalesce(i.records_with_issue, 0), g.records) as records_with_issue,
    round(100 * (1 - least(coalesce(i.records_with_issue, 0), g.records) / g.records), 3) as pct_valid,
    g.threshold_pct,
    case
        when pct_valid >= g.threshold_pct then 'green'
        when pct_valid >= g.threshold_pct - 1 then 'amber'
        else 'red'
    end as status
from grid g
left join issues i
    on i.source = g.source and equal_null(i.country_code, g.country_code) and i.day = g.day and i.dimension = g.dimension
