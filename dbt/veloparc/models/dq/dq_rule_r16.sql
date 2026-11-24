-- R16: each zone (of the postal-zone reference or served by a partner) has exactly one partner.
with zones as (
    select ZONE_CODE as zone_code, COUNTRY_CODE as country_code from {{ source('ref', 'POSTAL_ZONE') }}
    union
    select zone_code, left(zone_code, 2) from {{ ref('partner_zone') }}
),
partners as (
    select zone_code, count(distinct partner_id) as n, listagg(distinct partner_id, ', ') as ids
    from {{ ref('partner_zone') }}
    group by 1
)
select {{ dq_violation('R16', 'z.zone_code', 'master_data', 'z.country_code', 'null',
        "'partners: ' || coalesce(p.ids, 'none')") }}
from zones z
left join partners p on p.zone_code = z.zone_code
where z.country_code in (select distinct COUNTRY_CODE from {{ source('ref', 'POSTAL_ZONE') }})
  and coalesce(p.n, 0) <> 1
