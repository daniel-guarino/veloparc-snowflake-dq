-- SCD1, one row per partner x zone, with the partner's header attributes.
select
    {{ surrogate_key(['pz.partner_id', 'pz.zone_code']) }} as partner_zone_sk,
    pz.partner_id,
    pz.zone_code,
    pa.partner_name,
    pa.country_code,
    pa.base_depot_gln
from {{ ref('partner_zone') }} pz
join {{ ref('partner') }} pa on pa.partner_id = pz.partner_id
union all
select {{ unknown_sk() }}, 'UNKNOWN', 'UNKNOWN', 'Unknown partner', null, null
