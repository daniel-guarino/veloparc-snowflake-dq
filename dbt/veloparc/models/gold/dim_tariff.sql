-- One row per tariff x weight band. customer_sk: the customer version valid at the tariff's valid_from.
-- Band rule (mapping decision #10): weight_from_kg < weight <= weight_to_kg.
select
    {{ surrogate_key(['t.tariff_id', 'wb.band_index']) }} as tariff_sk,
    t.tariff_id,
    wb.band_index,
    {{ customer_sk_from('dc') }} as customer_sk,
    t.product,
    t.zone_code,
    wb.from_kg as weight_from_kg,
    wb.to_kg as weight_to_kg,
    wb.price,
    t.currency,
    t.fuel_surcharge_pct,
    t.valid_from,
    t.valid_to
from {{ ref('tariff') }} t
join {{ ref('tariff_weight_band') }} wb on wb.tariff_id = t.tariff_id
{{ join_customer_at('t.contact_id', 't.valid_from::timestamp_ntz', 'dc') }}
union all
select {{ unknown_sk() }}, 'UNKNOWN', null, {{ unknown_sk() }}, null, null, null, null, null, null, null, null, null
