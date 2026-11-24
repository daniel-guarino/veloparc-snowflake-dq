-- R14: bands start at 0, are contiguous and get more expensive; validTo > validFrom; no overlapping validity of
-- two tariffs for the same (customer, product, zone) (the later one is reported, as in the generator).
with bands as (
    select tariff_id, band_index, from_kg, to_kg, price,
           lead(from_kg) over (partition by tariff_id order by band_index) as next_from,
           lead(price) over (partition by tariff_id order by band_index) as next_price
    from {{ ref('tariff_weight_band') }}
),
bad_bands as (
    select tariff_id, 'first band does not start at 0' as details from bands where band_index = 1 and from_kg <> 0
    union
    select tariff_id, 'bands not contiguous or price not increasing' from bands
    where next_from is not null and (to_kg <> next_from or price >= next_price)
),
scoped as (
    select t.*, lag(valid_to) over (partition by contact_id, product, zone_code order by valid_from, tariff_id) as prev_to,
           lag(tariff_id) over (partition by contact_id, product, zone_code order by valid_from, tariff_id) as prev_id
    from {{ ref('tariff') }} t
)
select {{ dq_violation('R14', 't.tariff_id', 'tariff', 'left(t.zone_code, 2)', 't.valid_from', 'b.details',
        't._event_id', 't._source_txn_id', 't._file_name', 't._kafka_offset') }}
from bad_bands b
join {{ ref('tariff') }} t on t.tariff_id = b.tariff_id
union all
select {{ dq_violation('R14', 'tariff_id', 'tariff', 'left(zone_code, 2)', 'valid_from', "'validTo <= validFrom'",
        '_event_id', '_source_txn_id', '_file_name', '_kafka_offset') }}
from {{ ref('tariff') }}
where valid_to is not null and valid_to <= valid_from
union all
select {{ dq_violation('R14', 'tariff_id', 'tariff', 'left(zone_code, 2)', 'valid_from', "'validity overlaps ' || prev_id",
        '_event_id', '_source_txn_id', '_file_name', '_kafka_offset') }}
from scoped
where prev_id is not null and (prev_to is null or prev_to >= valid_from)
