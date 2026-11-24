-- Weight bands of each tariff's latest version; a new version replaces all its bands (delete + insert).
{{ config(unique_key='tariff_id', incremental_strategy='delete+insert') }}

with latest as (
    select tariff_id, weight_bands, _event_id, _source_ts, _kafka_ts, _kafka_offset, _inserted_at
    from {{ ref('stg_tariffs') }}
    {% if is_incremental() %}
    where tariff_id in (select tariff_id from {{ ref('stg_tariffs') }} where true {{ new_rows_only() }})
    {% endif %}
    qualify row_number() over (partition by tariff_id order by {{ latest_first() }}) = 1
)
select
    l.tariff_id,
    w.index + 1 as band_index,
    try_cast(w.value:fromKg::varchar as number(6, 2)) as from_kg,
    try_cast(w.value:toKg::varchar as number(6, 2)) as to_kg,
    try_cast(w.value:price::varchar as number(10, 2)) as price,
    l._event_id,
    l._inserted_at
from latest l, lateral flatten(input => l.weight_bands) w
