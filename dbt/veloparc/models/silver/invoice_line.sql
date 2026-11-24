-- Lines of each invoice's latest version; a new version replaces all its lines (delete + insert).
{{ config(unique_key='invoice_number', incremental_strategy='delete+insert') }}

with invoices as (
    select invoice_number, lines, _event_id, _inserted_at
    from {{ ref('stg_invoices') }}
    where true {{ new_rows_only() }}
    qualify row_number() over (partition by invoice_number order by {{ latest_first() }}) = 1
)
select
    i.invoice_number,
    try_cast(l.value:lineNo::varchar as integer) as line_no,
    l.value:trackId::varchar as track_id,
    l.value:tariffId::varchar as tariff_id,
    l.value:product::varchar as product,
    round(try_cast(l.value:weightKg::varchar as number(10, 3)), 2)::number(8, 2) as weight_kg,
    l.value:zone::varchar as zone_code,
    try_cast(l.value:basePrice::varchar as number(12, 2)) as base_price,
    try_cast(l.value:netAmount::varchar as number(12, 2)) as net_amount,
    i._event_id,
    i._inserted_at
from invoices i, lateral flatten(input => i.lines) l
