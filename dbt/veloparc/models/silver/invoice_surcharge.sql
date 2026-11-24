-- Surcharges of each line of each invoice's latest version, 1-based surcharge_index (delete + insert per invoice).
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
    s.index + 1 as surcharge_index,
    s.value:type::varchar as surcharge_type,
    try_cast(s.value:amount::varchar as number(12, 2)) as amount,
    i._event_id,
    i._inserted_at
from invoices i,
    lateral flatten(input => i.lines) l,
    lateral flatten(input => l.value:surcharges) s
