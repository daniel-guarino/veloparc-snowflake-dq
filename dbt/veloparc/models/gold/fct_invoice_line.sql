-- One row per invoice line, amounts in the invoice currency and in EUR (fx rate of the issue date, R10).
-- billing_country is the invoice number prefix (the issuing Veloparc company): the R13 key, as in the generator.
with fuel as (
    select invoice_number, line_no, sum(amount) as fuel_amount
    from {{ ref('invoice_surcharge') }}
    where surcharge_type = 'FUEL'
    group by 1, 2
)
select
    l.invoice_number,
    l.line_no,
    {{ customer_sk_from('dc') }} as customer_sk,
    coalesce(dt.tariff_sk, {{ unknown_sk() }}) as tariff_sk,
    l.track_id,
    {{ date_key('i.issue_date') }} as invoice_date_key,
    i.doc_type,
    i.related_invoice_number,
    i.billing_period,
    left(i.invoice_number, 2) as billing_country,
    i.currency,
    l.net_amount as amount_local,
    i.fx_rate_to_eur as fx_rate_eur,
    round(l.net_amount * i.fx_rate_to_eur, 2) as amount_eur,
    round(coalesce(f.fuel_amount, 0) * i.fx_rate_to_eur, 2) as fuel_surcharge_eur
from {{ ref('invoice_line') }} l
join {{ ref('invoice') }} i on i.invoice_number = l.invoice_number
left join fuel f on f.invoice_number = l.invoice_number and f.line_no = l.line_no
left join {{ ref('dim_tariff') }} dt
    on dt.tariff_id = l.tariff_id
    and l.weight_kg > dt.weight_from_kg
    and l.weight_kg <= dt.weight_to_kg
{{ join_customer_at('i.contact_id', 'i.issue_date::timestamp_ntz', 'dc') }}
