-- R13: ledger revenue per country and month = invoice revenue in EUR (sum of net x fxRateToEur), within 0.5%.
-- Invoice country = invoice number prefix (the issuing company), as in the generator's checker.
with invoices as (
    select left(invoice_number, 2) as country_code, billing_period as period, sum(round(total_net * fx_rate_to_eur, 2)) as invoice_eur
    from {{ ref('invoice') }}
    group by 1, 2
),
ledger as (
    select country_code, period, sum(amount_eur) as ledger_eur
    from {{ ref('gl_balance') }}
    group by 1, 2
)
select {{ dq_violation('R13', "l.country_code || '|' || l.period", 'gl_extract', 'l.country_code', "to_date(l.period || '-01')",
        "'ledger ' || l.ledger_eur || ' vs invoices ' || coalesce(i.invoice_eur, 0)
         || ' (' || round(100 * (coalesce(i.invoice_eur, 0) - l.ledger_eur) / nullif(abs(l.ledger_eur), 0), 2) || '%)'") }}
from ledger l
left join invoices i on i.country_code = l.country_code and i.period = l.period
where abs(coalesce(i.invoice_eur, 0) - l.ledger_eur) > abs(l.ledger_eur) * 0.005
