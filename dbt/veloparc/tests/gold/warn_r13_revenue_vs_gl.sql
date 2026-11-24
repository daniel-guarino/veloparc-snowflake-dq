-- R13 (contracts/README.md): per country and month, invoice revenue in EUR = GL revenue, within 0.5%.
-- A warning: the generator injects positive credit notes in one country and month (2-3% off, R9/R13), and this
-- test should show exactly that. Phase 9 turns it into a DQ rule with an owner.
{{ config(severity='warn') }}
with invoices as (
    select billing_country as country_code, billing_period as period, sum(amount_eur) as invoice_eur
    from {{ ref('fct_invoice_line') }}
    group by 1, 2
),
ledger as (
    select country_code, period, sum(amount_eur) as ledger_eur
    from {{ ref('fct_gl_revenue') }}
    group by 1, 2
)
select
    coalesce(l.country_code, i.country_code) as country_code,
    coalesce(l.period, i.period) as period,
    l.ledger_eur,
    i.invoice_eur,
    round(100 * (coalesce(i.invoice_eur, 0) - coalesce(l.ledger_eur, 0)) / nullif(abs(l.ledger_eur), 0), 2) as diff_pct
from ledger l
full outer join invoices i on i.country_code = l.country_code and i.period = l.period
where abs(coalesce(i.invoice_eur, 0) - coalesce(l.ledger_eur, 0)) > abs(coalesce(l.ledger_eur, 0)) * 0.005
