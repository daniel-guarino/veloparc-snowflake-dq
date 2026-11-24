-- One row per company x month x GL account, from the latest complete GL batch (silver.gl_balance). Already in EUR.
select company_code, period, gl_account, country_code, amount_eur
from {{ ref('gl_balance') }}
