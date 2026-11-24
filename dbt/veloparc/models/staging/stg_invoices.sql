-- Invoices and credit notes without technical duplicates or resends; lines kept as an array.
-- related_invoice_number closes gap #1 of the mapping (needed for R9).
select
    RAW_PAYLOAD:invoiceNumber::varchar as invoice_number,
    RAW_PAYLOAD:docType::varchar as doc_type,
    try_to_date(RAW_PAYLOAD:issueDate::varchar) as issue_date,
    RAW_PAYLOAD:billingPeriod::varchar as billing_period,
    RAW_PAYLOAD:relatedInvoiceNumber::varchar as related_invoice_number,
    RAW_PAYLOAD:customer:contactId::varchar as contact_id,
    upper(replace(RAW_PAYLOAD:customer:vatNumber::varchar, ' ', '')) as vat_number,
    RAW_PAYLOAD:currency::varchar as currency,
    try_cast(RAW_PAYLOAD:fxRateToEur::varchar as number(12, 6)) as fx_rate_to_eur,
    try_cast(RAW_PAYLOAD:totals:net::varchar as number(12, 2)) as total_net,
    try_cast(RAW_PAYLOAD:totals:vat::varchar as number(12, 2)) as total_vat,
    try_cast(RAW_PAYLOAD:totals:gross::varchar as number(12, 2)) as total_gross,
    RAW_PAYLOAD:lines as lines,
    {{ bronze_meta() }}
from {{ source('bronze', 'V_RAW_INVOICES') }}
where {{ not_duplicate() }}
