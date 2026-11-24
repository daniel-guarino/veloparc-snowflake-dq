-- One row per invoice or credit note (latest version). related_invoice_number added (mapping gap #1, R9).
{{ config(unique_key='invoice_number', incremental_strategy='merge') }}

select
    invoice_number, doc_type, issue_date, billing_period, related_invoice_number, contact_id, vat_number,
    currency, fx_rate_to_eur, total_net, total_vat, total_gross,
    _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
from {{ ref('stg_invoices') }}
where true {{ new_rows_only() }}
qualify row_number() over (partition by invoice_number order by {{ latest_first() }}) = 1
