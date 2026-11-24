-- R10: fxRateToEur = 1 for EUR invoices; otherwise the rate of the issue date in REF.FX_RATE_DAILY.
select {{ dq_violation('R10', 'i.invoice_number', 'invoice', 'left(i.invoice_number, 2)', 'i.issue_date',
        "'fxRateToEur ' || i.fx_rate_to_eur || ' != ' || coalesce(iff(i.currency = 'EUR', 1, fx.RATE_TO_EUR)::varchar, 'none')",
        'i._event_id', 'i._source_txn_id', 'i._file_name', 'i._kafka_offset') }}
from {{ ref('invoice') }} i
left join {{ source('ref', 'FX_RATE_DAILY') }} fx on fx.CURRENCY = i.currency and fx.RATE_DATE = i.issue_date
where not equal_null(i.fx_rate_to_eur, iff(i.currency = 'EUR', 1, fx.RATE_TO_EUR))
