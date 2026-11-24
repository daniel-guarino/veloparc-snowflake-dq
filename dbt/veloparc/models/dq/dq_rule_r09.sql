-- R9: credit notes have negative lines and totals and reference an existing invoice.
select {{ dq_violation('R9', "l.invoice_number || '#' || l.line_no", 'invoice', 'left(l.invoice_number, 2)', 'i.issue_date',
        "'credit note line not negative'", 'i._event_id', 'i._source_txn_id', 'i._file_name', 'i._kafka_offset') }}
from {{ ref('invoice_line') }} l
join {{ ref('invoice') }} i on i.invoice_number = l.invoice_number
where i.doc_type = 'CREDIT_NOTE' and l.net_amount >= 0
union all
select {{ dq_violation('R9', 'i.invoice_number', 'invoice', 'left(i.invoice_number, 2)', 'i.issue_date',
        "'credit note not negative or related invoice unknown'", 'i._event_id', 'i._source_txn_id', 'i._file_name', 'i._kafka_offset') }}
from {{ ref('invoice') }} i
where i.doc_type = 'CREDIT_NOTE'
  and (i.total_net >= 0 or coalesce(i.related_invoice_number, '') not in (
        select invoice_number from {{ ref('invoice') }} where doc_type = 'INVOICE'))
