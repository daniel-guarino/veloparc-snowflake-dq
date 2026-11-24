-- R7: netAmount = basePrice + surcharges per line; totals.net = sum of lines and gross = net + vat per invoice.
with surcharges as (
    select invoice_number, line_no, sum(amount) as total
    from {{ ref('invoice_surcharge') }}
    group by 1, 2
),
line_totals as (
    select invoice_number, sum(net_amount) as lines_net
    from {{ ref('invoice_line') }}
    group by 1
)
select {{ dq_violation('R7', "l.invoice_number || '#' || l.line_no", 'invoice', 'left(l.invoice_number, 2)', 'i.issue_date',
        "'netAmount ' || l.net_amount || ' != basePrice + surcharges ' || (l.base_price + coalesce(s.total, 0))",
        'i._event_id', 'i._source_txn_id', 'i._file_name', 'i._kafka_offset') }}
from {{ ref('invoice_line') }} l
join {{ ref('invoice') }} i on i.invoice_number = l.invoice_number
left join surcharges s on s.invoice_number = l.invoice_number and s.line_no = l.line_no
where l.net_amount <> l.base_price + coalesce(s.total, 0)
union all
select {{ dq_violation('R7', 'i.invoice_number', 'invoice', 'left(i.invoice_number, 2)', 'i.issue_date',
        "'totals do not add up'", 'i._event_id', 'i._source_txn_id', 'i._file_name', 'i._kafka_offset') }}
from {{ ref('invoice') }} i
left join line_totals t on t.invoice_number = i.invoice_number
where not equal_null(i.total_net, t.lines_net) or i.total_net + i.total_vat <> i.total_gross
