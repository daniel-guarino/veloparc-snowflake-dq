-- R8: the line's tariff is valid on the shipping date; basePrice = band price for the parcel weight
-- (negative on credit notes); FUEL surcharge = basePrice x fuelSurchargePct / 100.
with lines as (
    select l.invoice_number, l.line_no, l.track_id, l.tariff_id, l.base_price, i.doc_type, i.issue_date,
           s.shipping_date, p.weight_kg, i._event_id, i._source_txn_id, i._file_name, i._kafka_offset
    from {{ ref('invoice_line') }} l
    join {{ ref('invoice') }} i on i.invoice_number = l.invoice_number
    join {{ ref('veloship_parcel') }} p on p.track_id = l.track_id
    join {{ ref('veloship_shipment') }} s on s.message_id = p.message_id
),
fuel as (
    -- the line's first FUEL surcharge, as the generator's checker reads it
    select invoice_number, line_no, amount
    from {{ ref('invoice_surcharge') }}
    where surcharge_type = 'FUEL'
    qualify row_number() over (partition by invoice_number, line_no order by surcharge_index) = 1
),
checked as (
    select
        l.*,
        t.tariff_id is not null and l.shipping_date between t.valid_from and coalesce(t.valid_to, '9999-12-31') as tariff_valid,
        t.fuel_surcharge_pct,
        wb.price as band_price,
        f.amount as fuel_amount
    from lines l
    left join {{ ref('tariff') }} t on t.tariff_id = l.tariff_id
    left join {{ ref('tariff_weight_band') }} wb
        on wb.tariff_id = l.tariff_id and l.weight_kg > wb.from_kg and l.weight_kg <= wb.to_kg
    left join fuel f on f.invoice_number = l.invoice_number and f.line_no = l.line_no
)
select {{ dq_violation('R8', "invoice_number || '#' || line_no", 'invoice', 'left(invoice_number, 2)', 'issue_date',
        "case
            when not tariff_valid then 'tariff ' || tariff_id || ' not valid on ship date'
            when band_price is null or base_price <> iff(doc_type = 'CREDIT_NOTE', -1, 1) * band_price
                then 'basePrice ' || base_price || ' != band price ' || coalesce(band_price::varchar, 'none')
            else 'FUEL surcharge != basePrice x fuelSurchargePct'
        end", '_event_id', '_source_txn_id', '_file_name', '_kafka_offset') }}
from checked
where not tariff_valid
   or band_price is null
   or base_price <> iff(doc_type = 'CREDIT_NOTE', -1, 1) * band_price
   or (fuel_amount is not null and fuel_amount <> round(base_price * coalesce(fuel_surcharge_pct, 0) / 100, 2))
