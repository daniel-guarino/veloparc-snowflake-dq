-- R12: totalCost = stops x costPerStop; stops <= deliveredParcels.
select {{ dq_violation('R12', 'settlement_id', 'partner_settlement', 'left(zone_code, 2)', "to_date(period || '-01')",
        "iff(round(stops * cost_per_stop, 2) <> total_cost, 'totalCost != stops x costPerStop', 'stops > deliveredParcels')",
        '_event_id', '_source_txn_id', '_file_name', '_kafka_offset') }}
from {{ ref('partner_settlement') }}
where round(stops * cost_per_stop, 2) <> total_cost or stops > delivered_parcels
