-- Accumulating snapshot, one row per parcel (track_id): shipment attributes, delivery zone, first PICKUP and first
-- DELIVERED scan. Units without a TrackID (R1) have no parcel key and stay in silver.veloship_parcel.
-- Track IDs that appear only in events or invoices (R2) get an inferred row (is_inferred, mapping decision #5).
with parcels as (
    select p.track_id, p.parcel_number, p.sscc, p.weight_kg,
           s.contact_id, s.shipping_date, s.product, s.incoterm_code, s.consignee_country, s.consignee_zip
    from {{ ref('veloship_parcel') }} p
    join {{ ref('veloship_shipment') }} s on s.message_id = p.message_id
    where p.track_id is not null
),
invoiced as (
    -- the parcel's invoice (not a credit note): zone and customer for inferred parcels
    select l.track_id, any_value(l.zone_code) as zone_code, any_value(i.contact_id) as contact_id,
           min(i.issue_date) as issue_date
    from {{ ref('invoice_line') }} l
    join {{ ref('invoice') }} i on i.invoice_number = l.invoice_number
    where i.doc_type = 'INVOICE' and l.track_id is not null
    group by 1
),
scans as (
    select track_id,
           min(iff(status = 'PICKUP', event_time_utc, null)) as pickup_ts,
           min(iff(status = 'DELIVERED', event_time_utc, null)) as delivered_ts
    from {{ ref('tracking_event') }}
    group by 1
),
inferred as (
    select track_id from scans
    union
    select track_id from invoiced
    minus
    select track_id from parcels
),
all_parcels as (
    select track_id, parcel_number, sscc, weight_kg, contact_id, shipping_date, product, incoterm_code,
           consignee_country, consignee_zip, false as is_inferred
    from parcels
    union all
    select f.track_id, null, null, null, inv.contact_id, null, null, null, null, null, true
    from inferred f
    left join invoiced inv on inv.track_id = f.track_id
),
postal_zone as (
    -- delivery zone from the consignee address: the longest postal prefix of the country (gap #2)
    select a.track_id, z.ZONE_CODE as zone_code
    from all_parcels a
    join {{ source('ref', 'POSTAL_ZONE') }} z
        on z.COUNTRY_CODE = a.consignee_country and startswith(a.consignee_zip, z.POSTAL_PREFIX)
    qualify row_number() over (partition by a.track_id order by length(z.POSTAL_PREFIX) desc) = 1
),
zone_sk as (
    select zone_code, min(partner_zone_sk) as partner_zone_sk  -- R16 says one partner per zone
    from {{ ref('dim_partner_zone') }}
    where zone_code <> 'UNKNOWN'
    group by 1
)
select
    a.track_id,
    {{ customer_sk_from('dc') }} as customer_sk,
    coalesce(zs.partner_zone_sk, {{ unknown_sk() }}) as partner_zone_sk,
    {{ date_key('a.shipping_date') }} as shipping_date_key,
    a.parcel_number,
    a.sscc,
    a.product,
    a.incoterm_code,
    a.consignee_country,
    a.weight_kg,
    s.pickup_ts,
    s.delivered_ts,
    s.delivered_ts is not null as is_delivered,
    a.is_inferred
from all_parcels a
left join scans s on s.track_id = a.track_id
left join invoiced inv on inv.track_id = a.track_id
left join postal_zone pz on pz.track_id = a.track_id
left join zone_sk zs on zs.zone_code = coalesce(pz.zone_code, inv.zone_code)
{{ join_customer_at('a.contact_id', 'coalesce(a.shipping_date, inv.issue_date)::timestamp_ntz', 'dc') }}
