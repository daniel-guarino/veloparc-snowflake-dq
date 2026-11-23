# Silver → Gold (Star Schema) Field Mapping

Markdown version of `VLP_Silver_to_Gold_Field_Mapping.xlsx` (generated 2026-11-22). Sources: `docs/data-model/gold_star_schema.dbml`, `docs/data-model/silver_model.dbml`, `docs/data-model/source_to_silver_mapping.md`, `contracts/README.md` (R1–R16). Mapping types: Direct · Calculated · Derived · Lookup (SK) · Surrogate Key · Aggregated · Generated.

Aliases: p = veloship_parcel · s = veloship_shipment · e = tracking_event · i = invoice · l = invoice_line · sc = invoice_surcharge · o = crm_opportunity · ld = crm_lead · ps = partner_settlement · t = tariff · wb = tariff_weight_band · g = gl_balance · c = customer · dp = depot · pa = partner · pz = partner_zone · dc/dd/dpz/dt = gold dim_customer/dim_depot/dim_partner_zone/dim_tariff.

## Table overview

| Gold Table | Type | Grain | Silver Sources | SCD / History | Load Strategy |
|---|---|---|---|---|---|
| dim_date | Dimension | One row per calendar day | (generated) – range from silver business dates | Static | Generate once; extend yearly. |
| dim_customer | Dimension | One row per customer version | silver.customer | SCD2 (valid_from / valid_to / is_current) | MERGE new (contact_id, updated_at) versions; recompute valid_to/is_current for affected contact_ids. |
| dim_depot | Dimension | One row per depot (GLN) | silver.depot | SCD1 | MERGE on depot_gln. |
| dim_partner_zone | Dimension | One row per partner × zone | silver.partner_zone + silver.partner | SCD1 | MERGE on (partner_id, zone_code). |
| dim_tariff | Dimension | One row per tariff × weight band | silver.tariff + silver.tariff_weight_band | Type 0 per tariff_id (validity carried as attributes) | MERGE on (tariff_id, band_index). |
| fct_parcel | Fact (accumulating snapshot) | One row per parcel (track_id) | silver.veloship_parcel + silver.veloship_shipment + silver.tracking_event + silver.invoice_line | Milestones updated as events arrive | MERGE on track_id for parcels with new shipments/events/invoices (stream on silver). |
| fct_parcel_event | Fact (transaction) | One row per scan event | silver.tracking_event | Insert-only | Incremental insert on event_id (_loaded_at watermark or stream). |
| fct_invoice_line | Fact (transaction) | One row per invoice line | silver.invoice + silver.invoice_line + silver.invoice_surcharge | Insert-only | Incremental insert on (invoice_number, line_no). |
| fct_partner_cost | Fact (periodic snapshot) | One row per partner × zone × month | silver.partner_settlement | Replace per period | MERGE on settlement_id. |
| fct_gl_revenue | Fact (periodic snapshot) | One row per company × month × GL account | silver.gl_balance | Replace per (company_code, period) | DELETE + INSERT per (company_code, period) batch. |
| fct_opportunity | Fact (accumulating snapshot) | One row per opportunity (latest state) | silver.crm_opportunity | Overwrite latest state | MERGE on opportunity_id. |
| fct_lead | Fact (accumulating snapshot) | One row per lead (latest state) | silver.crm_lead | Overwrite latest state | MERGE on lead_id. |

## Bus matrix

| Fact | dim_date | dim_customer | dim_depot | dim_partner_zone | dim_tariff | Degenerate / other |
|---|---|---|---|---|---|---|
| fct_parcel | shipping_date_key | customer_sk (at shipping date) |  | partner_zone_sk (delivery zone) |  | track_id, parcel_number, sscc |
| fct_parcel_event | event_date_key (local) |  | depot_gln |  |  | event_id, track_id → fct_parcel |
| fct_invoice_line | invoice_date_key | customer_sk (at issue date) |  |  | tariff_sk | invoice_number, track_id → fct_parcel |
| fct_partner_cost | period (month) |  |  | partner_zone_sk |  | settlement_id |
| fct_gl_revenue | period (month) |  |  |  |  | company_code, gl_account |
| fct_opportunity | close_date_key | customer_sk (current) |  |  |  | opportunity_id |
| fct_lead | created_date_key |  |  |  |  | lead_id, converted_opportunity_id → fct_opportunity |
| (dim) dim_tariff |  | customer_sk (at valid_from) |  |  |  |  |
| (dim) dim_partner_zone |  |  | base_depot_gln |  |  |  |

## dim_date (Dimension)

| Gold Column | Type | Key | Mapping | Silver Source(s) | Join / Lookup Logic | Snowflake Expression | DQ / Notes |
|---|---|---|---|---|---|---|---|
| `date_key` | int | PK | Generated | silver.veloship_shipment.shipping_date, silver.tracking_event.event_time_utc, silver.invoice.issue_date, silver.crm_opportunity.close_date, silver.crm_lead.created_at | Date spine generated with GENERATOR from MIN to MAX of all business dates in silver (plus a forward buffer, e.g. +2 years). | `TO_NUMBER(TO_CHAR(d.full_date,'YYYYMMDD'))` | Smart key YYYYMMDD. Unknown member: date_key = -1, full_date = 1900-01-01 (see Gaps & Decisions). |
| `full_date` | date | NOT NULL | Generated | (generated) | One row per calendar day. | `DATEADD(day, SEQ4(), :start_date)  /* FROM TABLE(GENERATOR(ROWCOUNT => n)) */` |  |
| `year` | int |  | Calculated | (generated) | Calendar year. | `YEAR(full_date)` |  |
| `month` | int |  | Calculated | (generated) | Calendar month 1–12. | `MONTH(full_date)` |  |
| `period` | varchar |  | Calculated | (generated) | 'YYYY-MM' – conformed with silver billing_period / period columns. | `TO_CHAR(full_date,'YYYY-MM')` | Join key for monthly facts (fct_partner_cost, fct_gl_revenue). |
| `iso_week` | int |  | Calculated | (generated) | ISO-8601 week number. | `WEEKISO(full_date)` |  |
| `is_weekend` | boolean |  | Calculated | (generated) | Saturday or Sunday. | `DAYOFWEEKISO(full_date) IN (6,7)` | Volume simulation: weekends at 20%. |

## dim_customer (Dimension)

| Gold Column | Type | Key | Mapping | Silver Source(s) | Join / Lookup Logic | Snowflake Expression | DQ / Notes |
|---|---|---|---|---|---|---|---|
| `customer_sk` | int | PK | Surrogate Key | silver.customer.contact_id, silver.customer.updated_at | One surrogate per customer version (contact_id, updated_at). Stable across reloads. | `SEQ_DIM_CUSTOMER.NEXTVAL  /* assigned on first insert of (contact_id, updated_at) */` | Unknown member: customer_sk = -1, contact_id = 'UNKNOWN', is_current = TRUE. |
| `contact_id` | varchar | NOT NULL | Direct | silver.customer.contact_id | Natural key (VeloShip ContactID). | `c.contact_id` |  |
| `group_customer_id` | varchar |  | Direct | silver.customer.group_customer_id |  | `c.group_customer_id` |  |
| `customer_name` | varchar |  | Direct | silver.customer.customer_name |  | `c.customer_name` |  |
| `country_code` | char(2) |  | Direct | silver.customer.country_code |  | `c.country_code` |  |
| `segment` | varchar |  | Direct | silver.customer.segment |  | `c.segment` |  |
| `vat_number` | varchar |  | Direct | silver.customer.vat_number |  | `c.vat_number` |  |
| `valid_from` | timestamp | NOT NULL | Derived | silver.customer.updated_at | SCD2 start = version timestamp. | `c.updated_at` |  |
| `valid_to` | timestamp |  | Derived | silver.customer.updated_at | SCD2 end = next version's updated_at (exclusive). NULL for current version. | `LEAD(c.updated_at) OVER (PARTITION BY c.contact_id ORDER BY c.updated_at)` | Point-in-time lookup: valid_from ≤ ts < COALESCE(valid_to,'9999-12-31'). |
| `is_current` | boolean | NOT NULL | Derived | silver.customer.updated_at | TRUE for the latest version of each contact_id. | `valid_to IS NULL` |  |

## dim_depot (Dimension)

| Gold Column | Type | Key | Mapping | Silver Source(s) | Join / Lookup Logic | Snowflake Expression | DQ / Notes |
|---|---|---|---|---|---|---|---|
| `depot_gln` | char(13) | PK | Direct | silver.depot.gln | SCD1 (latest version). Natural key used as PK. | `dp.gln` | Unknown member: depot_gln = '0000000000000' (events with GLN not in master data, R6). |
| `depot_name` | varchar |  | Direct | silver.depot.depot_name |  | `dp.depot_name` |  |
| `depot_type` | varchar |  | Direct | silver.depot.depot_type | HUB, DEPOT, PARCELSHOP, LOCKER. | `dp.depot_type` |  |
| `country_code` | char(2) |  | Direct | silver.depot.country_code |  | `dp.country_code` |  |
| `city` | varchar |  | Direct | silver.depot.city |  | `dp.city` | silver.depot.postal_code is not carried to gold. |

## dim_partner_zone (Dimension)

| Gold Column | Type | Key | Mapping | Silver Source(s) | Join / Lookup Logic | Snowflake Expression | DQ / Notes |
|---|---|---|---|---|---|---|---|
| `partner_zone_sk` | int | PK | Surrogate Key | silver.partner_zone.partner_id, silver.partner_zone.zone_code | One surrogate per (partner_id, zone_code). SCD1. | `SEQ_DIM_PARTNER_ZONE.NEXTVAL  /* assigned on first insert of (partner_id, zone_code) */` | Unknown member: partner_zone_sk = -1. |
| `partner_id` | varchar | NOT NULL | Direct | silver.partner_zone.partner_id, silver.partner.partner_id | Grain: partner × zone (silver.partner_zone JOIN silver.partner ON partner_id). | `pz.partner_id` |  |
| `zone_code` | varchar | NOT NULL | Direct | silver.partner_zone.zone_code |  | `pz.zone_code` | R16: zone_code must be unique in this dimension (one partner per zone). |
| `partner_name` | varchar |  | Direct | silver.partner.partner_name | From partner header. | `pa.partner_name` |  |
| `country_code` | char(2) |  | Direct | silver.partner.country_code | From partner header. | `pa.country_code` |  |
| `base_depot_gln` | char(13) |  | Direct | silver.partner.base_depot_gln | FK to dim_depot. | `pa.base_depot_gln` | Must exist in dim_depot, else unknown depot. |

## dim_tariff (Dimension)

| Gold Column | Type | Key | Mapping | Silver Source(s) | Join / Lookup Logic | Snowflake Expression | DQ / Notes |
|---|---|---|---|---|---|---|---|
| `tariff_sk` | int | PK | Surrogate Key | silver.tariff_weight_band.tariff_id, silver.tariff_weight_band.band_index | One surrogate per tariff × weight band. | `SEQ_DIM_TARIFF.NEXTVAL  /* assigned on first insert of (tariff_id, band_index) */` | Unknown member: tariff_sk = -1. |
| `tariff_id` | varchar | NOT NULL | Direct | silver.tariff.tariff_id | Grain: silver.tariff JOIN silver.tariff_weight_band ON tariff_id. | `t.tariff_id` |  |
| `customer_sk` | int | FK | Lookup (SK) | silver.tariff.contact_id | dim_customer version valid at tariff.valid_from; if none (customer version created later) take the earliest version; if contact not found → -1. | `COALESCE(dc.customer_sk, -1)  /* dc.contact_id = t.contact_id AND t.valid_from::timestamp >= dc.valid_from AND t.valid_from::timestamp < COALESCE(dc.valid_to,'9999-12-31') */` |  |
| `product` | varchar |  | Direct | silver.tariff.product |  | `t.product` |  |
| `zone_code` | varchar |  | Direct | silver.tariff.zone_code |  | `t.zone_code` |  |
| `weight_from_kg` | decimal(6,2) |  | Direct | silver.tariff_weight_band.from_kg | Rename from_kg → weight_from_kg. | `wb.from_kg` | Band rule: weight_from_kg < weight ≤ weight_to_kg (first band includes 0). |
| `weight_to_kg` | decimal(6,2) |  | Direct | silver.tariff_weight_band.to_kg | Rename to_kg → weight_to_kg. | `wb.to_kg` |  |
| `price` | decimal(10,2) |  | Direct | silver.tariff_weight_band.price | Band price in tariff currency. | `wb.price` |  |
| `currency` | char(3) |  | Direct | silver.tariff.currency |  | `t.currency` |  |
| `fuel_surcharge_pct` | decimal(5,2) |  | Direct | silver.tariff.fuel_surcharge_pct | Percentage points (12.5 = 12.5%). | `t.fuel_surcharge_pct` |  |
| `valid_from` | date |  | Direct | silver.tariff.valid_from |  | `t.valid_from` |  |
| `valid_to` | date |  | Direct | silver.tariff.valid_to | NULL = open-ended. | `t.valid_to` | R14 checked in silver. |

## fct_parcel (Fact)

| Gold Column | Type | Key | Mapping | Silver Source(s) | Join / Lookup Logic | Snowflake Expression | DQ / Notes |
|---|---|---|---|---|---|---|---|
| `track_id` | varchar | PK | Direct | silver.veloship_parcel.track_id, silver.veloship_parcel.message_id, silver.veloship_shipment.message_id | Grain: one row per parcel. silver.veloship_parcel p JOIN silver.veloship_shipment s ON p.message_id = s.message_id. | `p.track_id` | Inferred members for orphan track_ids (R2) – see Gaps & Decisions. |
| `customer_sk` | int | FK, NOT NULL | Lookup (SK) | silver.veloship_shipment.contact_id, silver.veloship_shipment.shipping_date | dim_customer version valid on shipping_date (SCD2 point-in-time); not found → -1. | `COALESCE(dc.customer_sk, -1)  /* dc.contact_id = s.contact_id AND s.shipping_date::timestamp >= dc.valid_from AND s.shipping_date::timestamp < COALESCE(dc.valid_to,'9999-12-31') */` |  |
| `partner_zone_sk` | int | FK | Lookup (SK) | silver.invoice_line.zone_code, silver.invoice_line.track_id | Delivery zone taken from the parcel's (non-credit-note) invoice line, then mapped to dim_partner_zone on zone_code. NULL/-1 when the parcel is not invoiced. | `COALESCE(dpz.partner_zone_sk, -1)  /* dpz.zone_code = (SELECT ANY_VALUE(l.zone_code) FROM silver.invoice_line l JOIN silver.invoice i USING(invoice_number) WHERE l.track_id = p.track_id AND i.doc_type = 'INVOICE') */` | GAP: no postal-code → zone reference in silver; see Gaps & Decisions. |
| `shipping_date_key` | int | FK, NOT NULL | Lookup (SK) | silver.veloship_shipment.shipping_date | Role-playing dim_date (shipping date). | `COALESCE(TO_NUMBER(TO_CHAR(s.shipping_date,'YYYYMMDD')), -1)` |  |
| `parcel_number` | varchar |  | Direct | silver.veloship_parcel.parcel_number |  | `p.parcel_number` |  |
| `sscc` | char(18) |  | Direct | silver.veloship_parcel.sscc |  | `p.sscc` |  |
| `product` | varchar |  | Direct | silver.veloship_shipment.product | Shipment-level attribute repeated per parcel (degenerate). | `s.product` |  |
| `incoterm_code` | varchar |  | Direct | silver.veloship_shipment.incoterm_code | Degenerate attribute. | `s.incoterm_code` |  |
| `consignee_country` | char(2) |  | Direct | silver.veloship_shipment.consignee_country | Degenerate attribute. | `s.consignee_country` |  |
| `weight_kg` | decimal(8,2) |  | Direct | silver.veloship_parcel.weight_kg | Additive measure. | `p.weight_kg` |  |
| `pickup_ts` | timestamp |  | Aggregated | silver.tracking_event.event_time_utc, silver.tracking_event.status | First PICKUP scan of the parcel (UTC). | `MIN(IFF(e.status = 'PICKUP', e.event_time_utc, NULL))  /* GROUP BY e.track_id */` | NULL = not picked up yet. Invoiced parcel without PICKUP → R2 anomaly. |
| `delivered_ts` | timestamp |  | Aggregated | silver.tracking_event.event_time_utc, silver.tracking_event.status | First DELIVERED scan (UTC). PARCELSHOP drop-off is not counted as delivered. | `MIN(IFF(e.status = 'DELIVERED', e.event_time_utc, NULL))  /* GROUP BY e.track_id */` | Lead time = DATEDIFF(hour, pickup_ts, delivered_ts) (target 92% ≤ 24 h). |
| `is_delivered` | boolean |  | Calculated | silver.tracking_event.status | TRUE if a DELIVERED event exists. | `delivered_ts IS NOT NULL` |  |

## fct_parcel_event (Fact)

| Gold Column | Type | Key | Mapping | Silver Source(s) | Join / Lookup Logic | Snowflake Expression | DQ / Notes |
|---|---|---|---|---|---|---|---|
| `event_id` | varchar | PK | Direct | silver.tracking_event.event_id | Grain: one row per scan. Source silver.tracking_event e. | `e.event_id` | R4 business duplicates are kept (see Gaps & Decisions). |
| `track_id` | varchar | FK, NOT NULL | Direct | silver.tracking_event.track_id | FK to fct_parcel (fact-to-fact). | `e.track_id` | R2: track_id must exist in fct_parcel (inferred member otherwise). |
| `depot_gln` | char(13) | FK | Lookup (SK) | silver.tracking_event.location_gln | FK to dim_depot. NULL for DATA_RECEIVED (no location); GLN not in dim_depot → unknown depot. | `CASE WHEN e.location_gln IS NULL THEN NULL WHEN dd.depot_gln IS NULL THEN '0000000000000' ELSE e.location_gln END` |  |
| `event_date_key` | int | FK, NOT NULL | Lookup (SK) | silver.tracking_event.event_time_utc, silver.tracking_event.event_tz_offset | Local business date of the scan: UTC time shifted by the original offset. | `TO_NUMBER(TO_CHAR(DATEADD(minute, (IFF(e.event_tz_offset='Z',0, SPLIT_PART(e.event_tz_offset,':',1)::int*60 + SIGN(SPLIT_PART(e.event_tz_offset,':',1)::int)*SPLIT_PART(e.event_tz_offset,':',2)::int)), e.event_time_utc)::date,'YYYYMMDD'))` | Decision: local vs UTC date – see Gaps & Decisions. |
| `status` | varchar | NOT NULL | Direct | silver.tracking_event.status |  | `e.status` |  |
| `event_ts` | timestamp | NOT NULL | Direct | silver.tracking_event.event_time_utc | Event time in UTC. | `e.event_time_utc` |  |
| `reason_code` | varchar |  | Direct | silver.tracking_event.reason_code | Only on NOT_DELIVERED. | `e.reason_code` |  |
| `capture_delay_min` | int |  | Calculated | silver.tracking_event.event_time_utc, silver.tracking_event.record_time_utc | Minutes between scan and system record (timeliness, R5). | `DATEDIFF(minute, e.event_time_utc, e.record_time_utc)` | Negative value = R5 violation. |

## fct_invoice_line (Fact)

| Gold Column | Type | Key | Mapping | Silver Source(s) | Join / Lookup Logic | Snowflake Expression | DQ / Notes |
|---|---|---|---|---|---|---|---|
| `invoice_number` | varchar | PK (part) | Direct | silver.invoice_line.invoice_number, silver.invoice.invoice_number | Grain: invoice × line. silver.invoice_line l JOIN silver.invoice i ON invoice_number. | `l.invoice_number` | Degenerate dimension. |
| `line_no` | int | PK (part) | Direct | silver.invoice_line.line_no |  | `l.line_no` |  |
| `customer_sk` | int | FK, NOT NULL | Lookup (SK) | silver.invoice.contact_id, silver.invoice.issue_date | dim_customer version valid on issue_date; not in master data → -1 (unknown member). | `COALESCE(dc.customer_sk, -1)  /* dc.contact_id = i.contact_id AND i.issue_date::timestamp >= dc.valid_from AND i.issue_date::timestamp < COALESCE(dc.valid_to,'9999-12-31') */` | Injected anomaly: invoice of a customer not in master data. |
| `tariff_sk` | int | FK | Lookup (SK) | silver.invoice_line.tariff_id, silver.invoice_line.weight_kg | dim_tariff row of the line's tariff_id whose weight band contains the line weight. | `COALESCE(dt.tariff_sk, -1)  /* dt.tariff_id = l.tariff_id AND l.weight_kg > dt.weight_from_kg AND l.weight_kg <= dt.weight_to_kg */` | R8 check: l.base_price = dt.price. |
| `track_id` | varchar | FK | Direct | silver.invoice_line.track_id | FK to fct_parcel. | `l.track_id` | R2. |
| `invoice_date_key` | int | FK, NOT NULL | Lookup (SK) | silver.invoice.issue_date | Role-playing dim_date (issue date). | `COALESCE(TO_NUMBER(TO_CHAR(i.issue_date,'YYYYMMDD')), -1)` |  |
| `doc_type` | varchar |  | Direct | silver.invoice.doc_type | INVOICE / CREDIT_NOTE. | `i.doc_type` |  |
| `billing_period` | varchar |  | Direct | silver.invoice.billing_period | 'YYYY-MM'. | `i.billing_period` |  |
| `currency` | char(3) |  | Direct | silver.invoice.currency | Local currency of amount_local. | `i.currency` |  |
| `amount_local` | decimal(12,2) |  | Direct | silver.invoice_line.net_amount | Line net amount (base + surcharges) in invoice currency. Negative for credit notes. | `l.net_amount` | Additive measure. |
| `fx_rate_eur` | decimal(12,6) |  | Direct | silver.invoice.fx_rate_to_eur | Rate of the issue date. | `i.fx_rate_to_eur` |  |
| `amount_eur` | decimal(12,2) |  | Calculated | silver.invoice_line.net_amount, silver.invoice.fx_rate_to_eur | Net amount converted to EUR. | `ROUND(l.net_amount * i.fx_rate_to_eur, 2)` | R13: Σ amount_eur per country/month ≈ fct_gl_revenue (±0.5%). |
| `fuel_surcharge_eur` | decimal(12,2) |  | Aggregated | silver.invoice_surcharge.amount, silver.invoice_surcharge.surcharge_type, silver.invoice_surcharge.invoice_number, silver.invoice_surcharge.line_no, silver.invoice.fx_rate_to_eur | Sum of the line's FUEL surcharges, converted to EUR; 0 when none. | `ROUND(COALESCE(SUM(IFF(sc.surcharge_type='FUEL', sc.amount, 0)),0) * i.fx_rate_to_eur, 2)  /* LEFT JOIN silver.invoice_surcharge sc ON (invoice_number, line_no) GROUP BY line */` |  |

## fct_partner_cost (Fact)

| Gold Column | Type | Key | Mapping | Silver Source(s) | Join / Lookup Logic | Snowflake Expression | DQ / Notes |
|---|---|---|---|---|---|---|---|
| `settlement_id` | varchar | PK | Direct | silver.partner_settlement.settlement_id | Grain: partner × zone × month. Source silver.partner_settlement ps. | `ps.settlement_id` |  |
| `partner_zone_sk` | int | FK, NOT NULL | Lookup (SK) | silver.partner_settlement.partner_id, silver.partner_settlement.zone_code | dim_partner_zone on (partner_id, zone_code); not found → -1. | `COALESCE(dpz.partner_zone_sk, -1)  /* dpz.partner_id = ps.partner_id AND dpz.zone_code = ps.zone_code */` |  |
| `period` | varchar | NOT NULL | Direct | silver.partner_settlement.period | Joins to dim_date at month grain via dim_date.period. | `ps.period` |  |
| `stops` | int |  | Direct | silver.partner_settlement.stops |  | `ps.stops` | R12. |
| `delivered_parcels` | int |  | Direct | silver.partner_settlement.delivered_parcels | As declared by partner. | `ps.delivered_parcels` | R11: compare with COUNT of fct_parcel delivered in zone/month. |
| `failed_attempts` | int |  | Direct | silver.partner_settlement.failed_attempts |  | `ps.failed_attempts` |  |
| `total_cost_eur` | decimal(12,2) |  | Calculated | silver.partner_settlement.total_cost, silver.partner_settlement.currency | Settlement total converted to EUR; EUR → as is; other currencies need an FX rate for the period. | `IFF(ps.currency='EUR', ps.total_cost, ROUND(ps.total_cost * fx.rate_to_eur, 2))  /* LEFT JOIN REF.FX_RATE_MONTHLY fx ON fx.currency = ps.currency AND fx.period = ps.period */` | GAP: no FX source in silver for settlements (e.g. PLN partners). |

## fct_gl_revenue (Fact)

| Gold Column | Type | Key | Mapping | Silver Source(s) | Join / Lookup Logic | Snowflake Expression | DQ / Notes |
|---|---|---|---|---|---|---|---|
| `company_code` | varchar | PK (part) | Direct | silver.gl_balance.company_code | Grain: company × month × GL account. Source silver.gl_balance g. | `g.company_code` |  |
| `period` | varchar | PK (part) | Direct | silver.gl_balance.period | Month grain; joins to dim_date.period. | `g.period` |  |
| `gl_account` | varchar | PK (part) | Direct | silver.gl_balance.gl_account |  | `g.gl_account` | silver.gl_balance.account_name not carried (no GL account dimension in the model). |
| `country_code` | char(2) |  | Direct | silver.gl_balance.country_code |  | `g.country_code` |  |
| `amount_eur` | decimal(14,2) |  | Direct | silver.gl_balance.amount_eur | Already in EUR. | `g.amount_eur` | R13 reconciliation target. |

## fct_opportunity (Fact)

| Gold Column | Type | Key | Mapping | Silver Source(s) | Join / Lookup Logic | Snowflake Expression | DQ / Notes |
|---|---|---|---|---|---|---|---|
| `opportunity_id` | varchar | PK | Direct | silver.crm_opportunity.opportunity_id | Grain: one row per opportunity (latest state). Source silver.crm_opportunity o. | `o.opportunity_id` |  |
| `customer_sk` | int | FK | Lookup (SK) | silver.crm_opportunity.contact_id | Current dim_customer version (is_current). NULL when contact_id is NULL (not yet a customer); contact_id not found → -1. | `CASE WHEN o.contact_id IS NULL THEN NULL ELSE COALESCE(dc.customer_sk, -1) END  /* dc.contact_id = o.contact_id AND dc.is_current */` |  |
| `close_date_key` | int | FK | Lookup (SK) | silver.crm_opportunity.close_date | Role-playing dim_date (close date). | `COALESCE(TO_NUMBER(TO_CHAR(o.close_date,'YYYYMMDD')), -1)` |  |
| `stage` | varchar |  | Direct | silver.crm_opportunity.stage |  | `o.stage` |  |
| `amount_eur` | decimal(14,2) |  | Calculated | silver.crm_opportunity.amount, silver.crm_opportunity.currency, silver.crm_opportunity.close_date | Amount converted to EUR at the close-date rate. | `IFF(o.currency='EUR' OR o.currency IS NULL, o.amount, ROUND(o.amount * fx.rate_to_eur, 2))  /* LEFT JOIN REF.FX_RATE_DAILY fx ON fx.currency = o.currency AND fx.rate_date = o.close_date */` | GAP: no FX source in silver for CRM amounts. |
| `country_code` | char(2) |  | Direct | silver.crm_opportunity.country_code |  | `o.country_code` |  |
| `expected_parcels_month` | int |  | Direct | silver.crm_opportunity.expected_parcels_month |  | `o.expected_parcels_month` |  |

## fct_lead (Fact)

| Gold Column | Type | Key | Mapping | Silver Source(s) | Join / Lookup Logic | Snowflake Expression | DQ / Notes |
|---|---|---|---|---|---|---|---|
| `lead_id` | varchar | PK | Direct | silver.crm_lead.lead_id | Grain: one row per lead (latest state). Source silver.crm_lead ld. | `ld.lead_id` |  |
| `created_date_key` | int | FK | Lookup (SK) | silver.crm_lead.created_at | Role-playing dim_date (creation date, UTC). | `COALESCE(TO_NUMBER(TO_CHAR(ld.created_at::date,'YYYYMMDD')), -1)` |  |
| `converted_opportunity_id` | varchar | FK | Direct | silver.crm_lead.converted_opportunity_id | 1:1 link to fct_opportunity. | `ld.converted_opportunity_id` | Must exist in fct_opportunity. |
| `country_code` | char(2) |  | Direct | silver.crm_lead.country_code | ISO-normalised in silver (country_raw not carried). | `ld.country_code` |  |
| `lead_source` | varchar |  | Direct | silver.crm_lead.lead_source |  | `ld.lead_source` |  |
| `status` | varchar |  | Direct | silver.crm_lead.status |  | `ld.status` |  |

## Silver columns not carried to gold

- **veloship_shipment**: `shipment_reference`, `middleware`, `consignee_id`, `consignee_name`, `consignee_zip`, `consignee_city`, `received_at`, `_loaded_at`
- **veloship_parcel**: `unit_index`, `shipment_unit_reference`, `services`
- **tracking_event**: `event_type`, `sscc`, `biz_step`, `location_name`, `scanner_id`, `_loaded_at`
- **invoice**: `vat_number`, `total_net`, `total_vat`, `total_gross`
- **invoice_line**: `product`, `base_price`
- **invoice_surcharge**: `surcharge_index`
- **crm_opportunity**: `account_id`, `opportunity_name`, `last_modified_at`
- **crm_lead**: `company_name`, `country_raw`
- **partner_settlement**: `cost_per_stop`
- **gl_balance**: `account_name`
- **depot**: `postal_code`

## Gaps & Decisions

| # | Type | Gold Table(s) | Topic | Issue | Recommendation | Status |
|---|---|---|---|---|---|---|
| 1 | Gap | fct_partner_cost, fct_opportunity | FX rates | total_cost_eur and amount_eur need a rate, but only invoices carry fx_rate_to_eur in silver. | Add a reference table REF.FX_RATE_DAILY (e.g. ECB rates) and derive monthly averages for settlements. Do not borrow invoice rates. | Open |
| 2 | Gap | fct_parcel | Delivery zone (partner_zone_sk) | Silver has no postal-code → zone reference, so the parcel's zone is only known once it is invoiced. | Short term: take zone_code from the parcel's invoice line. Target: add REF.POSTAL_ZONE (country, zip range → zone_code) and derive from consignee_country + consignee_zip. | Open |
| 3 | Gap | fct_invoice_line | Credit note link | related_invoice_number is not in silver nor in gold (see silver mapping gap #1). | Add related_invoice_number to silver.invoice and as a degenerate column in fct_invoice_line. | Open |
| 4 | Decision | all facts | Unknown members | Lookups can fail (customer not in master data, GLN not in depots, settlement zone unknown, null dates). | Seed unknown rows: dim_customer -1, dim_partner_zone -1, dim_tariff -1, dim_depot '0000000000000', dim_date -1 (1900-01-01). Lookups use COALESCE(..., -1). | Proposed |
| 5 | Decision | fct_parcel, fct_parcel_event, fct_invoice_line | Orphan track_ids (R2) | Events or invoice lines may reference a track_id with no shipment; FK to fct_parcel would break. | Insert inferred fct_parcel rows (customer_sk from the invoice, other attributes NULL) and add an is_inferred boolean to fct_parcel. Snowflake FKs are not enforced, so this must be done in the load. | Open |
| 6 | Decision | dim_customer, fct_* | SCD2 lookup rule | Facts must point to the customer version valid at the business date. | Point-in-time join: valid_from ≤ business_date < COALESCE(valid_to,'9999-12-31'). Business dates: shipping_date (parcel), issue_date (invoice), valid_from (tariff); opportunities use the current version. | Proposed |
| 7 | Decision | fct_parcel_event | Event date key | eventTime is local with offset; silver stores UTC + offset. | Use the local business date for event_date_key (UTC shifted by event_tz_offset); keep event_ts in UTC. | Open |
| 8 | Decision | fct_parcel | Delivered definition | PARCELSHOP and NOT_DELIVERED are deviations, not final delivery. | is_delivered / delivered_ts based only on the first DELIVERED event. | Proposed |
| 9 | Decision | fct_parcel_event | R4 business duplicates | Repeated (track_id, status, GLN) events are kept in silver and flagged. | Keep them in gold for auditability but add an is_duplicate flag (not in current model) so KPIs can exclude them. | Open |
| 10 | Decision | dim_tariff, fct_invoice_line | Weight band boundary | Bands are contiguous (to_kg[n] = from_kg[n+1]); a weight equal to a boundary could match two bands. | Rule: weight_from_kg < weight ≤ weight_to_kg (first band from 0 inclusive). | Proposed |
| 11 | Decision | fct_invoice_line | Measures not carried | base_price, non-fuel surcharges (PEAK, TOLL, REMOTE_AREA), VAT and gross totals stay in silver. | If margin/price analysis is needed, add base_price_eur and other_surcharges_eur to fct_invoice_line. | Open |
| 12 | Decision | fct_partner_cost, fct_gl_revenue | Monthly grain | Monthly facts use period 'YYYY-MM' instead of a date_key. | Join to dim_date on period (or add period_key = YYYYMM and a month-level dimension view). | Proposed |
