# Source → Silver Field Mapping

Markdown version of `VLP_Source_to_Silver_Field_Mapping.xlsx` (generated 2026-11-22). Sources: `contracts/schemas/*.schema.json`, `docs/data-model/silver_model.dbml`, `docs/architecture/ingestion_kafka_s3_bronze.md`. Status legend: Direct · Transformed · Derived · Propagated · Explode · Routing · Metadata · Structural · Not mapped.


## veloship_shipment (bronze `RAW_VELOSHIP_SHIPMENTS`)

| Source JSON Path | Type | Req | Silver Table.Column | Silver Type | Key | Status | Snowflake Expression | Rule | DQ / Notes |
|---|---|---|---|---|---|---|---|---|---|
| `messageId` | string (uuid) | Yes | veloship_shipment.message_id | varchar | PK | Direct | `RAW_PAYLOAD:messageId::varchar` | Cast to varchar. | Must be unique in silver. Technical duplicates (same topic+partition+offset / same ce_id) already removed in bronze. |
| `messageId` | string (uuid) | Yes | veloship_parcel.message_id | varchar | FK, NOT NULL | Propagated | `RAW_PAYLOAD:messageId::varchar` | Parent key carried to every parcel row produced by the ShipmentUnit/ParcelData explode. | FK to veloship_shipment.message_id. |
| `receivedAt` | string (date-time) | Yes | veloship_shipment.received_at | timestamp |  | Transformed | `CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(RAW_PAYLOAD:receivedAt::varchar))::timestamp_ntz` | Parse ISO-8601 with offset and normalise to UTC (timestamp_ntz). | Null after TRY_ parse = invalid timestamp → DQ flag. |
| `request` | object | Yes | — | — |  | Structural | — | Container object; no column of its own. |  |
| `request.Shipment` | object | Yes | — | — |  | Structural | — | Container object; no column of its own. |  |
| `request.Shipment.ShipmentReference[]` | array<string> | No | veloship_shipment.shipment_reference | varchar |  | Transformed | `ARRAY_TO_STRING(RAW_PAYLOAD:request:Shipment:ShipmentReference, '\|')` | Array collapsed into a pipe-delimited string (order preserved). Null when absent. | Open decision: silver keeps varchar; if references must be searchable individually, consider a child table or VARIANT. |
| `request.Shipment.ShippingDate` | string (date) | Yes | veloship_shipment.shipping_date | date |  | Transformed | `TRY_TO_DATE(RAW_PAYLOAD:request:Shipment:ShippingDate::varchar)` | Parse ISO date. | Used by R8 to pick the valid tariff. |
| `request.Shipment.IncotermCode` | string | No | veloship_shipment.incoterm_code | varchar |  | Direct | `RAW_PAYLOAD:request:Shipment:IncotermCode::varchar` | Keep as string (preserve leading zeros). | Business rule: mandatory between different commercial zones (not enforced by schema). |
| `request.Shipment.Product` | string (enum) | Yes | veloship_shipment.product | varchar |  | Direct | `UPPER(TRIM(RAW_PAYLOAD:request:Shipment:Product::varchar))` | Trim/upper as defensive cleansing. | Domain check against enum. |
| `request.Shipment.Middleware` | string | Yes | veloship_shipment.middleware | varchar |  | Direct | `TRIM(RAW_PAYLOAD:request:Shipment:Middleware::varchar)` | Trim. |  |
| `request.Shipment.Shipper` | object | Yes | — | — |  | Structural | — | Container object. |  |
| `request.Shipment.Shipper.ContactID` | string | Yes | veloship_shipment.contact_id | varchar | NOT NULL | Direct | `RAW_PAYLOAD:request:Shipment:Shipper:ContactID::varchar` | Rename ContactID → contact_id. | Referential check: contact_id exists in silver.customer (else unknown member in gold). |
| `request.Shipment.Consignee` | object | Yes | — | — |  | Structural | — | Container object. |  |
| `request.Shipment.Consignee.ConsigneeID` | string | No | veloship_shipment.consignee_id | varchar |  | Direct | `RAW_PAYLOAD:request:Shipment:Consignee:ConsigneeID::varchar` | Cast to varchar; null when absent. |  |
| `request.Shipment.Consignee.Address` | object | Yes | — | — |  | Structural | — | Container object. |  |
| `request.Shipment.Consignee.Address.Name1` | string | Yes | veloship_shipment.consignee_name | varchar |  | Direct | `TRIM(RAW_PAYLOAD:request:Shipment:Consignee:Address:Name1::varchar)` | Trim. | PII – apply masking policy in Snowflake. |
| `request.Shipment.Consignee.Address.CountryCode` | string | Yes | veloship_shipment.consignee_country | char(2) |  | Direct | `UPPER(RAW_PAYLOAD:request:Shipment:Consignee:Address:CountryCode::varchar)` | Rename; upper-case. | Used by R6 (last scan depot belongs to consignee zone). |
| `request.Shipment.Consignee.Address.ZIPCode` | string | Yes | veloship_shipment.consignee_zip | varchar |  | Direct | `TRIM(RAW_PAYLOAD:request:Shipment:Consignee:Address:ZIPCode::varchar)` | Keep as string (leading zeros, letters). |  |
| `request.Shipment.Consignee.Address.City` | string | Yes | veloship_shipment.consignee_city | varchar |  | Direct | `TRIM(RAW_PAYLOAD:request:Shipment:Consignee:Address:City::varchar)` | Trim. |  |
| `request.Shipment.Consignee.Address.Street` | string | Yes | — | — |  | Not mapped | — | Dropped in silver (personal data not needed by Sales & Finance data products). | Gap / decision – see 'Gaps & Decisions'. Remains available in bronze RAW_PAYLOAD. |
| `request.Shipment.Consignee.Address.StreetNumber` | string | No | — | — |  | Not mapped | — | Dropped in silver (personal data). | Gap / decision – see 'Gaps & Decisions'. |
| `request.Shipment.ShipmentUnit[]` | array<object> | Yes | veloship_parcel.(one row per element) | — |  | Explode | `LATERAL FLATTEN(input => RAW_PAYLOAD:request:Shipment:ShipmentUnit) u` | One silver.veloship_parcel row per ShipmentUnit, zipped by position with response.CreatedShipment.ParcelData (u.index = p.index). | R1: len(ParcelData) = len(ShipmentUnit); mismatching messages flagged (Completeness). |
| `request.Shipment.ShipmentUnit[] (position)` | array index | — | veloship_parcel.unit_index | int | NOT NULL | Derived | `u.index + 1` | 1-based position of the unit inside the message. |  |
| `request.Shipment.ShipmentUnit[].ShipmentUnitReference[]` | array<string> | No | veloship_parcel.shipment_unit_reference | varchar |  | Transformed | `ARRAY_TO_STRING(u.value:ShipmentUnitReference, '\|')` | Array collapsed into pipe-delimited string. |  |
| `request.Shipment.ShipmentUnit[].Weight` | number | Yes | veloship_parcel.weight_kg | decimal(8,2) |  | Transformed | `ROUND(u.value:Weight::number(10,3), 2)::number(8,2)` | Rename; round to 2 decimals. | Range check (0, 30]. Used by R8 (weight band). |
| `request.Shipment.ShipmentUnit[].Service[]` | array<object> | No | veloship_parcel.services | variant |  | Transformed | `TRANSFORM(u.value:Service, s -> s:Service:ServiceName)` | Flatten wrapper objects into an ARRAY of ServiceName strings, e.g. ["service_flexible_delivery","service_carbon_neutral"]. Empty/absent → NULL. |  |
| `request.Shipment.ShipmentUnit[].Service[].Service` | object | Yes (within item) | — | — |  | Structural | — | Wrapper object, removed by the TRANSFORM above. |  |
| `request.Shipment.ShipmentUnit[].Service[].Service.ServiceName` | string (enum) | Yes (within item) | veloship_parcel.services | variant |  | Transformed | `(element of services array)` | Each value becomes one element of the services array. | Domain check against enum. |
| `response` | object | Yes | — | — |  | Structural | — | Container object. |  |
| `response.CreatedShipment` | object | Yes | — | — |  | Structural | — | Container object. |  |
| `response.CreatedShipment.ParcelData[]` | array<object> | Yes | veloship_parcel.(zipped with ShipmentUnit) | — |  | Explode | `LATERAL FLATTEN(input => RAW_PAYLOAD:response:CreatedShipment:ParcelData) p  /* JOIN ON p.index = u.index */` | Second flatten zipped by position with ShipmentUnit. | R1. Use FULL OUTER join on index so unmatched units/parcels are visible to DQ instead of silently lost. |
| `response.CreatedShipment.ParcelData[].TrackID` | string | Yes | veloship_parcel.track_id | varchar | PK | Direct | `p.value:TrackID::varchar` | Rename TrackID → track_id. | Unique across silver.veloship_parcel. Master key for R2 (events and invoices). |
| `response.CreatedShipment.ParcelData[].ParcelNumber` | string | Yes | veloship_parcel.parcel_number | varchar |  | Direct | `p.value:ParcelNumber::varchar` | Keep as string (leading zeros). |  |
| `response.CreatedShipment.ParcelData[].SSCC` | string | No | veloship_parcel.sscc | char(18) |  | Direct | `p.value:SSCC::varchar` | Cast. | R15: GS1 check digit must be valid. |
| `(bronze metadata) _LOAD_TS` | timestamp | — | veloship_shipment._loaded_at | timestamp |  | Metadata | `_LOAD_TS` | Bronze load timestamp carried as silver lineage column (alternatively CURRENT_TIMESTAMP() of the silver merge). | Used for freshness monitoring. |

## tracking_event (bronze `RAW_TRACKING_EVENTS`)

| Source JSON Path | Type | Req | Silver Table.Column | Silver Type | Key | Status | Snowflake Expression | Rule | DQ / Notes |
|---|---|---|---|---|---|---|---|---|---|
| `eventId` | string | Yes | tracking_event.event_id | varchar | PK | Direct | `RAW_PAYLOAD:eventId::varchar` | Cast. | Unique. Business duplicates (R4) are kept and flagged, not removed. |
| `eventType` | string (const) | Yes | tracking_event.event_type | varchar |  | Direct | `RAW_PAYLOAD:eventType::varchar` | Cast. | Must equal 'ObjectEvent'. |
| `eventTime` | string (date-time) | Yes | tracking_event.event_time_utc | timestamp | NOT NULL | Transformed | `CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(RAW_PAYLOAD:eventTime::varchar))::timestamp_ntz` | Parse with offset and normalise to UTC. | R5: record_time_utc ≥ event_time_utc; lag normally seconds to minutes (Timeliness). |
| `eventTime` | string (date-time) | Yes | tracking_event.event_tz_offset | varchar |  | Derived | `REGEXP_SUBSTR(RAW_PAYLOAD:eventTime::varchar, '([+-][0-9]{2}:[0-9]{2}\|Z)$')` | Original local offset kept so local event time can be rebuilt (e.g. '+01:00'). |  |
| `recordTime` | string (date-time) | Yes | tracking_event.record_time_utc | timestamp |  | Transformed | `CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(RAW_PAYLOAD:recordTime::varchar))::timestamp_ntz` | Parse and normalise to UTC. | R5. |
| `trackId` | string | Yes | tracking_event.track_id | varchar | NOT NULL | Direct | `RAW_PAYLOAD:trackId::varchar` | Rename. | R2: must exist in silver.veloship_parcel. |
| `epcList[]` | array<string> | No | tracking_event.sscc | char(18) |  | Derived | `GS1_SSCC_FROM_URN(RAW_PAYLOAD:epcList[0]::varchar)` | Convert EPC URN to 18-digit SSCC: extension digit (1st digit of serial ref) + company prefix (7) + remaining serial (9) + computed GS1 check digit. Implemented as a SQL UDF. | Should match veloship_parcel.sscc for the same track_id. |
| `status` | string (enum) | Yes | tracking_event.status | varchar | NOT NULL | Direct | `UPPER(TRIM(RAW_PAYLOAD:status::varchar))` | Cast; defensive trim/upper. | Invalid values (e.g. 'DELIVERD') are quarantined in bronze. R3 sequence check runs on silver. |
| `bizStep` | string (enum) | No | tracking_event.biz_step | varchar |  | Transformed | `REPLACE(RAW_PAYLOAD:bizStep::varchar, 'urn:epcglobal:cbv:bizstep:', '')` | Strip CBV URN prefix → short value (e.g. 'arriving'). | Decision: keep full URN instead if downstream needs CBV compliance. |
| `bizLocation` | object | Conditional | — | — |  | Structural | — | Container object. | Conditional presence checked by schema (if/then/else). |
| `bizLocation.gln` | string | Yes (within object) | tracking_event.location_gln | char(13) |  | Direct | `RAW_PAYLOAD:bizLocation:gln::varchar` | Rename gln → location_gln. | R6: must exist in silver.depot; R15 check digit; R4 uniqueness of (track_id, status, location_gln). |
| `bizLocation.name` | string | No | tracking_event.location_name | varchar |  | Direct | `TRIM(RAW_PAYLOAD:bizLocation:name::varchar)` | Trim. | Descriptive only; depot master (silver.depot.depot_name) is the source of truth. |
| `reasonCode` | string \| null (enum) | Conditional | tracking_event.reason_code | varchar |  | Direct | `RAW_PAYLOAD:reasonCode::varchar` | JSON null → SQL NULL. | Required when status = NOT_DELIVERED. |
| `device` | object | No | — | — |  | Structural | — | Container object. |  |
| `device.scannerId` | string | No | tracking_event.scanner_id | varchar |  | Direct | `RAW_PAYLOAD:device:scannerId::varchar` | Rename. |  |
| `device.firmware` | string | No | — | — |  | Not mapped | — | Technical device attribute, not needed in silver. | Remains in bronze RAW_PAYLOAD. |
| `(bronze metadata) _LOAD_TS` | timestamp | — | tracking_event._loaded_at | timestamp |  | Metadata | `_LOAD_TS` | Bronze load timestamp carried as lineage. |  |

## invoice (bronze `RAW_INVOICES`)

| Source JSON Path | Type | Req | Silver Table.Column | Silver Type | Key | Status | Snowflake Expression | Rule | DQ / Notes |
|---|---|---|---|---|---|---|---|---|---|
| `invoiceNumber` | string | Yes | invoice.invoice_number | varchar | PK | Direct | `RAW_PAYLOAD:invoiceNumber::varchar` | Cast. | Unique. |
| `invoiceNumber` | string | Yes | invoice_line.invoice_number | varchar | PK (part), FK | Propagated | `RAW_PAYLOAD:invoiceNumber::varchar` | Parent key carried to each line. |  |
| `invoiceNumber` | string | Yes | invoice_surcharge.invoice_number | varchar | PK (part), FK | Propagated | `RAW_PAYLOAD:invoiceNumber::varchar` | Parent key carried to each surcharge. |  |
| `docType` | string (enum) | Yes | invoice.doc_type | varchar | NOT NULL | Direct | `RAW_PAYLOAD:docType::varchar` | Cast. | R9: CREDIT_NOTE amounts must be negative. |
| `issueDate` | string (date) | Yes | invoice.issue_date | date |  | Transformed | `TRY_TO_DATE(RAW_PAYLOAD:issueDate::varchar)` | Parse ISO date. | R10: FX rate must be the rate of the issue date. |
| `billingPeriod` | string | Yes | invoice.billing_period | varchar |  | Direct | `RAW_PAYLOAD:billingPeriod::varchar` | Keep 'YYYY-MM' string. |  |
| `relatedInvoiceNumber` | string | Conditional | — | — |  | Not mapped | — | No target column in silver.invoice. | GAP: needed for R9 (credit note → existing invoice). Recommend adding related_invoice_number varchar. |
| `customer` | object | Yes | — | — |  | Structural | — | Container object. |  |
| `customer.contactId` | string | Yes | invoice.contact_id | varchar | NOT NULL | Direct | `RAW_PAYLOAD:customer:contactId::varchar` | Rename. | Must exist in silver.customer (else unknown member in gold). |
| `customer.vatNumber` | string | Yes | invoice.vat_number | varchar |  | Direct | `UPPER(REPLACE(RAW_PAYLOAD:customer:vatNumber::varchar,' ',''))` | Remove spaces, upper-case. | R15: prefix = customer country. |
| `currency` | string (enum) | Yes | invoice.currency | char(3) |  | Direct | `RAW_PAYLOAD:currency::varchar` | Cast. |  |
| `fxRateToEur` | number | Yes | invoice.fx_rate_to_eur | decimal(12,6) |  | Transformed | `RAW_PAYLOAD:fxRateToEur::number(12,6)` | Cast to fixed precision. | R10. |
| `lines[]` | array<object> | Yes | invoice_line.(one row per element) | — |  | Explode | `LATERAL FLATTEN(input => RAW_PAYLOAD:lines) l` | One silver.invoice_line row per element. |  |
| `lines[].lineNo` | integer | Yes | invoice_line.line_no | int | PK (part) | Direct | `l.value:lineNo::int` | Cast. | Sequential and unique per invoice (not enforced by schema). |
| `lines[].lineNo` | integer | Yes | invoice_surcharge.line_no | int | PK (part), FK | Propagated | `l.value:lineNo::int` | Parent line key carried to each surcharge. |  |
| `lines[].trackId` | string | Yes | invoice_line.track_id | varchar |  | Direct | `l.value:trackId::varchar` | Rename. | R2: must exist in veloship_parcel; invoiced parcel without PICKUP flagged. |
| `lines[].tariffId` | string | Yes | invoice_line.tariff_id | varchar |  | Direct | `l.value:tariffId::varchar` | Rename. | R8: must exist in silver.tariff and be valid on shipping date. |
| `lines[].product` | string (enum) | Yes | invoice_line.product | varchar |  | Direct | `l.value:product::varchar` | Cast. |  |
| `lines[].weightKg` | number | Yes | invoice_line.weight_kg | decimal(8,2) |  | Transformed | `ROUND(l.value:weightKg::number(10,3),2)::number(8,2)` | Round to 2 decimals. |  |
| `lines[].zone` | string | Yes | invoice_line.zone_code | varchar |  | Direct | `l.value:zone::varchar` | Rename zone → zone_code. |  |
| `lines[].basePrice` | number | Yes | invoice_line.base_price | decimal(12,2) |  | Transformed | `l.value:basePrice::number(12,2)` | Cast to fixed precision. | R8: = weight-band price of the valid tariff. |
| `lines[].surcharges[]` | array<object> | No | invoice_surcharge.(one row per element) | — |  | Explode | `LATERAL FLATTEN(input => l.value:surcharges, OUTER => FALSE) s` | Nested flatten: one silver.invoice_surcharge row per surcharge of each line. |  |
| `lines[].surcharges[] (position)` | array index | — | invoice_surcharge.surcharge_index | int | PK (part) | Derived | `s.index + 1` | 1-based position of the surcharge within the line. |  |
| `lines[].surcharges[].type` | string (enum) | Yes (within item) | invoice_surcharge.surcharge_type | varchar | NOT NULL | Direct | `s.value:type::varchar` | Rename type → surcharge_type. | R8: FUEL = base_price × fuel_surcharge_pct / 100. |
| `lines[].surcharges[].amount` | number | Yes (within item) | invoice_surcharge.amount | decimal(12,2) |  | Transformed | `s.value:amount::number(12,2)` | Cast. |  |
| `lines[].netAmount` | number | Yes | invoice_line.net_amount | decimal(12,2) |  | Transformed | `l.value:netAmount::number(12,2)` | Cast. | R7: net_amount = base_price + Σ surcharges. |
| `totals` | object | Yes | — | — |  | Structural | — | Container object. |  |
| `totals.net` | number | Yes | invoice.total_net | decimal(12,2) |  | Transformed | `RAW_PAYLOAD:totals:net::number(12,2)` | Rename; cast. | R7: = Σ invoice_line.net_amount. R13 GL reconciliation. |
| `totals.vat` | number | Yes | invoice.total_vat | decimal(12,2) |  | Transformed | `RAW_PAYLOAD:totals:vat::number(12,2)` | Rename; cast. |  |
| `totals.gross` | number | Yes | invoice.total_gross | decimal(12,2) |  | Transformed | `RAW_PAYLOAD:totals:gross::number(12,2)` | Rename; cast. | R7: gross = net + vat. |

## crm_object (Opportunity) (bronze `RAW_CRM_OBJECTS`)

| Source JSON Path | Type | Req | Silver Table.Column | Silver Type | Key | Status | Snowflake Expression | Rule | DQ / Notes |
|---|---|---|---|---|---|---|---|---|---|
| `attributes` | object | Yes | — | — |  | Structural | — | Container object. |  |
| `attributes.type` | string (const) | Yes | crm_opportunity.(row filter) | — |  | Routing | `WHERE RAW_PAYLOAD:attributes:type::varchar = 'Opportunity'` | Routes the record to silver.crm_opportunity. Not stored as a column. | Compacted topic: keep latest version per Id (QUALIFY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY LastModifiedDate DESC, _KAFKA_OFFSET DESC) = 1). |
| `Id` | string | Yes | crm_opportunity.opportunity_id | varchar | PK | Direct | `RAW_PAYLOAD:Id::varchar` | Rename Id → opportunity_id. |  |
| `AccountId` | string | Yes | crm_opportunity.account_id | varchar |  | Direct | `RAW_PAYLOAD:AccountId::varchar` | Rename. |  |
| `VLP_ContactID__c` | string \| null | No | crm_opportunity.contact_id | varchar |  | Direct | `RAW_PAYLOAD:VLP_ContactID__c::varchar` | Rename; JSON null → SQL NULL. | 'Closed Won' opportunities should link to an existing customer. |
| `Name` | string | Yes | crm_opportunity.opportunity_name | varchar |  | Direct | `TRIM(RAW_PAYLOAD:Name::varchar)` | Rename; trim. |  |
| `StageName` | string (enum) | Yes | crm_opportunity.stage | varchar |  | Direct | `RAW_PAYLOAD:StageName::varchar` | Rename. |  |
| `Amount` | number \| null | Conditional | crm_opportunity.amount | decimal(14,2) |  | Transformed | `RAW_PAYLOAD:Amount::number(14,2)` | Cast. |  |
| `CurrencyIsoCode` | string (enum) | No | crm_opportunity.currency | char(3) |  | Direct | `RAW_PAYLOAD:CurrencyIsoCode::varchar` | Rename. |  |
| `CloseDate` | string (date) | Yes | crm_opportunity.close_date | date |  | Transformed | `TRY_TO_DATE(RAW_PAYLOAD:CloseDate::varchar)` | Parse ISO date. |  |
| `Country__c` | string | Yes | crm_opportunity.country_code | char(2) |  | Direct | `UPPER(RAW_PAYLOAD:Country__c::varchar)` | Rename. |  |
| `Expected_Parcels_Month__c` | integer \| null | No | crm_opportunity.expected_parcels_month | int |  | Direct | `RAW_PAYLOAD:Expected_Parcels_Month__c::int` | Rename. |  |
| `LastModifiedDate` | string | Yes | crm_opportunity.last_modified_at | timestamp |  | Transformed | `CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(RAW_PAYLOAD:LastModifiedDate::varchar, 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZHTZM'))::timestamp_ntz` | Parse Salesforce format (offset without colon) to UTC. | Also used as version ordering for dedup. |

## crm_object (Lead) (bronze `RAW_CRM_OBJECTS`)

| Source JSON Path | Type | Req | Silver Table.Column | Silver Type | Key | Status | Snowflake Expression | Rule | DQ / Notes |
|---|---|---|---|---|---|---|---|---|---|
| `attributes` | object | Yes | — | — |  | Structural | — | Container object. |  |
| `attributes.type` | string (const) | Yes | crm_lead.(row filter) | — |  | Routing | `WHERE RAW_PAYLOAD:attributes:type::varchar = 'Lead'` | Routes the record to silver.crm_lead. Not stored as a column. | Keep latest version per Id (no LastModifiedDate in schema → order by _SOURCE_TS, _KAFKA_OFFSET). |
| `Id` | string | Yes | crm_lead.lead_id | varchar | PK | Direct | `RAW_PAYLOAD:Id::varchar` | Rename Id → lead_id. |  |
| `Company` | string | Yes | crm_lead.company_name | varchar |  | Direct | `TRIM(RAW_PAYLOAD:Company::varchar)` | Rename; trim. |  |
| `Country` | string | No | crm_lead.country_raw | varchar |  | Direct | `RAW_PAYLOAD:Country::varchar` | Original value kept as received. |  |
| `Country` | string | No | crm_lead.country_code | char(2) |  | Derived | `COALESCE(IFF(REGEXP_LIKE(UPPER(TRIM(RAW_PAYLOAD:Country::varchar)),'[A-Z]{2}'), UPPER(TRIM(RAW_PAYLOAD:Country::varchar)), NULL), ca.iso2)  /* LEFT JOIN REF.COUNTRY_ALIAS ca ON ca.alias_norm = UPPER(TRIM(Country)) */` | Normalise to ISO 3166-1 alpha-2: already 2 letters → keep; otherwise lookup in alias table ('Spain', 'España' → ES). | Any non-ISO2 value → DQ flag `LEAD_COUNTRY_ISO2` (Validity); unresolved values → NULL. |
| `LeadSource` | string (enum) | Yes | crm_lead.lead_source | varchar |  | Direct | `RAW_PAYLOAD:LeadSource::varchar` | Rename. |  |
| `Status` | string (enum) | Yes | crm_lead.status | varchar |  | Direct | `RAW_PAYLOAD:Status::varchar` | Rename. |  |
| `ConvertedOpportunityId` | string \| null | Conditional | crm_lead.converted_opportunity_id | varchar |  | Direct | `RAW_PAYLOAD:ConvertedOpportunityId::varchar` | Rename; JSON null → SQL NULL. | Must exist in silver.crm_opportunity. |
| `CreatedDate` | string | Yes | crm_lead.created_at | timestamp |  | Transformed | `CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(RAW_PAYLOAD:CreatedDate::varchar, 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZHTZM'))::timestamp_ntz` | Parse Salesforce format to UTC. |  |

## partner_settlement (bronze `RAW_PARTNER_SETTLEMENTS`)

| Source JSON Path | Type | Req | Silver Table.Column | Silver Type | Key | Status | Snowflake Expression | Rule | DQ / Notes |
|---|---|---|---|---|---|---|---|---|---|
| `settlementId` | string | Yes | partner_settlement.settlement_id | varchar | PK | Direct | `RAW_PAYLOAD:settlementId::varchar` | Rename. |  |
| `partnerId` | string | Yes | partner_settlement.partner_id | varchar | NOT NULL | Direct | `RAW_PAYLOAD:partnerId::varchar` | Rename. | Must exist in silver.partner. |
| `partnerName` | string | No | — | — |  | Not mapped | — | Redundant with silver.partner.partner_name (master data is the source of truth). | Optional DQ: compare with master name. |
| `period` | string | Yes | partner_settlement.period | varchar |  | Direct | `RAW_PAYLOAD:period::varchar` | Keep 'YYYY-MM'. |  |
| `zone` | string | Yes | partner_settlement.zone_code | varchar |  | Direct | `RAW_PAYLOAD:zone::varchar` | Rename zone → zone_code. | (partner_id, zone_code) should exist in silver.partner_zone. |
| `currency` | string (enum) | Yes | partner_settlement.currency | char(3) |  | Direct | `RAW_PAYLOAD:currency::varchar` | Cast. |  |
| `stops` | integer | Yes | partner_settlement.stops | int |  | Direct | `RAW_PAYLOAD:stops::int` | Cast. | R12: stops ≤ delivered_parcels. |
| `deliveredParcels` | integer | Yes | partner_settlement.delivered_parcels | int |  | Direct | `RAW_PAYLOAD:deliveredParcels::int` | Rename. | R11: = count of DELIVERED track_ids for partner/zone/month. |
| `failedAttempts` | integer | No | partner_settlement.failed_attempts | int |  | Direct | `RAW_PAYLOAD:failedAttempts::int` | Rename. |  |
| `costPerStop` | number | Yes | partner_settlement.cost_per_stop | decimal(8,2) |  | Transformed | `RAW_PAYLOAD:costPerStop::number(8,2)` | Rename; cast. |  |
| `totalCost` | number | Yes | partner_settlement.total_cost | decimal(12,2) |  | Transformed | `RAW_PAYLOAD:totalCost::number(12,2)` | Rename; cast. | R12: total_cost = stops × cost_per_stop. |

## tariff (bronze `RAW_TARIFFS`)

| Source JSON Path | Type | Req | Silver Table.Column | Silver Type | Key | Status | Snowflake Expression | Rule | DQ / Notes |
|---|---|---|---|---|---|---|---|---|---|
| `tariffId` | string | Yes | tariff.tariff_id | varchar | PK | Direct | `RAW_PAYLOAD:tariffId::varchar` | Rename. Compacted topic → keep latest version per tariffId. |  |
| `tariffId` | string | Yes | tariff_weight_band.tariff_id | varchar | PK (part), FK | Propagated | `RAW_PAYLOAD:tariffId::varchar` | Parent key carried to each band. Bands are fully replaced on every new tariff version. |  |
| `contactId` | string | Yes | tariff.contact_id | varchar | NOT NULL | Direct | `RAW_PAYLOAD:contactId::varchar` | Rename. | Must exist in silver.customer. |
| `product` | string (enum) | Yes | tariff.product | varchar |  | Direct | `RAW_PAYLOAD:product::varchar` | Cast. |  |
| `zone` | string | Yes | tariff.zone_code | varchar |  | Direct | `RAW_PAYLOAD:zone::varchar` | Rename zone → zone_code. |  |
| `currency` | string (enum) | Yes | tariff.currency | char(3) |  | Direct | `RAW_PAYLOAD:currency::varchar` | Cast. |  |
| `validFrom` | string (date) | Yes | tariff.valid_from | date |  | Transformed | `TRY_TO_DATE(RAW_PAYLOAD:validFrom::varchar)` | Parse ISO date. | R14: no overlapping validity per (contact_id, product, zone_code). |
| `validTo` | string (date) \| null | No | tariff.valid_to | date |  | Transformed | `TRY_TO_DATE(RAW_PAYLOAD:validTo::varchar)` | Parse; NULL = open-ended. | R14. |
| `fuelSurchargePct` | number | No | tariff.fuel_surcharge_pct | decimal(5,2) |  | Transformed | `RAW_PAYLOAD:fuelSurchargePct::number(5,2)` | Rename; keep as percentage points (12.5 = 12.5%), not a fraction. | R8. |
| `weightBands[]` | array<object> | Yes | tariff_weight_band.(one row per element) | — |  | Explode | `LATERAL FLATTEN(input => RAW_PAYLOAD:weightBands) w` | One silver.tariff_weight_band row per band. |  |
| `weightBands[] (position)` | array index | — | tariff_weight_band.band_index | int | PK (part) | Derived | `w.index + 1` | 1-based band position. |  |
| `weightBands[].fromKg` | number | Yes (within item) | tariff_weight_band.from_kg | decimal(6,2) |  | Transformed | `w.value:fromKg::number(6,2)` | Rename; cast. | R14: first from_kg = 0; contiguous (to_kg[n] = from_kg[n+1]). |
| `weightBands[].toKg` | number | Yes (within item) | tariff_weight_band.to_kg | decimal(6,2) |  | Transformed | `w.value:toKg::number(6,2)` | Rename; cast. | R14. |
| `weightBands[].price` | number | Yes (within item) | tariff_weight_band.price | decimal(10,2) |  | Transformed | `w.value:price::number(10,2)` | Cast. | R14: price increasing with band. |

## gl_extract_row (bronze `RAW_GL_EXTRACT`)

| Source JSON Path | Type | Req | Silver Table.Column | Silver Type | Key | Status | Snowflake Expression | Rule | DQ / Notes |
|---|---|---|---|---|---|---|---|---|---|
| `company_code` | string | Yes | gl_balance.company_code | varchar | PK (part) | Direct | `RAW_PAYLOAD:company_code::varchar` | Cast. Keep only the latest complete batch per (company_code, period) (_SOURCE_TXN_ID). | Completeness: COUNT(*) per _SOURCE_TXN_ID = _SOURCE_TXN_COUNT. |
| `country_code` | string | Yes | gl_balance.country_code | char(2) |  | Direct | `RAW_PAYLOAD:country_code::varchar` | Cast. | VLP_XX must match country XX. |
| `period` | string | Yes | gl_balance.period | varchar | PK (part) | Direct | `RAW_PAYLOAD:period::varchar` | Keep 'YYYY-MM'. |  |
| `gl_account` | string | Yes | gl_balance.gl_account | varchar | PK (part) | Direct | `RAW_PAYLOAD:gl_account::varchar` | Keep as string. |  |
| `account_name` | string (enum) | Yes | gl_balance.account_name | varchar |  | Direct | `RAW_PAYLOAD:account_name::varchar` | Cast. |  |
| `amount_eur` | number | Yes | gl_balance.amount_eur | decimal(14,2) |  | Transformed | `RAW_PAYLOAD:amount_eur::number(14,2)` | Cast. | R13: revenue per country/month = Σ invoice net × FX (±0.5%). |

## master_data (customer) (bronze `RAW_MASTER_DATA`)

| Source JSON Path | Type | Req | Silver Table.Column | Silver Type | Key | Status | Snowflake Expression | Rule | DQ / Notes |
|---|---|---|---|---|---|---|---|---|---|
| `entity` | string (const) | Yes | customer.(row filter) | — |  | Routing | `WHERE RAW_PAYLOAD:entity::varchar = 'customer'` | Routes to silver.customer. Every version is kept (insert when (contact_id, updated_at) is new). |  |
| `contactId` | string | Yes | customer.contact_id | varchar | PK (part) | Direct | `RAW_PAYLOAD:contactId::varchar` | Rename. |  |
| `groupCustomerId` | string | Yes | customer.group_customer_id | varchar |  | Direct | `RAW_PAYLOAD:groupCustomerId::varchar` | Rename. |  |
| `name` | string | Yes | customer.customer_name | varchar |  | Direct | `TRIM(RAW_PAYLOAD:name::varchar)` | Rename name → customer_name. |  |
| `countryCode` | string | Yes | customer.country_code | char(2) |  | Direct | `RAW_PAYLOAD:countryCode::varchar` | Rename. |  |
| `segment` | string (enum) | Yes | customer.segment | varchar |  | Direct | `RAW_PAYLOAD:segment::varchar` | Cast. |  |
| `vatNumber` | string | Yes | customer.vat_number | varchar |  | Direct | `UPPER(REPLACE(RAW_PAYLOAD:vatNumber::varchar,' ',''))` | Remove spaces; upper. | R15: prefix = country_code. |
| `updatedAt` | string (date-time) | Yes | customer.updated_at | timestamp | PK (part), NOT NULL | Transformed | `CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(RAW_PAYLOAD:updatedAt::varchar))::timestamp_ntz` | Parse to UTC. Version key. | Basis for SCD2 in gold (valid_from = updated_at). |

## master_data (depot) (bronze `RAW_MASTER_DATA`)

| Source JSON Path | Type | Req | Silver Table.Column | Silver Type | Key | Status | Snowflake Expression | Rule | DQ / Notes |
|---|---|---|---|---|---|---|---|---|---|
| `entity` | string (const) | Yes | depot.(row filter) | — |  | Routing | `WHERE RAW_PAYLOAD:entity::varchar = 'depot'` | Routes to silver.depot. Latest version per gln (order by _SOURCE_TS, _KAFKA_OFFSET). |  |
| `gln` | string | Yes | depot.gln | char(13) | PK | Direct | `RAW_PAYLOAD:gln::varchar` | Cast. | R15 check digit. |
| `name` | string | Yes | depot.depot_name | varchar |  | Direct | `TRIM(RAW_PAYLOAD:name::varchar)` | Rename. |  |
| `type` | string (enum) | Yes | depot.depot_type | varchar |  | Direct | `RAW_PAYLOAD:type::varchar` | Rename type → depot_type. |  |
| `countryCode` | string | Yes | depot.country_code | char(2) |  | Direct | `RAW_PAYLOAD:countryCode::varchar` | Rename. |  |
| `city` | string | Yes | depot.city | varchar |  | Direct | `TRIM(RAW_PAYLOAD:city::varchar)` | Trim. |  |
| `postalCode` | string | No | depot.postal_code | varchar |  | Direct | `TRIM(RAW_PAYLOAD:postalCode::varchar)` | Rename. |  |

## master_data (partner) (bronze `RAW_MASTER_DATA`)

| Source JSON Path | Type | Req | Silver Table.Column | Silver Type | Key | Status | Snowflake Expression | Rule | DQ / Notes |
|---|---|---|---|---|---|---|---|---|---|
| `entity` | string (const) | Yes | partner.(row filter) | — |  | Routing | `WHERE RAW_PAYLOAD:entity::varchar = 'partner'` | Routes to silver.partner and silver.partner_zone. Latest version per partnerId. |  |
| `partnerId` | string | Yes | partner.partner_id | varchar | PK | Direct | `RAW_PAYLOAD:partnerId::varchar` | Rename. |  |
| `partnerId` | string | Yes | partner_zone.partner_id | varchar | PK (part), FK | Propagated | `RAW_PAYLOAD:partnerId::varchar` | Parent key carried to each zone row. |  |
| `name` | string | Yes | partner.partner_name | varchar |  | Direct | `TRIM(RAW_PAYLOAD:name::varchar)` | Rename. |  |
| `countryCode` | string | Yes | partner.country_code | char(2) |  | Direct | `RAW_PAYLOAD:countryCode::varchar` | Rename. |  |
| `zones[]` | array<string> | Yes | partner_zone.(one row per element) | — |  | Explode | `LATERAL FLATTEN(input => RAW_PAYLOAD:zones) z` | One row per zone; the partner's zone set is replaced on every new version (delete + insert). | R16: each zone has exactly one partner. |
| `zones[] (item)` | string | Yes (within array) | partner_zone.zone_code | varchar | PK (part) | Direct | `z.value::varchar` | Cast. |  |
| `baseDepotGln` | string | Yes | partner.base_depot_gln | char(13) |  | Direct | `RAW_PAYLOAD:baseDepotGln::varchar` | Rename. | Must exist in silver.depot. |

## Gaps & Decisions

| # | Type | Source | Field / Topic | Silver Impact | Recommendation | Status |
|---|---|---|---|---|---|---|
| 1 | Gap | invoice | relatedInvoiceNumber | No column in silver.invoice → R9 (credit note references an existing invoice) cannot be checked in silver; credit notes cannot be linked to the original invoice in gold. | Add related_invoice_number varchar to silver.invoice (and to gold fact/degenerate dimension). | Done (silver.invoice, Phase 7) |
| 2 | Gap | veloship_shipment | Consignee.Address.Street / StreetNumber | Not in silver.veloship_shipment. | Keep out of silver (data minimisation, GDPR) unless a use case needs geocoding; remains in bronze with restricted access. | Proposed: keep dropped |
| 3 | Drop | tracking_event | device.firmware | Not in silver. | Technical attribute; leave in bronze. | Proposed: keep dropped |
| 4 | Drop | partner_settlement | partnerName | Not in silver (redundant). | Use silver.partner.partner_name; optional DQ to compare names. | Proposed: keep dropped |
| 5 | Decision | veloship_shipment | ShipmentReference[] / ShipmentUnitReference[] | Arrays (up to 3 refs) mapped to a single varchar. | Pipe-delimited string. Switch to VARIANT or child table if lookups by single reference are needed. | Open |
| 6 | Decision | veloship_shipment | ShipmentUnit[] ↔ ParcelData[] | No explicit link between request unit and response parcel other than array position. | Zip by position (FULL OUTER join on index) so R1 violations surface as rows with NULL track_id or NULL weight. | Proposed |
| 7 | Decision | tracking_event | bizStep | Full CBV URN vs short value. | Strip 'urn:epcglobal:cbv:bizstep:' prefix in silver. | Open |
| 8 | Decision | tracking_event | epcList[0] → sscc | URN format omits the GS1 check digit. | Build 18-digit SSCC with the dbt macros sscc_body_from_urn / sscc_from_body (extension digit + prefix + serial + check digit); no UDF. | Done (stg_tracking_events) |
| 9 | Decision | crm_object (Lead) | Country (free text) | Values like 'Spain', 'España', 'ES'. | Clean contract: ISO 3166-1 alpha-2. Any other value is a Validity anomaly (rule `LEAD_COUNTRY_ISO2`, flagged even when REF.COUNTRY_ALIAS resolves it); alias → ISO2 lookup still fills country_code, unresolved → NULL; keep country_raw. | Decided |
| 10 | Decision | crm_object / master_data / tariff | Compacted topics (multiple versions per key) | Silver PK holds one row per key (except customer). | Keep latest version (QUALIFY ROW_NUMBER over key ORDER BY source timestamp, _KAFKA_OFFSET DESC). Customer keeps all versions. | Proposed |
| 11 | Decision | master_data (depot, partner) | No updatedAt in schema | Version ordering must rely on bronze metadata. | Order by _SOURCE_TS then _KAFKA_OFFSET. | Proposed |
| 12 | Decision | veloship_shipment / tracking_event | _loaded_at | Only two silver tables carry a load timestamp. | Consider adding _loaded_at (from bronze _LOAD_TS) to all silver tables for freshness monitoring and incremental gold loads. | Done: every silver table carries _loaded_at and _inserted_at (incremental watermark) |
| 13 | Decision | gl_extract_row | Re-sent batches | Same (company_code, period, gl_account) may arrive in more than one batch. | Keep only the latest complete batch per (company_code, period), identified by _SOURCE_TXN_ID. | Proposed |
