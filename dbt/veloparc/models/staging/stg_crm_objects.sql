-- CRM leads and opportunities from bronze, without technical duplicates or resends.
-- object_type routes rows to crm_lead / crm_opportunity; fields of the other type are NULL.
select
    RAW_PAYLOAD:attributes:type::varchar as object_type,
    RAW_PAYLOAD:Id::varchar as crm_id,
    -- opportunity
    RAW_PAYLOAD:AccountId::varchar as account_id,
    RAW_PAYLOAD:VLP_ContactID__c::varchar as contact_id,
    trim(RAW_PAYLOAD:Name::varchar) as opportunity_name,
    RAW_PAYLOAD:StageName::varchar as stage,
    try_cast(RAW_PAYLOAD:Amount::varchar as number(14, 2)) as amount,
    RAW_PAYLOAD:CurrencyIsoCode::varchar as currency,
    try_to_date(RAW_PAYLOAD:CloseDate::varchar) as close_date,
    upper(RAW_PAYLOAD:Country__c::varchar) as opportunity_country_code,
    try_cast(RAW_PAYLOAD:Expected_Parcels_Month__c::varchar as integer) as expected_parcels_month,
    {{ ts_salesforce_utc('RAW_PAYLOAD:LastModifiedDate::varchar') }} as last_modified_at,
    -- lead
    trim(RAW_PAYLOAD:Company::varchar) as company_name,
    RAW_PAYLOAD:Country::varchar as lead_country_raw,
    RAW_PAYLOAD:LeadSource::varchar as lead_source,
    RAW_PAYLOAD:Status::varchar as lead_status,
    RAW_PAYLOAD:ConvertedOpportunityId::varchar as converted_opportunity_id,
    {{ ts_salesforce_utc('RAW_PAYLOAD:CreatedDate::varchar') }} as created_at,
    {{ bronze_meta() }}
from {{ source('bronze', 'V_RAW_CRM_OBJECTS') }}
where {{ not_duplicate() }}
