-- Latest version per lead. country_code is ISO 3166-1 alpha-2: kept when the raw value already is one, otherwise
-- looked up in REF.COUNTRY_ALIAS; country_raw keeps what was received (mapping decision #9: any non-ISO2 value is a
-- Validity anomaly, flagged in Phase 9 even when the alias resolves it).
{{ config(unique_key='lead_id', incremental_strategy='merge') }}

with candidates as (
    select
        crm_id as lead_id, company_name, lead_country_raw as country_raw, lead_source, lead_status as status,
        converted_opportunity_id, created_at,
        _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
    from {{ ref('stg_crm_objects') }}
    where object_type = 'Lead' {{ new_rows_only() }}
    {% if is_incremental() %}
    union all
    select
        lead_id, company_name, country_raw, lead_source, status, converted_opportunity_id, created_at,
        _event_id, _source_ts, _kafka_ts, _source_txn_id, _kafka_offset, _file_name, _loaded_at, _inserted_at
    from {{ this }}
    {% endif %}
),
latest as (
    select * from candidates
    qualify row_number() over (partition by lead_id order by {{ latest_first() }}) = 1
)
select
    l.lead_id,
    l.company_name,
    l.country_raw,
    iff(regexp_like(upper(trim(l.country_raw)), '[A-Z]{2}'), upper(trim(l.country_raw)), ca.ISO2) as country_code,
    l.lead_source,
    l.status,
    l.converted_opportunity_id,
    l.created_at,
    l._event_id, l._source_ts, l._kafka_ts, l._source_txn_id, l._kafka_offset, l._file_name, l._loaded_at,
    l._inserted_at
from latest l
left join {{ source('ref', 'COUNTRY_ALIAS') }} ca on ca.ALIAS_NORM = upper(trim(l.country_raw))
