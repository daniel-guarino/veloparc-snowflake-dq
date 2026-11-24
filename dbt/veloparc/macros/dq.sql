{#-
  Standard columns of every dq_rule_* model (Phase 9): one row per violation.
    rule_id, business_key (same format as the generator's anomaly_manifest / rule_violations), source,
    country_code, event_date, details, and the bronze lineage of the offending record for root cause.
-#}
{% macro dq_violation(rule_id, business_key, source, country_code, event_date, details,
                      event_id='null', txn_id='null', file_name='null', kafka_offset='null') -%}
    '{{ rule_id }}' as rule_id,
    ({{ business_key }})::varchar as business_key,
    '{{ source }}' as source,
    ({{ country_code }})::varchar as country_code,
    ({{ event_date }})::date as event_date,
    ({{ details }})::varchar as details,
    ({{ event_id }})::varchar as _event_id,
    ({{ txn_id }})::varchar as _source_txn_id,
    ({{ file_name }})::varchar as _file_name,
    ({{ kafka_offset }})::number as _kafka_offset
{%- endmacro %}

{#- Tracking events with the consignee country of their parcel (scans carry no country of their own). -#}
{% macro events_with_country() -%}
    select e.*, s.consignee_country, s.consignee_zip
    from {{ ref('tracking_event') }} e
    left join {{ ref('veloship_parcel') }} p on p.track_id = e.track_id
    left join {{ ref('veloship_shipment') }} s on s.message_id = p.message_id
{%- endmacro %}
