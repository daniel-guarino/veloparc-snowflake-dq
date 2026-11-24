{#- Rows that came from landing files (not the connector), across all bronze views, with the dedup flags. -#}
{% macro bronze_file_rows() -%}
    {%- set views = [
        'V_RAW_VELOSHIP_SHIPMENTS', 'V_RAW_TRACKING_EVENTS', 'V_RAW_INVOICES', 'V_RAW_CRM_OBJECTS',
        'V_RAW_PARTNER_SETTLEMENTS', 'V_RAW_TARIFFS', 'V_RAW_GL_EXTRACT', 'V_RAW_MASTER_DATA',
    ] -%}
    {%- for view in views %}
    select _KAFKA_TOPIC, _IS_TECH_DUP, _IS_RESEND from {{ source('bronze', view) }} where _FILE_NAME is not null
    {% if not loop.last %}union all{% endif %}
    {%- endfor %}
{%- endmacro %}

{#- Expected counts per topic from the latest backfill run_manifest.json: messages written and live records. -#}
{% macro generator_expected_counts() -%}
    with latest_run as (
        select MANIFEST
        from {{ source('generator', 'GENERATOR_RUNS') }}
        where MANIFEST:mode::varchar = 'backfill'
        qualify row_number() over (order by _LOADED_AT desc) = 1
    )
    select
        t.value:name::varchar as kafka_topic,
        t.value:messages::number as expected_messages,
        get(latest_run.MANIFEST:records, t.key)::number as expected_records
    from latest_run, lateral flatten(input => latest_run.MANIFEST:sink:topics) t
    where t.value:name::varchar <> 'vlp.dlq.s3-sink.v1'
{%- endmacro %}
