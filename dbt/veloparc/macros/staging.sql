{#- Bronze lineage columns every stg_ model carries (root cause in Phase 9). -#}
{% macro bronze_meta() -%}
    _EVENT_ID as _event_id,
    _OP_TYPE as _op_type,
    _SOURCE_TS as _source_ts,
    _SOURCE_SYSTEM as _source_system,
    _SOURCE_TXN_ID as _source_txn_id,
    _KAFKA_TOPIC as _kafka_topic,
    _KAFKA_PARTITION as _kafka_partition,
    _KAFKA_OFFSET as _kafka_offset,
    _KAFKA_TS as _kafka_ts,
    _FILE_NAME as _file_name,
    _LOAD_TS as _loaded_at,
    _INSERTED_AT as _inserted_at
{%- endmacro %}

{#- Drop technical duplicates and producer resends (ADR 0004): silver has none. -#}
{% macro not_duplicate() -%}
    not _IS_TECH_DUP and not _IS_RESEND
{%- endmacro %}

{#- ISO-8601 text with an offset (or Z) to UTC timestamp_ntz; NULL if unparsable. -#}
{% macro ts_utc(expr, format=none) -%}
    {%- if format -%}
    convert_timezone('UTC', try_to_timestamp_tz({{ expr }}, '{{ format }}'))::timestamp_ntz
    {%- else -%}
    convert_timezone('UTC', try_to_timestamp_tz({{ expr }}))::timestamp_ntz
    {%- endif -%}
{%- endmacro %}

{#- Salesforce date-time: offset without a colon (2026-09-01T10:00:00.000+0000). -#}
{% macro ts_salesforce_utc(expr) -%}
    {{ ts_utc(expr, 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZHTZM') }}
{%- endmacro %}

{#- Incremental watermark on the bronze insert time, which only grows per table (unlike the source or load times). -#}
{% macro new_rows_only(column='_inserted_at') -%}
    {%- if is_incremental() -%}
    and {{ column }} > (select coalesce(max({{ column }}), '1900-01-01'::timestamp_tz) from {{ this }})
    {%- endif -%}
{%- endmacro %}

{#- Latest version first: the order used to keep one row per key (mapping decisions #10, #11). -#}
{% macro latest_first() -%}
    _source_ts desc nulls last, _kafka_ts desc nulls last, _kafka_offset desc
{%- endmacro %}
