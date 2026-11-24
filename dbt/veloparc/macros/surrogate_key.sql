{#-
  Hash surrogate key from business key columns, like dbt_utils.generate_surrogate_key: MD5 of the values cast to
  text, NULLs replaced by a marker, joined with '-'. Written here so the project needs no Hub package (dbt Projects
  on Snowflake would need an external access integration to download one).
-#}
{% macro surrogate_key(columns) -%}
    md5(
        {%- for column in columns %}
        coalesce(cast({{ column }} as varchar), '_dbt_null_'){% if not loop.last %} || '-' ||{% endif %}
        {%- endfor %}
    )
{%- endmacro %}
