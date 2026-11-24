{#- Surrogate key of the unknown member in every dimension (mapping decision #4). -#}
{% macro unknown_sk() -%}'-1'{%- endmacro %}

{#- YYYYMMDD smart key of a date; -1 when the date is NULL. -#}
{% macro date_key(expr) -%}
    coalesce(to_number(to_char({{ expr }}, 'YYYYMMDD')), -1)
{%- endmacro %}

{#-
  Point-in-time lookup in dim_customer (mapping decision #6): the version valid at `ts`; if the business date is
  before the first version received, the first version; if the contact is not in master data, the unknown member.
  Emits two LEFT JOINs; select the key with customer_sk_from(alias).
-#}
{% macro join_customer_at(contact, ts, alias='dc') -%}
    left join {{ ref('dim_customer') }} {{ alias }}
        on {{ alias }}.contact_id = {{ contact }}
        and {{ ts }} >= {{ alias }}.valid_from
        and {{ ts }} < coalesce({{ alias }}.valid_to, '9999-12-31'::timestamp_ntz)
    left join {{ ref('dim_customer') }} {{ alias }}_first
        on {{ alias }}_first.contact_id = {{ contact }}
        and {{ alias }}_first.version_number = 1
{%- endmacro %}

{% macro customer_sk_from(alias='dc') -%}
    coalesce({{ alias }}.customer_sk, {{ alias }}_first.customer_sk, {{ unknown_sk() }})
{%- endmacro %}
