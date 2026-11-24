{#-
  GS1 keys in SQL (same logic as generator/src/veloparc_gen/gs1.py). Mod-10 check digit: weights 3,1,3,... from
  the rightmost digit of the body. `length` is the fixed body length (12 for a GLN, 17 for an SSCC).
-#}
{% macro gs1_check_digit(body, length) -%}
    mod(10 - mod(
        {%- for i in range(length) %}
        try_to_number(substr({{ body }}, {{ length - i }}, 1)) * {{ 3 if i % 2 == 0 else 1 }}{% if not loop.last %} +{% endif %}
        {%- endfor %}
    , 10), 10)
{%- endmacro %}

{#- True when a 13-digit GLN / 18-digit SSCC has the right check digit (R15); NULL input gives NULL. -#}
{% macro gs1_is_valid(code, length) -%}
    (length({{ code }}) = {{ length }} and try_to_number({{ code }}) is not null
     and {{ gs1_check_digit('substr(' ~ code ~ ', 1, ' ~ (length - 1) ~ ')', length - 1) }}
         = try_to_number(substr({{ code }}, {{ length }}, 1)))
{%- endmacro %}

{#-
  EPC pure-identity URN, as the tracking_event contract sends it (urn:epc:id:sscc:<prefix>.<extension><serial>, no
  check digit), to the 17-digit SSCC body: extension digit + company prefix + rest of the serial (mapping decision
  #8). NULL when prefix and serial do not add up to 17 digits.
-#}
{% macro sscc_body_from_urn(urn) -%}
    {%- set ref = "split_part(" ~ urn ~ ", ':', 5)" -%}
    {%- set prefix = "split_part(" ~ ref ~ ", '.', 1)" -%}
    {%- set serial = "split_part(" ~ ref ~ ", '.', 2)" -%}
    iff(length({{ prefix }}) + length({{ serial }}) = 17,
        left({{ serial }}, 1) || {{ prefix }} || substr({{ serial }}, 2),
        null)
{%- endmacro %}

{#- 17-digit body (a column, computed once) to the 18-digit SSCC with its check digit. -#}
{% macro sscc_from_body(body) -%}
    iff({{ body }} is null, null, {{ body }} || to_varchar({{ gs1_check_digit(body, 17) }}))
{%- endmacro %}

{#- One-expression version, for literals and tests. -#}
{% macro sscc_from_urn(urn) -%}
    {{ sscc_from_body('(' ~ sscc_body_from_urn(urn) ~ ')') }}
{%- endmacro %}
