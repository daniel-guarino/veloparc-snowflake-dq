{#- A DQ rule model passes when it returns no rows. Severity and meta come from the model's yml. -#}
{% test no_violations(model) %}
select * from {{ model }}
{% endtest %}
