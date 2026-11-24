-- One row per violation of any DQ rule, with the rule's dimension, severity and owner from the catalog
-- (seeds/dq_rule_catalog.csv) and the bronze lineage of the offending record. Base of the scorecard and the detection evaluation.
with violations as (
    {% for rule in ['schema', 'r01', 'r02', 'r03', 'r04', 'r05', 'r06', 'r07', 'r08', 'r09', 'r10', 'r11', 'r12', 'r13',
                 'r14', 'r15', 'r16', 'lead_country_iso2'] %}
    select * from {{ ref('dq_rule_' ~ rule) }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
select
    {{ surrogate_key(['v.rule_id', 'v.business_key', 'v.details']) }} as issue_id,
    v.*,
    c.dimension,
    c.severity,
    c.owner
from violations v
left join {{ ref('dq_rule_catalog') }} c on c.rule_id = v.rule_id
