-- Detection evaluation per rule, for the latest backfill run (the answer key covers the backfill only):
--  * against the injected anomalies (DQ.ANOMALY_GROUND_TRUTH): injected, found, missed, detections not injected,
--    precision and recall. Detections not injected include natural violations (e.g. R5 late scans).
--  * against the generator's own rule checker (DQ.GENERATOR_RULE_VIOLATIONS), which also counts natural violations.
-- A detection matches an expected key when the keys are equal or the detection is a line of it ('INV#3' for 'INV').
-- Detections of connector rows (no file name; the publish of 2026-11-01) are left out: the answer key covers the
-- backfill files only. Detections without lineage (group rules such as R11, R13, R16) are kept.
with run as (
    select RUN_ID as run_id
    from {{ source('generator', 'GENERATOR_RUNS') }}
    where MANIFEST:mode::varchar = 'backfill'
    qualify row_number() over (order by _LOADED_AT desc) = 1
),
detected as (
    select distinct i.rule_id, i.business_key
    from {{ ref('dq_issue') }} i
    where not (i._file_name is null and i._event_id is not null)
),
injected as (
    select distinct g.RULE_ID as rule_id, g.BUSINESS_KEY as business_key
    from {{ source('generator', 'ANOMALY_GROUND_TRUTH') }} g join run on run.run_id = g.RUN_ID
    where g.RULE_ID is not null
),
checker as (
    select distinct v.RULE_ID as rule_id, v.BUSINESS_KEY as business_key
    from {{ source('generator', 'GENERATOR_RULE_VIOLATIONS') }} v join run on run.run_id = v.RUN_ID
),
{%- for expected in ['injected', 'checker'] %}
{{ expected }}_pairs as (
    select e.rule_id, e.business_key as expected_key, d.business_key as detected_key
    from {{ expected }} e
    join detected d
      on d.rule_id = e.rule_id
     and (d.business_key = e.business_key or startswith(d.business_key, e.business_key || '#'))
),
{{ expected }}_stats as (
    select
        r.rule_id,
        (select count(*) from {{ expected }} e where e.rule_id = r.rule_id) as expected,
        (select count(distinct expected_key) from {{ expected }}_pairs p where p.rule_id = r.rule_id) as found,
        (select count(distinct detected_key) from {{ expected }}_pairs p where p.rule_id = r.rule_id) as detected_matched
    from {{ ref('dq_rule_catalog') }} r
),
{%- endfor %}
detected_stats as (
    select rule_id, count(*) as detected from detected group by 1
)
select
    c.rule_id,
    c.dimension,
    c.owner,
    coalesce(d.detected, 0) as detected,
    i.expected as injected,
    i.found as injected_found,
    i.expected - i.found as injected_missed,
    coalesce(d.detected, 0) - i.detected_matched as detected_not_injected,
    round(i.detected_matched / nullif(d.detected, 0), 3) as precision_vs_injected,
    round(i.found / nullif(i.expected, 0), 3) as recall_vs_injected,
    k.expected as checker_violations,
    k.found as checker_found,
    coalesce(d.detected, 0) - k.detected_matched as detected_not_in_checker,
    round(k.detected_matched / nullif(d.detected, 0), 3) as precision_vs_checker,
    round(k.found / nullif(k.expected, 0), 3) as recall_vs_checker
from {{ ref('dq_rule_catalog') }} c
left join detected_stats d on d.rule_id = c.rule_id
left join injected_stats i on i.rule_id = c.rule_id
left join checker_stats k on k.rule_id = c.rule_id
