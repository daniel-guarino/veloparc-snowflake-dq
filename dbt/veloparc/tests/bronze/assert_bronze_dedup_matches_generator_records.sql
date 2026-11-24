-- Checks the dedup flags: per topic, landing-file rows that are neither technical duplicates nor producer resends
-- equal the live records the generator produced (one row per ce_id). Returns the topics that differ.
with expected as ({{ generator_expected_counts() }}),
actual as (
    select _KAFKA_TOPIC as kafka_topic, count_if(not _IS_TECH_DUP and not _IS_RESEND) as deduped_rows
    from ({{ bronze_file_rows() }})
    group by 1
)
select coalesce(e.kafka_topic, a.kafka_topic) as kafka_topic, e.expected_records, a.deduped_rows
from expected e
full outer join actual a on a.kafka_topic = e.kafka_topic
where not equal_null(e.expected_records, a.deduped_rows)
