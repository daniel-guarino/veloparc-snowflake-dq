-- Phase 6 check (replaces bronze-check): per topic, the landing-file rows in bronze, without technical duplicates,
-- equal the messages the generator wrote (producer retries included). Returns the topics that differ.
with expected as ({{ generator_expected_counts() }}),
actual as (
    select _KAFKA_TOPIC as kafka_topic, count_if(not _IS_TECH_DUP) as loaded_messages
    from ({{ bronze_file_rows() }})
    group by 1
)
select coalesce(e.kafka_topic, a.kafka_topic) as kafka_topic, e.expected_messages, a.loaded_messages
from expected e
full outer join actual a on a.kafka_topic = e.kafka_topic
where not equal_null(e.expected_messages, a.loaded_messages)
