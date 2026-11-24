{% docs col__event_id %}
CloudEvents id of the bronze message (header `ce_id`) the row was built from. Joins back to `BRONZE.V_RAW_*._EVENT_ID`.
{% enddocs %}

{% docs col__source_ts %}
Business time of the source message, UTC (bronze `_SOURCE_TS`; per topic, see the ingestion doc).
{% enddocs %}

{% docs col__kafka_ts %}
Kafka timestamp, UTC: simulated broker time for landing files, producer CreateTime for the connector path.
{% enddocs %}

{% docs col__source_txn_id %}
Source batch id (header `vlp_batch_id`): the transaction the message belonged to, for root-cause analysis.
{% enddocs %}

{% docs col__kafka_offset %}
Kafka offset of the message. Simulated in landing files, broker offset on the connector path (they overlap).
{% enddocs %}

{% docs col__file_name %}
Landing file the row was loaded from (`METADATA$FILENAME`); NULL for rows that came through the Kafka connector.
{% enddocs %}

{% docs col__loaded_at %}
Arrival in Snowflake, UTC (bronze `_LOAD_TS`: COPY scan start, or the connector push time). For freshness.
{% enddocs %}

{% docs col__inserted_at %}
Insert time into the bronze table (`_INSERTED_AT`). Watermark of the incremental silver models: it only grows.
{% enddocs %}
