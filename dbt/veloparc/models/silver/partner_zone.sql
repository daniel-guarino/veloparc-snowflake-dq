-- Zones of each partner's latest version; a new version replaces the partner's whole zone set (delete + insert).
{{ config(unique_key='partner_id', incremental_strategy='delete+insert') }}

with latest as (
    select partner_id, zones, _event_id, _source_ts, _kafka_ts, _kafka_offset, _inserted_at
    from {{ ref('stg_master_data') }}
    where entity = 'partner'
    {% if is_incremental() %}
    and partner_id in (select partner_id from {{ ref('stg_master_data') }} where entity = 'partner' {{ new_rows_only() }})
    {% endif %}
    qualify row_number() over (partition by partner_id order by {{ latest_first() }}) = 1
)
select
    l.partner_id,
    z.value::varchar as zone_code,
    l._event_id,
    l._inserted_at
from latest l, lateral flatten(input => l.zones) z
