-- LEAD_COUNTRY_ISO2 (mapping decision #9): the lead's Country must be 2 upper-case letters as received. Silver still
-- resolves aliases into country_code; the source value is flagged here.
select {{ dq_violation('LEAD_COUNTRY_ISO2', 'lead_id', 'crm_object', 'country_code', 'created_at',
        "'Country ' || country_raw", '_event_id', '_source_txn_id', '_file_name', '_kafka_offset') }}
from {{ ref('crm_lead') }}
where country_raw is not null
  and not (length(country_raw) = 2 and translate(country_raw, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', '') = '')
