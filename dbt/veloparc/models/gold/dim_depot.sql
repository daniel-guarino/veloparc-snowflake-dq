-- SCD1: latest version per GLN; unknown member '0000000000000' for scans at a GLN not in master data (R6).
select gln as depot_gln, depot_name, depot_type, country_code, city
from {{ ref('depot') }}
union all
select '0000000000000', 'Unknown depot', null, null, null
