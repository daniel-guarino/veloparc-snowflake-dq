-- The GS1 macros against the cases of generator/tests/test_gs1.py and contracts/examples. Returns failing cases.
with cases as (
    select 'sscc_from_urn' as case_name,
           {{ sscc_from_urn("'urn:epc:id:sscc:5601234.3000000001'") }} as actual, '356012340000000017' as expected
    union all
    select 'sscc_from_urn malformed', {{ sscc_from_urn("'urn:epc:id:sscc:5601234.30000001'") }}, null
    union all
    select 'gln valid', to_varchar({{ gs1_is_valid("'5601234000016'", 13) }}), 'true'
    union all
    select 'gln invalid', to_varchar({{ gs1_is_valid("'5601234000017'", 13) }}), 'false'
    union all
    select 'gln 12 digits', to_varchar({{ gs1_is_valid("'560123400001'", 13) }}), 'false'
    union all
    select 'sscc valid', to_varchar({{ gs1_is_valid("'356012340000000024'", 18) }}), 'true'
)
select * from cases where not equal_null(actual, expected)
