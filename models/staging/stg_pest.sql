with stg_pest as (

    select
        try_to_timestamp(timestamp, 'mm/dd/yyyy hh24:mi') as timestamp,
        (case
            when pest_type = 'NA' then NULL else pest_type
        end)::string as pest_type,
        pest_severity,
        initcap(trim(regexp_substr(
            (case
                when pest_description = 'NA' then NULL else pest_description
            end)::string, ':.+'
        ), ':')) as pest_description
    from {{ source('DFA23', 'PESTDATARAW') }}

)

select * from stg_pest
