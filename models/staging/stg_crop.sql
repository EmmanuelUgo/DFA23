WITH stg_crop AS (

    SELECT
        TRY_TO_TIMESTAMP(timestamp::varchar, 'mm/dd/yyyy hh24:mi') AS timestamp,
        (CASE WHEN crop_type = 'NA' THEN NULL ELSE crop_type END)::string
            AS crop_type,
        (CASE WHEN crop_yield = 'NA' THEN NULL ELSE crop_yield END)::decimal(
            15, 2
        ) AS crop_yield,
        (CASE
            WHEN growth_stage = 'NA' THEN NULL ELSE growth_stage
        END)::string AS growth_stage,
        (CASE WHEN pest_issue = 'NA' THEN NULL ELSE pest_issue END)::string
            AS PEST_TYPE
    from {{ source('DFA23', 'CROPDATARAW') }}
    where timestamp is not null

)

SELECT * FROM stg_crop
