WITH stg_irrigation AS (

    SELECT
        trim(regexp_substr(sensor_id, '_.+_'), '_')::string AS sensor_id,
        try_to_timestamp(timestamp::varchar, 'mm/dd/yyyy hh24:mi') AS timestamp,
        (CASE
            WHEN irrigation_method = 'NA' THEN NULL ELSE irrigation_method
        END)::string AS irrigation_method,
        (CASE
            WHEN water_source = 'NA' THEN NULL ELSE water_source
        END)::string AS water_source,
        (CASE
            WHEN irrigation_duration_min = 'NA' THEN NULL ELSE
                irrigation_duration_min
        END)::decimal(15, 2) AS irrigation_duration_min
    from {{ source('DFA23', 'IRRIGATIONDATARAW') }}

)

SELECT * FROM stg_irrigation
