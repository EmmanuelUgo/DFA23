WITH stg_location AS (

    SELECT
        sensor_id as sensor_id_raw,
        trim(regexp_substr(sensor_id, '_.+_'), '_')::string AS sensor_id,
        replace(trim(sensor_id, '*'),'#','') as location_id,
        (CASE
            WHEN location_name = 'NA' THEN NULL ELSE
                location_name
        END)::string AS location_name,
        (CASE
            WHEN latitude = 'NA' THEN NULL ELSE
                latitude
        END)::decimal(15, 8) AS latitude,
        (CASE
            WHEN longitude = 'NA' THEN NULL ELSE
                longitude
        END)::decimal(15, 8) AS longitude,
        (CASE
            WHEN elevation = 'NA' THEN NULL ELSE
                elevation
        END)::decimal(15, 2) AS elevation,
        (CASE
            WHEN region = 'NA' THEN NULL ELSE
                region
        END)::string AS region
    from {{ source('DFA23', 'LOCATIONDATARAW') }}
    WHERE sensor_id IS NOT NULL
),
stg_location_final as (
    SELECT 
        sensor_id_raw,
        sensor_id,
        replace(location_id, sensor_id) as location_id,
        LOCATION_NAME,
        LATITUDE,
        LONGITUDE,
        ELEVATION
    FROM stg_location 
    WHERE CONCAT(sensor_id, replace(location_id, sensor_id)) NOT IN ('S747I__SWION', 'S680L__BPTSW', 'S519Z__KNAND')
)

select * FROM stg_location_final 