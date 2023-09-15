WITH stg_location AS (

    SELECT
        trim(regexp_substr(sensor_id, '_.+_'), '_')::string AS sensor_id,
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
)

SELECT * FROM stg_location
