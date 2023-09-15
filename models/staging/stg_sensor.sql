WITH stg_sensor AS (

    SELECT
        cast(trim(sensor_id, '"') AS string) AS sensor_id,
        try_to_timestamp(trim(timestamp, '"'), 'yyyy-mm-dd hh24:mi:ss') AS timestamp,
        cast(
            CASE
                WHEN temperature = 'NA' THEN NULL ELSE temperature
            END AS decimal(15, 2)
        ) AS temperature,
        cast(
            CASE WHEN humidity = 'NA' THEN NULL ELSE humidity END AS decimal(
                15, 2
            )
        ) AS humidity,
        cast(
            CASE
                WHEN soil_moisture = 'NA' THEN NULL ELSE soil_moisture
            END AS decimal(15, 2)
        ) AS soil_moisture,
        cast(
            CASE
                WHEN light_intensity = 'NA' THEN NULL ELSE light_intensity
            END AS decimal(15, 2)
        ) AS light_intensity,
        cast(
            CASE
                WHEN battery_level = 'NA' THEN NULL ELSE battery_level
            END AS decimal(15, 2)
        ) AS battery_level

    FROM dfa23rawdata.rawdata.sensordataraw

)

SELECT * FROM stg_sensor
