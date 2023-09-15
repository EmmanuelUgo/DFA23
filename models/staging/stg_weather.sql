WITH stg_weather AS (

    SELECT
        try_to_timestamp(timestamp, 'mm/dd/yyyy hh24:mi') AS timestamp,
        cast(
            CASE WHEN weather_condition = 'NA' THEN NULL ELSE weather_condition END AS string
        ) AS weather_condition,
        cast(
            CASE
                WHEN wind_speed = 'NA' THEN NULL ELSE wind_speed
            END AS decimal(
                15, 2
            )
        ) AS wind_speed,
        cast(
            CASE
                WHEN precipitation = 'NA' THEN NULL ELSE precipitation
            END AS decimal(
                15, 2
            )
        ) AS precipitation
    FROM dfa23rawdata.rawdata.weatherdataraw
)

SELECT * FROM stg_weather
