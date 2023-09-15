WITH stg_soil AS (

    SELECT
        try_to_timestamp(timestamp, 'mm/dd/yyyy hh24:mi') AS timestamp,
        cast(
            CASE WHEN soil_comp = 'NA' THEN NULL ELSE soil_comp END AS decimal(
                15, 2
            )
        ) AS soil_comp,
        cast(
            CASE
                WHEN soil_moisture = 'NA' THEN NULL ELSE soil_moisture
            END AS decimal(
                15, 2
            )
        ) AS soil_moisture,
        cast(
            CASE WHEN soil_ph = 'NA' THEN NULL ELSE soil_ph END AS decimal(
                15, 2
            )
        ) AS soil_ph,
        cast(
            CASE
                WHEN nitrogen_level = 'NA' THEN NULL ELSE nitrogen_level
            END AS decimal(
                15, 2
            )
        ) AS nitrogen_level,
        cast(
            CASE
                WHEN phosphorus_level = 'NA' THEN NULL ELSE phosphorus_level
            END AS decimal(
                15, 2
            )
        ) AS phosphorus_level,
        cast(
            CASE
                WHEN organic_matter = 'NA' THEN NULL ELSE organic_matter
            END AS decimal(
                15, 2
            )
        ) AS organic_matter
    FROM dfa23rawdata.rawdata.soildataraw
)

SELECT * FROM stg_soil
