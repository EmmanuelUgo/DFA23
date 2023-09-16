WITH 
    soil_data AS (
        SELECT * FROM {{ source('DFA23', 'SOILDATARAW') }}
    ),

    stg_soil AS (

        SELECT
        --- timestamp
            try_to_timestamp(timestamp, 'mm/dd/yyyy hh24:mi') AS timestamp,

        --- decimal
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
        from soil_data
        where timestamp is not null 
        order by timestamp asc
    ),

    stg_soil_plus_derived as (

        -- Soil type from soil ph
        -- 6.5 to 7.5—neutral
        -- over 7.5—alkaline
        -- less than 6.5—acidic, and soils with pH less than 5.5 are considered strongly acidic.

        SELECT *,
            CASE 
                WHEN soil_ph < 6.5 then 'Acidic'
                WHEN soil_ph between 6.5 and 7.5 then 'Neutral'
                WHEN soil_ph > 7.5 then 'Alkaline'
                END AS soil_type
        FROM stg_soil

    )   

SELECT * FROM stg_soil_plus_derived
