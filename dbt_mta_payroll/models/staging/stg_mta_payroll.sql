-- models/staging/stg_mta_payroll.sql

WITH source AS (
    SELECT * FROM {{ source('bronze_mta_payroll', 'raw_mta_payroll') }}
),

renamed_and_cast AS (
    SELECT
        -- 1. Integers
        SAFE_CAST(Year AS INT64) AS fiscal_year,

        -- 2. Strings & Keys (Nettoyées avec TRIM)
        TRIM(CAST(name_hash AS STRING)) AS name_hash,
        TRIM(COALESCE(CAST(Working_Agency AS STRING), 'Unknown')) AS agency_name,
        TRIM(COALESCE(CAST(Title AS STRING), 'Unknown')) AS job_title,
        TRIM(COALESCE(CAST(Department AS STRING), 'Unknown')) AS department_name,
        TRIM(COALESCE(CAST(Pay_Basis AS STRING), 'Unknown')) AS pay_basis,

        -- 3. Dates (Conversion MM/DD/YYYY to DATE)
        SAFE.PARSE_DATE('%m/%d/%Y', Start_Date) AS start_date,
        SAFE.PARSE_DATE('%m/%d/%Y', Separation_Date) AS separation_date,
        SAFE.PARSE_DATE('%m/%d/%Y', Updated_At) AS record_updated_at,

        -- 4. Numeric / Floats (Utilisation de SAFE_CAST pour la robustesse)
        SAFE_CAST(Hourly_Rate AS FLOAT64) AS hourly_rate,
        SAFE_CAST(Regular_Pay AS FLOAT64) AS regular_pay,
        SAFE_CAST(Overtime_Pay AS FLOAT64) AS overtime_pay,
        SAFE_CAST(Cash_Outs AS FLOAT64) AS cash_outs,
        SAFE_CAST(Retro_Pay AS FLOAT64) AS retro_pay,
        SAFE_CAST(Other_Pay AS FLOAT64) AS other_pay,
        SAFE_CAST(Total_Earnings AS FLOAT64) AS total_earnings,

        -- 5. Metadata & Lineage from Source
        TRIM(CAST(source_file AS STRING)) AS source_file,
        CAST(raw_ingested_at AS TIMESTAMP) AS raw_ingested_at,
        
        -- dbt Metadata
        CURRENT_TIMESTAMP() AS stg_processed_at

    FROM source
)

SELECT * FROM renamed_and_cast