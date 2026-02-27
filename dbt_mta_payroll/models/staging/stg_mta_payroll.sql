-- models/staging/stg_mta_payroll.sql

WITH source AS (
    SELECT * FROM {{ source('bronze_mta_payroll', 'raw_mta_payroll') }}
),

renamed_and_cast AS (
    SELECT
        -- 1. Integers
        SAFE_CAST(Year AS INT64) AS fiscal_year,

        -- 2. Cleaned Strings (TRIM to remove extra spaces, COALESCE to handle nulls)
        TRIM(CAST(name_hash AS STRING)) AS name_hash,
        UPPER(TRIM(COALESCE(CAST(Working_Agency AS STRING), 'UNKNOWN'))) AS agency_name,
        UPPER(TRIM(COALESCE(CAST(Title AS STRING), 'UNKNOWN'))) AS job_title,
        UPPER(TRIM(COALESCE(CAST(Department AS STRING), 'UNKNOWN'))) AS department_name,
        UPPER(TRIM(COALESCE(CAST(Pay_Basis AS STRING), 'UNKNOWN'))) AS pay_basis,

        -- 3. Dates (Conversion MM/DD/YYYY to DATE)
        SAFE.PARSE_DATE('%m/%d/%Y', Start_Date) AS start_date,
        SAFE.PARSE_DATE('%m/%d/%Y', Separation_Date) AS separation_date,
        SAFE.PARSE_DATE('%m/%d/%Y', Updated_At) AS record_updated_at,

        -- 4. Numerics / Floats (SAFE_CAST to handle non-numeric gracefully)
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