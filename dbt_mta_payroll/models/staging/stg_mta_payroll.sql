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

        -- Agency code normalization: the source uses inconsistent codes for the
        -- SAME agency across fiscal years (confirmed on real data — 2025 vs 2026
        -- had 9 vs 13 distinct agency_name values, several near-duplicates).
        -- Without this, dim_agency splits one real agency into two rows, and
        -- dim_employee's dedup key (which includes agency_name) fragments the
        -- same employee's history across fiscal years too.
        --   LIRR -> LIR: same agency (Long Island Rail Road), inconsistent spelling
        --   SIR -> SIRTOA: same legal entity — FTA/NTD lists it as "Staten Island
        --     Rapid Transit Operating Authority, dba: MTA Staten Island Railway
        --     (SIRTOA)"; SIR is just the dba/operating name
        -- NOT merged (insufficient evidence, deliberately left as-is):
        --   MTA BUS vs MTB BUS — looked like a typo, but MTA.info's own docs
        --     reference a distinct "MTB 5303" exam code, suggesting MTB may be
        --     a real internal designation, not an error. Needs a firmer source
        --     before merging.
        --   MTA-P vs MTA POLICE — both coexist within fiscal_year 2026 with
        --     comparable row counts (2,610 vs 2,656); if this were just
        --     inconsistent spelling, both forms coexisting in the same year at
        --     similar volume would be unusual. Possibly a real sub-distinction
        --     (e.g. active vs retired). No evidence found either way.
        CASE UPPER(TRIM(COALESCE(CAST(Working_Agency AS STRING), 'UNKNOWN')))
            WHEN 'LIRR' THEN 'LIR'
            WHEN 'SIR' THEN 'SIRTOA'
            ELSE UPPER(TRIM(COALESCE(CAST(Working_Agency AS STRING), 'UNKNOWN')))
        END AS agency_name,
        UPPER(TRIM(COALESCE(CAST(Title AS STRING), 'UNKNOWN'))) AS job_title,
        UPPER(TRIM(COALESCE(CAST(Department AS STRING), 'UNKNOWN'))) AS department_name,
        UPPER(TRIM(COALESCE(CAST(Pay_Basis AS STRING), 'UNKNOWN'))) AS pay_basis,

        -- 3. Dates (Conversion MM/DD/YYYY to DATE)
        SAFE.PARSE_DATE('%m/%d/%Y', Start_Date) AS start_date,
        SAFE.PARSE_DATE('%m/%d/%Y', Separation_Date) AS separation_date,
        SAFE.PARSE_DATE('%m/%d/%Y', Updated_At) AS record_updated_at,

        -- 4. Numerics / Floats (SAFE_CAST to handle non-numeric gracefully)
        COALESCE(SAFE_CAST(Hourly_Rate AS FLOAT64), 0) AS hourly_rate,
        COALESCE(SAFE_CAST(Regular_Pay AS FLOAT64), 0) AS regular_pay,
        COALESCE(SAFE_CAST(Overtime_Pay AS FLOAT64), 0) AS overtime_pay,
        COALESCE(SAFE_CAST(Cash_Outs AS FLOAT64), 0) AS cash_outs,
        COALESCE(SAFE_CAST(Retro_Pay AS FLOAT64), 0) AS retro_pay,
        COALESCE(SAFE_CAST(Other_Pay AS FLOAT64), 0) AS other_pay,
        COALESCE(SAFE_CAST(Total_Earnings AS FLOAT64), 0) AS total_earnings,

        -- 5. Metadata & Lineage from Source
        TRIM(CAST(source_file AS STRING)) AS source_file,
        CAST(raw_ingested_at AS TIMESTAMP) AS raw_ingested_at,
        
        -- dbt Metadata
        CURRENT_TIMESTAMP() AS stg_processed_at

    FROM source
)

SELECT * FROM renamed_and_cast