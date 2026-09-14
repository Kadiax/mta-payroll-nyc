{{ config(
    materialized='table',
    partition_by={
      "field": "fiscal_year",
      "data_type": "int64",
      "range": {"start": 2025, "end": 2030, "interval": 1}
    },
    cluster_by=["agency_key", "job_title_key"]
) }}

with staging as (
    select * from {{ ref('stg_mta_payroll') }}
),

-- The source publishes cumulative payroll snapshots multiple times within
-- an open fiscal year (e.g. a quarterly cutoff) — the same employee/
-- agency/job_title/start_date/fiscal_year combination can appear more than
-- once with different record_updated_at and different (larger, cumulative)
-- pay amounts. fct_payroll's grain is one row per employee per agency per
-- job per fiscal year (no snapshot dimension in its surrogate key), so we
-- keep only the latest snapshot per fiscal year — same pattern dim_employee
-- already uses to pick its own "golden record".
deduped_staging as (
    select * except (row_num)
    from (
        select
            *,
            row_number() over (
                partition by name_hash, agency_name, job_title, start_date, fiscal_year
                order by record_updated_at desc, raw_ingested_at desc
            ) as row_num
        from staging
    )
    where row_num = 1
),

-- Get foreign keys from dimensions
joined as (
    select
        stg.fiscal_year,
        stg.hourly_rate,
        stg.regular_pay,
        stg.overtime_pay,
        stg.cash_outs,
        stg.retro_pay,
        stg.other_pay,
        stg.total_earnings,
        stg.record_updated_at,
        stg.raw_ingested_at,

        -- foreign keys
        de.employee_key,
        da.agency_key,
        djt.job_title_key

    from deduped_staging stg
    -- Employee Join (Composite logic to match dim_employee)
    left join {{ ref('dim_employee') }} de 
        on de.name_hash = stg.name_hash 
        and de.agency_name = stg.agency_name 
        and de.job_title = stg.job_title
        and coalesce(de.start_date, '0001-01-01') = coalesce(stg.start_date, '0001-01-01')

    -- Agency Join
    left join {{ ref('dim_agency') }} da 
        on da.agency_name = stg.agency_name

    -- Job Title Join
    left join {{ ref('dim_job_title') }} djt 
        on djt.job_title = stg.job_title
)

select
    -- Generation of the surrogate key for the fact table
    farm_fingerprint(
        concat(
            cast(employee_key as string),
            cast(agency_key as string),
            cast(job_title_key as string),
            cast(fiscal_year as string)
        )
    ) as payroll_key,
    
    employee_key,
    agency_key,
    job_title_key,
    fiscal_year,
    
    -- Mesures
    hourly_rate,
    regular_pay,
    overtime_pay,
    cash_outs,
    retro_pay,
    other_pay,
    total_earnings,
    
    -- Dates & Metadata
    record_updated_at,
    raw_ingested_at,
    current_timestamp() as dbt_updated_at

from joined