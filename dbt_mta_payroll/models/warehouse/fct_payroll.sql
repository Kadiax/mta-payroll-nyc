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
        
        -- foreign keys
        de.employee_key,
        da.agency_key,
        djt.job_title_key

    from staging stg
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
    current_timestamp() as dbt_updated_at

from joined