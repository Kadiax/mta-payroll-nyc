{{ config(
    materialized='table',
    partition_by={
      "field": "fiscal_year",
      "data_type": "int64",
      "range": {"start": 2025, "end": 2030, "interval": 1}
    },
    cluster_by=["agency_name", "job_title"]
) }}

with fact as (
    select * from {{ ref('fct_payroll') }}
),

dim_employee as (
    select * from {{ ref('dim_employee') }}
),

dim_agency as (
    select * from {{ ref('dim_agency') }}
),

dim_job_title as (
    select * from {{ ref('dim_job_title') }}
)

select
    -- 1. Identifiers (We keep the keys just in case, but we prefer text)
    f.payroll_key,
    f.employee_key,
    f.agency_key,
    f.job_title_key,
    
    -- 2. Employee Attributes
    de.name_hash,
    de.start_date,
    de.separation_date,
    de.employment_status,
    de.pay_basis,
    de.department_name,

    -- 3. Agency & Job Attributes
    da.agency_name,
    djt.job_title as job_title,

    -- 4. Time
    f.fiscal_year,

    -- 5. Metrics (The facts)
    f.hourly_rate,
    f.regular_pay,
    f.overtime_pay,
    f.total_earnings,
    f.retro_pay,
    f.cash_outs,
    f.other_pay,

    -- 6. Metadata
    current_timestamp() as obt_updated_at

from fact f
left join dim_employee de on f.employee_key = de.employee_key
left join dim_agency da    on f.agency_key = da.agency_key
left join dim_job_title djt on f.job_title_key = djt.job_title_key