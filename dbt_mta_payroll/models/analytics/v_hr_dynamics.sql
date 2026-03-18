{{ config(
    materialized='view'
) }}

with hr_events_unpivoted as (
    -- Hires
    select
        fiscal_year,
        agency_name,
        job_title,
        employee_key,
        'Hire' as event_type
    from {{ ref('obt_payroll') }}
    where start_date is not null 
        -- We ensure that the start date took place during the relevant fiscal year.
        and extract(year from start_date) = fiscal_year 

    union all

    -- Separations
    select
        fiscal_year,
        agency_name,
        job_title,
        employee_key,
        'Separation' as event_type
    from {{ ref('obt_payroll') }}
    where separation_date is not null 
      -- We ensure that the separation took place during the relevant fiscal year.
      and extract(year from separation_date) = fiscal_year
)

select
    fiscal_year,
    agency_name,
    job_title,
    event_type,
    count(distinct employee_key) as volume_events
from hr_events_unpivoted
group by 1, 2, 3, 4