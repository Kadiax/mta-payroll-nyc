{{ config(
    materialized='table',
    cluster_by=["agency_name", "job_title"]
) }}

with employee_raw as (
    select distinct
        name_hash,
        agency_name,
        job_title,
        start_date,
        separation_date,
        department_name,
        pay_basis,
        case 
            when start_date is null then 'INCOMPLETE/RETROACTIVE'
            else 'ACTIVE/STANDARD'
        end as employment_status,
    from {{ ref('stg_mta_payroll') }}
)

select
    farm_fingerprint(
        concat(
            coalesce(name_hash), 
            agency_name, 
            job_title, 
            coalesce(cast(start_date as string), '0001-01-01')
        )
    ) as employee_key,
    
    name_hash,
    start_date,
    separation_date,
    agency_name,
    department_name,
    job_title,
    pay_basis,
    employment_status,
    
    -- Audit metadata
    current_timestamp() as dbt_updated_at
from employee_raw