{{ config(
    materialized='incremental',
    unique_key='employee_key',
    incremental_strategy='merge',
    cluster_by=["agency_name", "job_title"]
) }}

with deduped_raw as (
    select *
    from (
        select 
            *,
            row_number() over (
                partition by name_hash, agency_name, job_title, start_date 
                order by coalesce(separation_date, '9999-12-31') desc, raw_ingested_at desc
            ) as row_num
        from {{ ref('stg_mta_payroll') }}
        
        {% if is_incremental() %}
        -- We ONLY read the lines that have arrived since the last run.
        where raw_ingested_at > (select max(dbt_updated_at) from {{ this }})
        {% endif %}
        
    )
    where row_num = 1
)

select
    farm_fingerprint(
        concat(
            name_hash, 
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
    current_timestamp() as dbt_updated_at
from deduped_raw