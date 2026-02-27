with raw_jobs as (
    select distinct
        job_title
    from {{ ref('stg_mta_payroll') }}
    where job_title is not null
)

select
    farm_fingerprint(job_title) as job_title_key,    
    job_title,
    -- Audit metadata
    current_timestamp() as dbt_updated_at
from raw_jobs