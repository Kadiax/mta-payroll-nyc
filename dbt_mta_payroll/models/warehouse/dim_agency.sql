with raw_agencies as (
    select distinct
        agency_name
    from {{ ref('stg_mta_payroll') }}
)

select
    farm_fingerprint(agency_name) as agency_key,
    agency_name,    
    -- Audit metadata
    current_timestamp() as dbt_updated_at
from raw_agencies
where agency_name is not null