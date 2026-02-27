{{ config(
    materialized='materialized_view',
    on_configuration_change='apply',
    partition_by={
      "field": "fiscal_year",
      "data_type": "int64",
      "range": {"start": 2025, "end": 2030, "interval": 1}
    },
    cluster_by=["job_title"]
) }}

select
    fiscal_year,
    agency_name,
    job_title,
    -- Bar chart metrics
    sum(overtime_pay) as total_overtime_pay,
    sum(total_earnings) as total_earnings_combined,
    -- How many employees in each job had overtime pay?
    count(*) as employee_count
from {{ ref('obt_payroll') }}
group by 1, 2, 3