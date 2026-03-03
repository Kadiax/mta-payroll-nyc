select
    fiscal_year,
    agency_name,
    -- KPI 1: Headcount (Unique employee count)
    count(distinct employee_key) as headcount,
    
    -- KPI 2: Total Payroll 
    sum(total_earnings) as total_payroll,
    
    -- KPI 3: Overtime Pay
    sum(overtime_pay) as total_overtime,

    -- KPI 4: Regular Pay
    sum(regular_pay) as total_regular_pay,
    
    count(*) as total_records
from {{ ref('obt_payroll') }}
group by 1, 2