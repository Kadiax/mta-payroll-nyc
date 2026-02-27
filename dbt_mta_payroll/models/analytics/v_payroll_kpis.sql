select
    fiscal_year,
    agency_name,
    -- KPI 1: Headcount (Unique employee count)
    count(distinct employee_key) as headcount,
    
    -- KPI 2: Total Payroll 
    sum(total_earnings) as total_payroll,
    
    -- KPI 3: Avg Salary 
    avg(regular_pay) as avg_regular_salary,
    
    -- KPI 4: Overtime Pay
    sum(overtime_pay) as total_overtime,
    
    count(*) as total_records
from {{ ref('obt_payroll') }}
group by 1, 2