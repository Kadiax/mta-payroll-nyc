with source_metrics as (
    select 
        sum(total_earnings) as src_total_payroll,
        sum(overtime_pay) as src_total_overtime,
        avg(regular_pay) as src_avg_salary,
        count(distinct employee_key) as src_headcount
    from {{ ref('obt_payroll') }}
),

view_metrics as (
    select 
        sum(total_payroll) as view_total_payroll,
        sum(total_overtime) as view_total_overtime,
        avg(avg_regular_salary) as view_avg_salary, 
        sum(headcount) as view_headcount
    from {{ ref('v_payroll_kpis') }}
)

select *
from source_metrics, view_metrics
where 
    abs(src_total_payroll - view_total_payroll) > 0.01
    or abs(src_total_overtime - view_total_overtime) > 0.01
    or src_headcount != view_headcount