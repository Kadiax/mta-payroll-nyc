with source_metrics as (
    select 
        sum(total_earnings) as src_total_payroll,
        sum(overtime_pay) as src_total_overtime
    from {{ ref('obt_payroll') }}
),

view_metrics as (
    select 
        sum(total_payroll) as view_total_payroll,
        sum(total_overtime) as view_total_overtime
    from {{ ref('v_payroll_kpis') }}
)

select *
from source_metrics, view_metrics
where 
    abs(src_total_payroll - view_total_payroll) > 0.01
    or abs(src_total_overtime - view_total_overtime) > 0.01