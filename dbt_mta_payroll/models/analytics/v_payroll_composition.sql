with payroll_unpivoted as (
    -- Branch 1: Base salary
    select
        fiscal_year,
        agency_name,
        'Base Salary' as pay_type,
        regular_pay as amount
    from {{ ref('obt_payroll') }}
    
    union all

    -- Branch 2 : Overtime
    select
        fiscal_year,
        agency_name,
        'Overtime' as pay_type,
        overtime_pay as amount
    from {{ ref('obt_payroll') }}

    union all

    -- Branch 3 : Retroactive pay
    select
        fiscal_year,
        agency_name,
        'Retroactive' as pay_type,
        retro_pay as amount
    from {{ ref('obt_payroll') }}

    union all

    -- Branch 4: Cash outs (paid leave bought back, etc.)
    select
        fiscal_year,
        agency_name,
        'Cash Outs' as pay_type,
        cash_outs as amount
    from {{ ref('obt_payroll') }}

    union all

    -- Branch 5: Other pay (other bonuses, etc.)
    select
        fiscal_year,
        agency_name,
        'Other Pay' as pay_type,
        other_pay as amount
    from {{ ref('obt_payroll') }}
)

select
    fiscal_year,
    agency_name,
    pay_type,
    sum(amount) as total_amount
from payroll_unpivoted
group by 1, 2, 3