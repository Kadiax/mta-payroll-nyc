{% docs col_fiscal_year %}
Fiscal year of the payroll record.
{% enddocs %}

{% docs col_name_hash %}
Salted SHA-256 hash of the employee name (Anonymized).
{% enddocs %}

{% docs col_agency_name %}
MTA Agency name (e.g., NYCTA, MMNR).
{% enddocs %}

{% docs col_job_title %}
Official job title description.
{% enddocs %}

{% docs col_start_date %}
Employee agency start date (MM/DD/YYYY).
{% enddocs %}

{% docs col_separation_date %}
Date the employee left the agency, if applicable.
{% enddocs %}

{% docs col_department_name %}
Internal department code or name.
{% enddocs %}

{% docs col_pay_basis %}
Frequency of pay (e.g., Biweekly, Weekly).
{% enddocs %}

{% docs col_hourly_rate %}
Hourly pay rate if applicable.
{% enddocs %}

{% docs col_regular_pay %}
Gross pay for regular working hours.
{% enddocs %}

{% docs col_overtime_pay %}
Amount paid for overtime hours.
{% enddocs %}

{% docs col_cash_outs %}
Payouts for unused leave or other benefits.
{% enddocs %}

{% docs col_retro_pay %}
Back-pay for previous periods.
{% enddocs %}

{% docs col_other_pay %}
Miscellaneous additional compensation.
{% enddocs %}

{% docs col_total_earnings %}
Sum of all pay components for the period.
{% enddocs %}

{% docs col_updated_at %}
Timestamp when the record was last updated by source.
{% enddocs %}

{% docs col_source_file %}
Name of the source file from which the record was ingested.
Stored in a Cloud Storage bucket.
{% enddocs %}

{% docs col_raw_ingested_at %}
Timestamp indicating when the record was loaded into BigQuery.
{% enddocs %}

{% docs col_stg_processed_at %}
Technical timestamp of the last dbt staging execution.
{% enddocs %}

{% docs col_dbt_updated_at %}
Technical timestamp of the last dbt model execution.
{% enddocs %}

{% docs fct_payroll %}
Central fact table containing financial metrics of MTA payroll, with foreign keys to dimensions and business logic
{% enddocs %}

{% docs col_payroll_key %}
Surrogate key for the fact table.
Hash of employee_key + agency_key + job_title_key + fiscal_year.
{% enddocs %}

{% docs dim_employee %}
Unique dimension per employee, based on the combination of Name + Agency + Job Title + Start Date.
{% enddocs %}

{% docs col_employee_key %}
Surrogate key for the dim_employee.
Hash of name_hash + agency_name + job_title + start_date.
{% enddocs %}

{% docs dim_job_title %}
Referential table for job titles, with a technical key and metadata for tracking updates.
{% enddocs %}

{% docs col_job_title_key %}
Surrogate key for the dim_employee.
Hash of job_title.
{% enddocs %}

{% docs dim_agency %}
Referential table for MTA Agencies.
{% enddocs %}

{% docs col_agency_key %}
Surrogate key for the dim_agency.
Hash of agency_name.
{% enddocs %}

{% docs dim_calendar %}
Calendar dimension for date-based analysis, created from GENERATE_DATE_ARRAY.
{% enddocs %}

{% docs col_calenday_key %}
Unique identifier for each date (format YYYY-MM-DD)
{% enddocs %}

{% docs col_day_is_weekday %}
Indicator if the day is a weekday (1) or weekend (0).
{% enddocs %}

{% docs obt_payroll %}
Final denormalized table for Looker Studio.
{% enddocs %}
