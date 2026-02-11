# MTA PAYROLL NYC

## Google Cloud SDK Shell OAUTH

- gcloud config set project mta-payroll-nyc
- gcloud config list
- gcloud auth application-default login

## Power shell

- Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
- .\venv\Scripts\activate

## Packages installation

- pip install -r requirements.txt
- cd dbt_mta_payroll_nyc
- dbt deps
