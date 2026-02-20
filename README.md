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

## GDPR Compliance

To ensure data privacy, this pipeline implements PII Redaction. Employee names are never stored in the Cloud. A pseudonymization process using SHA-256 hashing is performed locally during the extraction phase to create a unique name_hash while maintaining data utility for analytical purposes.
