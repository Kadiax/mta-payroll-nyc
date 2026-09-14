# NOTE: Terraform does NOT allow variables/expressions inside a `backend`
# block (hard language limitation) — `bucket` must stay a literal string
# here, it can't be pulled from terraform.tfvars like the rest of this
# repo's values.
terraform {
  backend "gcs" {
    bucket = "mta-payroll-nyc-tfstate" # output of setup/'s WIF bootstrap (PR1) — never reuse another repo's bucket
    prefix = "terraform/state"
  }
}
