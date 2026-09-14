terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.45.0"
    }
  }
}

# project_id/region/zone come from the variables declared in wif-setup.tf
# (same directory, same Terraform config) — nothing to hardcode here.
provider "google" {
  project                     = var.project_id
  region                      = var.region
  zone                        = var.zone
  impersonate_service_account = "wif-github-gcp-bootstrap@${var.project_id}.iam.gserviceaccount.com"
}
