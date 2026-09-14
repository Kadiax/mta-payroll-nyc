terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.45.0"
    }
  }
}

# No `provider "google" {}` block here — credentials come from WIF in CI,
# never hardcoded or impersonated locally. Without even this much,
# `terraform init` silently grabs the latest provider version instead of
# the project's pinned 7.45.0.
