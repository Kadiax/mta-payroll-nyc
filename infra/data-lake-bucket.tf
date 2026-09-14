##############################################
# data-lake-bucket.tf — the app's GCS bucket (bronze CSVs + every script's
# GCS logs). Pre-existed this Terraform setup (created manually before
# CI/CD was added) — imported into state rather than let Terraform try to
# create a bucket name that's already taken:
#
#   terraform import google_storage_bucket.data_lake mta-payroll-nyc
#
# The resource block below matches the bucket's real settings exactly
# (confirmed via `gcloud storage buckets describe` before writing this),
# so `terraform plan` shows zero drift after import.
##############################################

resource "google_storage_bucket" "data_lake" {
  project                     = var.project_id
  name                        = "mta-payroll-nyc"
  location                    = "EUROPE-WEST9"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
}

##############################################
# Bucket-scoped IAM for the 4 runtime SAs (created in setup/wif-setup.tf,
# PR1 — different Terraform state, referenced here by email string, no
# direct resource reference possible). Deploy SA can apply these via its
# project-level storage.admin role (granted in setup/, PR1).
#
# Roles match each script's actual GCS usage, not a shared blanket role:
# - create-datasets / transform: write-only, they only ever write their
#   own GCS logs (utils/gcs_logger.py), never touch bronze CSVs.
# - extract: uploads bronze CSVs + writes logs.
# - load: lists/reads bronze CSVs (idempotency check against BigQuery) +
#   writes logs.
##############################################

locals {
  runtime_create_datasets_email = "mta-rt-create-ds@${var.project_id}.iam.gserviceaccount.com"
  runtime_extract_email         = "mta-rt-extract@${var.project_id}.iam.gserviceaccount.com"
  runtime_load_email            = "mta-rt-load@${var.project_id}.iam.gserviceaccount.com"
  runtime_transform_email       = "mta-rt-transform@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_storage_bucket_iam_member" "create_datasets_can_write_logs" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${local.runtime_create_datasets_email}"
}

resource "google_storage_bucket_iam_member" "extract_can_manage_objects" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${local.runtime_extract_email}"
}

resource "google_storage_bucket_iam_member" "load_can_manage_objects" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${local.runtime_load_email}"
}

resource "google_storage_bucket_iam_member" "transform_can_write_logs" {
  bucket = google_storage_bucket.data_lake.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${local.runtime_transform_email}"
}
