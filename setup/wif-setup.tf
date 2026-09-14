##############################################
# variables.tf
##############################################

variable "project_id" {
  description = "GCP project ID — no default, must come from terraform.tfvars"
  type        = string
}

variable "region" {
  description = "Default GCP region — also used for provider.tf and the state bucket location"
  type        = string
  default     = "europe-west9"
}

variable "zone" {
  description = "Default GCP zone, used by provider.tf"
  type        = string
  default     = "europe-west9-a"
}

# project_number (GCP-generated numeric ID, different from the human-readable
# project_id) is required by the IAM API to build the pool/provider path.
# Fetched automatically instead of hardcoded, to avoid having to look it up by hand.
data "google_project" "current" {
  project_id = var.project_id
}

variable "github_repo" {
  description = "GitHub repo allowed to use this WIF pool, format 'owner/repo'"
  type        = string
  default     = "Kadiax/mta-payroll-nyc"
}

variable "pool_id" {
  description = "Workload Identity Pool ID — must be unique across the project"
  type        = string
  default     = "mta-wif-pool"
}

variable "provider_id" {
  description = "Workload Identity Provider ID"
  type        = string
  default     = "github"
}

variable "service_account_id" {
  description = "ID (before @) of the Service Account dedicated to the GitHub Actions pipeline (deploy identity, not runtime)"
  type        = string
  default     = "mta-wif-deploy"
}

variable "tfstate_bucket_name" {
  description = "Name of the GCS bucket that will store this repo's Terraform state — MUST be unique, never reuse a bucket from another repo"
  type        = string
  default     = "mta-payroll-nyc-tfstate"
}

variable "tfstate_force_destroy" {
  description = "Default protection against accidental state deletion. Only set to true via -var at the moment of an intentional destroy (never hardcoded). NOTE: 'terraform destroy -var=...' alone is unreliable for this flag on google_storage_bucket — apply the var change first (terraform apply -var=...), then destroy (terraform destroy -var=...), so the value is genuinely written to state before deletion."
  type        = bool
  default     = false
}


##############################################
# bootstrap.tf — Terraform state bucket
#
# This bucket is referenced by the application repo's backend.tf (infra/),
# so it MUST exist before that repo runs `terraform init`. Same
# chicken-and-egg problem as WIF itself: this file runs with LOCAL state
# (no "gcs" backend here), applied once manually before anything else.
##############################################

resource "google_storage_bucket" "tfstate" {
  project                    = var.project_id
  name                       = var.tfstate_bucket_name
  location                   = var.region
  force_destroy              = var.tfstate_force_destroy # false by default = protected; explicitly flipped to true at destroy time
  public_access_prevention   = "enforced"
  uniform_bucket_level_access = true

  versioning {
    enabled = true # allows restoring a previous state version in case of corruption/mistake
  }
}


##############################################
# wif.tf — Workload Identity Pool + Provider
##############################################

resource "google_iam_workload_identity_pool" "github_pool" {
  project                   = var.project_id
  workload_identity_pool_id = var.pool_id
  display_name              = "GitHub Actions Pool"
  description               = "Federated pool for keyless GitHub Actions -> GCP authentication"
  disabled                  = false
}

resource "google_iam_workload_identity_pool_provider" "github_provider" {
  project                            = var.project_id
  workload_identity_pool_id          = google_iam_workload_identity_pool.github_pool.workload_identity_pool_id
  workload_identity_pool_provider_id = var.provider_id
  display_name                       = "GitHub"

  # Official GitHub Actions OIDC issuer
  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }

  # Maps GitHub's OIDC token claims (assertion.*) to Google attributes
  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.aud"        = "assertion.aud"
    "attribute.repository" = "assertion.repository"
  }

  # Key security point: restricts token exchange to this one authorized repo.
  # Without this condition, ANY GitHub repo could authenticate through this pool.
  attribute_condition = "attribute.repository=='${var.github_repo}'"
}


##############################################
# service-account.tf — Deploy Service Account + project roles
#
# Business roles this pipeline actually needs: run.admin (deploy the 4
# Cloud Run Jobs + set resource-scoped IAM on them for the workflow SA in
# infra/), artifactregistry.admin (create/manage the AR repo),
# storage.admin (create/manage the data-lake bucket + its bucket-scoped
# IAM bindings for the 4 runtime SAs in infra/).
##############################################

resource "google_service_account" "deploy_sa" {
  project      = var.project_id
  account_id   = var.service_account_id
  display_name = "GitHub Actions WIF deploy Service Account"
  description  = "Impersonated by GitHub Actions via Workload Identity Federation — deploy identity, not runtime"
}

resource "google_project_iam_member" "deploy_sa_run_admin" {
  project = var.project_id
  role    = "roles/run.admin"
  member  = "serviceAccount:${google_service_account.deploy_sa.email}"
}

resource "google_project_iam_member" "deploy_sa_artifact_registry_admin" {
  project = var.project_id
  role    = "roles/artifactregistry.admin"
  member  = "serviceAccount:${google_service_account.deploy_sa.email}"
}

resource "google_project_iam_member" "deploy_sa_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.deploy_sa.email}"
}

# Access to the state bucket — REQUIRED regardless of what the pipeline
# deploys, otherwise `terraform init` fails in CI with a 403 on
# storage.objects.list. Frequently forgotten.
resource "google_storage_bucket_iam_member" "deploy_sa_tfstate_access" {
  bucket = google_storage_bucket.tfstate.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.deploy_sa.email}"
}


##############################################
# runtime-service-accounts.tf — 4 dedicated runtime identities, one per
# Cloud Run Job, NOT the singular runtime SA from the base template
# (adapted here: this pipeline runs 4 distinct scripts with different
# GCP needs, so each gets its own minimal identity instead of one shared
# runtime SA with the union of all permissions).
#
# account_id below is hardcoded, not a variable — same reasoning as the
# workflow/scheduler SAs further down: a fixed, known set of 4 identities
# for this specific pipeline, not something that varies per environment.
#
# BigQuery project roles granted here (not in infra/) because
# google_project_iam_member needs resourcemanager.projectIamAdmin, which
# only the bootstrap SA has — see gcp-wif-bootstrap convention #10.
# Bucket-scoped storage roles for these same SAs are granted in infra/
# instead (resource-scoped, deploy SA's storage.admin is enough there).
##############################################

resource "google_service_account" "runtime_create_datasets" {
  project      = var.project_id
  account_id   = "mta-rt-create-ds"
  display_name = "Runtime identity — mta-create-datasets Cloud Run Job"
  description  = "Creates the bronze/silver/gold/analytics BigQuery datasets if missing (idempotent)"
}

resource "google_service_account" "runtime_extract" {
  project      = var.project_id
  account_id   = "mta-rt-extract"
  display_name = "Runtime identity — mta-extract Cloud Run Job"
  description  = "Downloads the source CSV, hashes PII, uploads to the GCS data-lake bucket"
}

resource "google_service_account" "runtime_load" {
  project      = var.project_id
  account_id   = "mta-rt-load"
  display_name = "Runtime identity — mta-load Cloud Run Job"
  description  = "Loads new GCS CSVs into the BigQuery bronze raw table"
}

resource "google_service_account" "runtime_transform" {
  project      = var.project_id
  account_id   = "mta-rt-transform"
  display_name = "Runtime identity — mta-transform Cloud Run Job"
  description  = "Runs `dbt build --full-refresh` (silver/gold/analytics)"
}

# Deploy SA must be allowed to "act as" each runtime SA at deploy time
# (`--service-account=` on `gcloud run jobs deploy`) — scoped to each SA
# individually, never a project-level serviceAccountUser.
resource "google_service_account_iam_member" "deploy_sa_can_act_as_create_datasets" {
  service_account_id = google_service_account.runtime_create_datasets.name
  role                = "roles/iam.serviceAccountUser"
  member              = "serviceAccount:${google_service_account.deploy_sa.email}"
}

resource "google_service_account_iam_member" "deploy_sa_can_act_as_extract" {
  service_account_id = google_service_account.runtime_extract.name
  role                = "roles/iam.serviceAccountUser"
  member              = "serviceAccount:${google_service_account.deploy_sa.email}"
}

resource "google_service_account_iam_member" "deploy_sa_can_act_as_load" {
  service_account_id = google_service_account.runtime_load.name
  role                = "roles/iam.serviceAccountUser"
  member              = "serviceAccount:${google_service_account.deploy_sa.email}"
}

resource "google_service_account_iam_member" "deploy_sa_can_act_as_transform" {
  service_account_id = google_service_account.runtime_transform.name
  role                = "roles/iam.serviceAccountUser"
  member              = "serviceAccount:${google_service_account.deploy_sa.email}"
}

# BigQuery project roles — only where the script actually touches BigQuery.
# mta-rt-extract gets none here (GCS-only, bucket-scoped rights in infra/).
resource "google_project_iam_member" "runtime_create_datasets_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor" # bigquery.datasets.create
  member  = "serviceAccount:${google_service_account.runtime_create_datasets.email}"
}

resource "google_project_iam_member" "runtime_load_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor" # create/insert into the raw table
  member  = "serviceAccount:${google_service_account.runtime_load.email}"
}

resource "google_project_iam_member" "runtime_load_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser" # run load/query jobs
  member  = "serviceAccount:${google_service_account.runtime_load.email}"
}

resource "google_project_iam_member" "runtime_transform_bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor" # dbt creates/replaces tables & views
  member  = "serviceAccount:${google_service_account.runtime_transform.email}"
}

resource "google_project_iam_member" "runtime_transform_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser" # dbt runs query/build jobs
  member  = "serviceAccount:${google_service_account.runtime_transform.email}"
}


##############################################
# workflow-scheduler-service-accounts.tf — Cloud Workflows + Cloud
# Scheduler orchestration (gcp-cloud-run-job-pipeline skill's optional
# extension, wired up starting PR4, but created here in PR1 since we
# already know this pipeline needs it and creating a Service Account
# needs iam.serviceAccountAdmin, which only the bootstrap SA has).
#
# account_id below is hardcoded, not a variable — matches how this is
# done for the 4 runtime SAs above (a fixed, known pair of identities
# for this pipeline, not something that varies per environment).
##############################################

resource "google_service_account" "workflow_sa" {
  project      = var.project_id
  account_id   = "mta-workflow"
  display_name = "Cloud Workflows execution identity"
  description  = "Runs the Workflow that chains the pipeline's 4 Cloud Run Jobs sequentially"
}

resource "google_service_account" "scheduler_sa" {
  project      = var.project_id
  account_id   = "mta-scheduler"
  display_name = "Cloud Scheduler invocation identity"
  description  = "Invokes the Workflow on a monthly cron schedule"
}

# Deploy SA must be allowed to "act as" the workflow SA to set
# `service_account = workflow_sa` on the google_workflows_workflow
# resource in infra/ (PR4) — assigning a Service Account to any resource
# requires iam.serviceAccounts.actAs on that SA.
resource "google_service_account_iam_member" "deploy_sa_can_act_as_workflow" {
  service_account_id = google_service_account.workflow_sa.name
  role                = "roles/iam.serviceAccountUser"
  member              = "serviceAccount:${google_service_account.deploy_sa.email}"
}

# Deploy SA must be allowed to "act as" the scheduler SA to configure its
# OAuth token on the Cloud Scheduler job (infra/, PR4) — only the
# bootstrap SA (via serviceAccountAdmin) can grant this.
resource "google_service_account_iam_member" "deploy_sa_can_act_as_scheduler" {
  service_account_id = google_service_account.scheduler_sa.name
  role                = "roles/iam.serviceAccountUser"
  member              = "serviceAccount:${google_service_account.deploy_sa.email}"
}

# workflow_sa needs run.viewer to poll the long-running operations its
# Cloud Run Job invocations spawn (run.operations.get) — not a
# sub-resource of any specific Job, so can't be scoped narrower than
# project-level. roles/run.invoker and even plain roles/run.developer
# alone both proved insufficient here — see gcp-cloud-run-job-pipeline
# skill's Gotchas for the full story.
resource "google_project_iam_member" "workflow_can_view_run_operations" {
  project = var.project_id
  role    = "roles/run.viewer"
  member  = "serviceAccount:${google_service_account.workflow_sa.email}"
}

# scheduler_sa needs workflows.invoker to call the Workflow — provider
# 7.45.0 has no resource-scoped IAM resource type for Cloud Workflows, so
# this is project-level regardless (acceptable with a single Workflow in
# the project).
resource "google_project_iam_member" "scheduler_can_invoke_workflow" {
  project = var.project_id
  role    = "roles/workflows.invoker"
  member  = "serviceAccount:${google_service_account.scheduler_sa.email}"
}


##############################################
# iam-binding.tf — WIF Pool <-> deploy Service Account link
##############################################

# Terraform equivalent of:
# gcloud iam service-accounts add-iam-policy-binding "<SA>" \
#   --role="roles/iam.workloadIdentityUser" \
#   --member="principalSet://iam.googleapis.com/projects/<NUM>/locations/global/workloadIdentityPools/<POOL>/attribute.repository/<OWNER>/<REPO>"
#
# Grants the principalSet (every identity in the pool matching the provider's
# CEL condition) the right to impersonate this specific Service Account.
resource "google_service_account_iam_member" "wif_impersonation" {
  service_account_id = google_service_account.deploy_sa.name
  role                = "roles/iam.workloadIdentityUser"
  member              = "principalSet://iam.googleapis.com/projects/${data.google_project.current.number}/locations/global/workloadIdentityPools/${google_iam_workload_identity_pool.github_pool.workload_identity_pool_id}/attribute.repository/${var.github_repo}"
}


##############################################
# outputs.tf — values to paste into the GitHub Actions workflow / repo vars
##############################################

output "workload_identity_provider" {
  description = "Value for 'workload_identity_provider:' in the workflow YAML"
  value       = "projects/${data.google_project.current.number}/locations/global/workloadIdentityPools/${google_iam_workload_identity_pool.github_pool.workload_identity_pool_id}/providers/${google_iam_workload_identity_pool_provider.github_provider.workload_identity_pool_provider_id}"
}

output "service_account_email" {
  description = "Value for 'service_account:' in the workflow YAML (deploy SA)"
  value       = google_service_account.deploy_sa.email
}

output "runtime_create_datasets_email" {
  description = "Runtime SA for the mta-create-datasets Cloud Run Job"
  value       = google_service_account.runtime_create_datasets.email
}

output "runtime_extract_email" {
  description = "Runtime SA for the mta-extract Cloud Run Job"
  value       = google_service_account.runtime_extract.email
}

output "runtime_load_email" {
  description = "Runtime SA for the mta-load Cloud Run Job"
  value       = google_service_account.runtime_load.email
}

output "runtime_transform_email" {
  description = "Runtime SA for the mta-transform Cloud Run Job"
  value       = google_service_account.runtime_transform.email
}

output "workflow_service_account_email" {
  description = "Identity the Workflow runs as — referenced by email in infra/, not by resource (different Terraform state)"
  value       = google_service_account.workflow_sa.email
}

output "scheduler_service_account_email" {
  description = "Identity Cloud Scheduler uses to invoke the Workflow — referenced by email in infra/, not by resource (different Terraform state)"
  value       = google_service_account.scheduler_sa.email
}

output "tfstate_bucket_name" {
  description = "Value for 'bucket =' in infra/backend.tf"
  value       = google_storage_bucket.tfstate.name
}
