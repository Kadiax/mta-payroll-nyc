##############################################
# workflow_sa / scheduler_sa are created in setup/wif-setup.tf (PR1), not
# here — creating a Service Account needs iam.serviceAccountAdmin, which
# only the bootstrap SA has (deploy SA deliberately stays narrower).
# Different Terraform state, so referenced by email string, not by
# resource. Their account_id strings below MUST match setup/'s literal
# "mta-workflow" / "mta-scheduler" values exactly.
##############################################

locals {
  workflow_service_account_email  = "mta-workflow@${var.project_id}.iam.gserviceaccount.com"
  scheduler_service_account_email = "mta-scheduler@${var.project_id}.iam.gserviceaccount.com"
}

##############################################
# workflow-job-iam.tf — lets the Workflow SA invoke each of the 4 Cloud
# Run Jobs. roles/run.invoker is NOT enough for the
# googleapis.run.v2.projects.locations.jobs.run connector (missing
# run.jobs.get) — roles/run.developer is the narrowest role that works,
# still scoped to just these 4 Jobs, not project-wide.
##############################################

resource "google_cloud_run_v2_job_iam_member" "workflow_can_run_create_datasets" {
  project  = var.project_id
  location = var.region
  name     = "mta-create-datasets"
  role     = "roles/run.developer"
  member   = "serviceAccount:${local.workflow_service_account_email}"
}

resource "google_cloud_run_v2_job_iam_member" "workflow_can_run_extract" {
  project  = var.project_id
  location = var.region
  name     = "mta-extract"
  role     = "roles/run.developer"
  member   = "serviceAccount:${local.workflow_service_account_email}"
}

resource "google_cloud_run_v2_job_iam_member" "workflow_can_run_load" {
  project  = var.project_id
  location = var.region
  name     = "mta-load"
  role     = "roles/run.developer"
  member   = "serviceAccount:${local.workflow_service_account_email}"
}

resource "google_cloud_run_v2_job_iam_member" "workflow_can_run_transform" {
  project  = var.project_id
  location = var.region
  name     = "mta-transform"
  role     = "roles/run.developer"
  member   = "serviceAccount:${local.workflow_service_account_email}"
}

# The bindings above still aren't enough on their own: the connector polls
# the long-running operation each jobs.run call spawns (run.operations.get),
# which isn't a sub-resource of any specific Job — no way to scope IAM
# narrower than project-level for that. roles/run.viewer on workflow_sa,
# project-level, is granted in setup/wif-setup.tf (PR1), not here — that
# grant needs resourcemanager.projectIamAdmin, which only the bootstrap SA
# has.

##############################################
# workflow.tf — Cloud Workflows definition
##############################################

resource "google_workflows_workflow" "pipeline" {
  project         = var.project_id
  region          = var.region
  name            = "mta-pipeline-workflow"
  description     = "Chains the 4 MTA payroll Cloud Run Jobs sequentially: create-datasets -> extract -> load -> transform"
  service_account = local.workflow_service_account_email
  source_contents = file("${path.module}/workflow-definition.yaml")
  # Provider defaults this to true (a newer safety feature) — false here
  # since this is a throwaway/learning TP that may eventually be torn down,
  # otherwise `terraform destroy` refuses outright.
  deletion_protection = false
}

# No google_workflows_workflow_iam_member/_binding/_policy resource type
# exists in provider 7.45.0 — nothing to scope here even if we wanted to;
# not that this resource needs any inbound IAM anyway (only Scheduler
# calls it, via workflows.invoker granted to scheduler_sa in setup/).

##############################################
# scheduler.tf — Cloud Scheduler, triggers the Workflow on a monthly cron
##############################################

resource "google_cloud_scheduler_job" "trigger" {
  project     = var.project_id
  region      = var.region
  name        = "mta-pipeline-scheduler-trigger"
  description = "Triggers the MTA payroll pipeline Workflow on the 1st of each month at 3am (Europe/Paris)"
  schedule    = "0 3 1 * *"
  time_zone   = "Europe/Paris"

  http_target {
    http_method = "POST"
    uri         = "https://workflowexecutions.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/workflows/${google_workflows_workflow.pipeline.name}/executions"

    oauth_token {
      service_account_email = local.scheduler_service_account_email
    }
  }

  # No depends_on here — the roles/workflows.invoker grant this needs lives
  # in setup/wif-setup.tf's Terraform state (different state, no direct
  # reference possible). setup/ was applied before infra/ (PR1 before PR4),
  # so no propagation concern in practice.
}
