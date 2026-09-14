variable "project_id" {
  description = "GCP project ID — no default, must come from terraform.tfvars"
  type        = string
}

variable "region" {
  description = "Default GCP region — used as the Artifact Registry repo location"
  type        = string
  default     = "europe-west9"
}

variable "repository_id" {
  description = "Artifact Registry repository ID — must be unique within the project/region"
  type        = string
  default     = "mta-pipeline"
}

variable "repository_description" {
  description = "Human-readable description for the Artifact Registry repo"
  type        = string
  default     = "Docker images for the MTA payroll ELT pipeline"
}

resource "google_artifact_registry_repository" "app_repo" {
  project       = var.project_id
  location      = var.region
  repository_id = var.repository_id
  description   = var.repository_description
  format        = "DOCKER"
}
