#!/usr/bin/env bash
# setup/teardown.sh
#
# Cleanly destroys everything created for this repo (gcp-wif-bootstrap +
# gcp-cloud-run-job-pipeline skills), which has its OWN dedicated
# Terraform state bucket (mta-payroll-nyc-tfstate, never shared).
#
# Order:
#   0. Resources never tracked by Terraform (the 4 Cloud Run Jobs)
#   1. Application resources (infra/*.tf: Artifact Registry, the
#      data-lake bucket, its bucket-scoped IAM, the Workflow, the
#      Scheduler)
#   2. Bootstrap infra (setup/wif-setup.tf — pool, SAs, state bucket)
#      -> the state bucket is destroyed automatically here, thanks to
#         -var="tfstate_force_destroy=true" passed explicitly
#   3. Final verification
#   4. (optional, prompted) Destroy the dedicated bootstrap SA itself
#      (wif-github-gcp-bootstrap)
#
# ⚠️ WARNING specific to this repo: infra/data-lake-bucket.tf manages the
# REAL data-lake bucket (mta-payroll-nyc), imported from a pre-existing
# bucket that already held production bronze CSVs before Terraform ever
# touched it (see PROGRESS.md). Step 1's `terraform destroy` WILL delete
# this bucket and everything in it along with the CI/CD plumbing. If you
# ever want to tear down the CI/CD infra but KEEP the data-lake bucket
# and its contents, run `terraform state rm google_storage_bucket.data_lake
# google_storage_bucket_iam_member.create_datasets_can_write_logs
# google_storage_bucket_iam_member.extract_can_manage_objects
# google_storage_bucket_iam_member.load_can_manage_objects
# google_storage_bucket_iam_member.transform_can_write_logs` (from infra/)
# BEFORE step 1's destroy, so Terraform forgets about the bucket without
# touching the real resource.
#
# Prerequisite: gcloud auth application-default login (my own account,
# project owner — simpler than SA impersonation for a one-off cleanup).
# Reads PROJECT_ID/REGION from setup/config.sh — never committed (see
# gcp-wif-bootstrap SKILL.md convention #9), must already exist locally.
#
# Usage: ./teardown.sh [path_to_repo]
#   Defaults to the current directory if no path_to_repo is given.

set -euo pipefail

REPO_DIR="${1:-.}"
CONFIG_FILE="$REPO_DIR/setup/config.sh"
if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: $CONFIG_FILE not found."
  echo "Copy setup/config.sh.template to $CONFIG_FILE and fill in the real PROJECT_ID/REGION first."
  exit 1
fi
source "$CONFIG_FILE"

CLOUD_RUN_SERVICE=""   # this repo has no Cloud Run Service, only Jobs
CLOUD_RUN_JOBS=("mta-create-datasets" "mta-extract" "mta-load" "mta-transform")

confirm() {
  read -r -p "$1 [y/N] " response
  case "$response" in
    [yY][eE][sS]|[yY]) true ;;
    *) echo "Cancelled."; exit 1 ;;
  esac
}

echo "=================================================="
echo " Teardown — $REPO_DIR"
echo "=================================================="
echo "GCP project: $PROJECT_ID"
confirm "This will destroy ALL GCP resources created by this repo, INCLUDING the real data-lake bucket and its contents (see the warning at the top of this script). Continue?"

# --- Step 0 — Resources never tracked by Terraform ---
echo
echo "==> [0/4] Deleting resources never managed by Terraform"

if [ -n "$CLOUD_RUN_SERVICE" ]; then
  if gcloud run services describe "$CLOUD_RUN_SERVICE" --region="$REGION" --project="$PROJECT_ID" &>/dev/null; then
    gcloud run services delete "$CLOUD_RUN_SERVICE" --region="$REGION" --project="$PROJECT_ID" --quiet
    echo "    Service $CLOUD_RUN_SERVICE deleted."
  else
    echo "    Service $CLOUD_RUN_SERVICE not found, already deleted or never deployed."
  fi
fi

for JOB in "${CLOUD_RUN_JOBS[@]}"; do
  if gcloud run jobs describe "$JOB" --region="$REGION" --project="$PROJECT_ID" &>/dev/null; then
    gcloud run jobs delete "$JOB" --region="$REGION" --project="$PROJECT_ID" --quiet
    echo "    Job $JOB deleted."
  else
    echo "    Job $JOB not found, already deleted or never deployed."
  fi
done

# --- Step 1 — Application resources (infra/) ---
echo
echo "==> [1/4] Destroying application resources ($REPO_DIR/infra)"
(cd "$REPO_DIR/infra" && terraform init -input=false && terraform destroy -auto-approve)

# --- Step 2 — Bootstrap infra (pool, SAs, state bucket) ---
echo
echo "==> [2/4] Destroying bootstrap infra ($REPO_DIR/setup)"
echo "    State bucket protection lifted explicitly for this intentional destroy."
echo "    NOTE: 'terraform destroy -var=...' alone was unreliable in practice for"
echo "    force_destroy on google_storage_bucket (known provider quirk — destroy"
echo "    can fall back to the value already stored in state). Apply the var change"
echo "    first so it's actually written to state via a real API call, then destroy."
(cd "$REPO_DIR/setup" && terraform init -input=false && \
  terraform apply -auto-approve -var="tfstate_force_destroy=true" && \
  terraform destroy -auto-approve -var="tfstate_force_destroy=true")

# --- Step 3 — Final verification ---
echo
echo "==> [3/4] Final verification"
echo "    Remaining Service Accounts (wif-github-gcp-bootstrap, unless destroyed"
echo "    in step 4 below, is expected):"
gcloud iam service-accounts list --project="$PROJECT_ID" --format="value(email)"

echo "    Remaining buckets:"
gcloud storage buckets list --project="$PROJECT_ID" --format="value(name)" || true

echo "    Remaining Artifact Registry repos:"
gcloud artifacts repositories list --project="$PROJECT_ID" --format="value(name)" || true

echo "    Remaining Cloud Run jobs:"
gcloud run jobs list --project="$PROJECT_ID" --format="value(metadata.name)" || true

echo "    Remaining Workflows:"
gcloud workflows list --project="$PROJECT_ID" --format="value(name)" || true

echo "    Remaining Scheduler jobs:"
gcloud scheduler jobs list --project="$PROJECT_ID" --location="$REGION" --format="value(name)" || true

echo "    Remaining BigQuery datasets (bronze/silver/gold/analytics — never"
echo "    Terraform-managed, see PROGRESS.md — not touched by this script,"
echo "    delete manually with 'bq rm -r -f -d' if you also want these gone):"
bq ls --project_id="$PROJECT_ID" || true

# --- Step 4 (optional) — Destroy the dedicated bootstrap SA itself ---
echo
echo "==> [4/4] (optional) Destroy the dedicated bootstrap SA"
read -r -p "Also destroy the dedicated bootstrap SA wif-github-gcp-bootstrap? [y/N] " DESTROY_BOOTSTRAP
if [[ "$DESTROY_BOOTSTRAP" =~ ^[yY]([eE][sS])?$ ]]; then
  BOOTSTRAP_SA="wif-github-gcp-bootstrap@${PROJECT_ID}.iam.gserviceaccount.com"
  if gcloud iam service-accounts describe "$BOOTSTRAP_SA" --project="$PROJECT_ID" &>/dev/null; then
    gcloud iam service-accounts delete "$BOOTSTRAP_SA" --project="$PROJECT_ID" --quiet
    echo "    $BOOTSTRAP_SA deleted — name is now free for the next TP."
  else
    echo "    $BOOTSTRAP_SA not found, already deleted."
  fi
else
  echo "    Skipped — wif-github-gcp-bootstrap left in place."
fi

echo
echo "==> Done."
