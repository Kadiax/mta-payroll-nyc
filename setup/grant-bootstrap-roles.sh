#!/usr/bin/env bash
# setup/grant-bootstrap-roles.sh
#
# Grants the dedicated bootstrap SA (wif-github-gcp-bootstrap@...) the roles
# needed to run `terraform apply` on wif-setup.tf (create the WIF Pool,
# the deploy Service Account, the 4 runtime SAs, the workflow/scheduler SAs,
# and the state bucket).
# Also grants MY personal account the right to impersonate this SA.
#
# wif-github-gcp-bootstrap is a FIXED, non-parameterized name — only ONE
# such SA can exist at a time in this project. Make sure any previous TP's
# teardown fully destroyed its own wif-github-gcp-bootstrap before creating
# it again here (see gcp-wif-bootstrap/SKILL.md Gotchas).
#
# Creates the "wif-github-gcp-bootstrap" SA itself (idempotent — skipped if
# it already exists, e.g. on a re-run of this script for the same TP).
#
# Run ONCE per TP, before the first `terraform apply` in setup/, using a user
# account that already has IAM Admin rights on the project (e.g. the
# account that created the GCP project, via `gcloud auth login`).
#
# Expects setup/config.sh (copied from config.sh.template, real values
# filled in — config.sh itself is never committed) to already exist
# next to this script — that's where PROJECT_ID comes from.
#
# Usage: chmod +x grant-bootstrap-roles.sh && ./grant-bootstrap-roles.sh MY_EMAIL@example.com

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 MY_EMAIL@example.com"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/config.sh"

MY_EMAIL="$1"
SA_EMAIL="wif-github-gcp-bootstrap@${PROJECT_ID}.iam.gserviceaccount.com"

echo "==> Checking gcloud is authenticated"
gcloud auth list --filter="status:ACTIVE" --format="value(account)" | grep -q . \
  || { echo "Error: no active gcloud account. Run 'gcloud auth login' first."; exit 1; }

echo "==> Enabling required APIs on $PROJECT_ID"
gcloud services enable \
  iam.googleapis.com \
  iamcredentials.googleapis.com \
  sts.googleapis.com \
  storage.googleapis.com \
  cloudresourcemanager.googleapis.com \
  --project="$PROJECT_ID"

echo "==> Creating $SA_EMAIL (skipped if it already exists)"
if gcloud iam service-accounts describe "$SA_EMAIL" --project="$PROJECT_ID" > /dev/null 2>&1; then
  echo "    Already exists — reusing it. If this is a NEW TP, make sure this SA isn't a leftover"
  echo "    from a previous TP's incomplete teardown (see gcp-wif-bootstrap/SKILL.md Gotchas:"
  echo "    wif-github-gcp-bootstrap is a fixed name, only one may exist at a time)."
else
  gcloud iam service-accounts create wif-github-gcp-bootstrap \
    --project="$PROJECT_ID" \
    --display-name="WIF bootstrap (dedicated, per-TP)" \
    --quiet > /dev/null
fi

echo "==> Granting project roles to $SA_EMAIL"
for ROLE in \
  roles/iam.workloadIdentityPoolAdmin \
  roles/iam.serviceAccountAdmin \
  roles/resourcemanager.projectIamAdmin \
  roles/storage.admin \
  roles/serviceusage.serviceUsageAdmin
do
  echo "    - $ROLE"
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$ROLE" \
    --quiet > /dev/null
done

echo "==> Allowing $MY_EMAIL to impersonate $SA_EMAIL"
# --project is REQUIRED here even though it's embedded in $SA_EMAIL — this
# command resolves against the gcloud CLI's active config project otherwise,
# not the project encoded in the SA email, and fails NOT_FOUND if they differ.
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --project="$PROJECT_ID" \
  --member="user:${MY_EMAIL}" \
  --role="roles/iam.serviceAccountTokenCreator" \
  --quiet > /dev/null

echo "==> Final check — SA IAM policy (must include serviceAccountTokenCreator for $MY_EMAIL)"
gcloud iam service-accounts get-iam-policy "$SA_EMAIL" --project="$PROJECT_ID"

echo "==> Done. I can now impersonate $SA_EMAIL and run 'terraform apply' on wif-setup.tf."
