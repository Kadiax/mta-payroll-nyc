#!/usr/bin/env bash
# Sets this repo's GitHub Actions repository variables — the non-secret
# config ci-cd.yml reads as ${{ vars.* }} instead of hardcoding it in the
# workflow YAML.
#
# These are genuinely not secrets (WIF provider path, SA emails, resource
# names — nothing here authenticates on its own), hence GitHub Actions
# VARIABLES, not SECRETS. Even so, the actual values live in
# gh-vars-config.sh (next to this script), never committed — same
# convention as setup/config.sh. This script itself has no per-repo data
# in it, safe to commit and run as-is.
#
# Copy gh-vars-config-template.sh to gh-vars-config.sh (next to this
# script) first and fill in every XXX.
#
# Requires: gh CLI authenticated (gh auth login), run from inside the
# target repo.
#
# Usage: chmod +x gh-repo-vars.sh && ./gh-repo-vars.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/gh-vars-config.sh"

gh variable set PROJECT_ID --body "$PROJECT_ID"
gh variable set REGION --body "$REGION"
gh variable set GAR_NAME --body "$GAR_NAME"
gh variable set REPOSITORY_DESCRIPTION --body "$REPOSITORY_DESCRIPTION"
gh variable set WIF_PROVIDER --body "$WIF_PROVIDER" # output of setup/'s WIF bootstrap (PR1): workload_identity_provider
gh variable set WIF_SERVICE_ACCOUNT --body "$WIF_SERVICE_ACCOUNT" # deploy SA
gh variable set RUNTIME_SA_CREATE_DATASETS --body "$RUNTIME_SA_CREATE_DATASETS"
gh variable set RUNTIME_SA_EXTRACT --body "$RUNTIME_SA_EXTRACT"
gh variable set RUNTIME_SA_LOAD --body "$RUNTIME_SA_LOAD"
gh variable set RUNTIME_SA_TRANSFORM --body "$RUNTIME_SA_TRANSFORM"

echo "==> Done. Verify with: gh variable list"
