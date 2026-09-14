# gh-vars-config-template.sh
#
# Committed trace of the shape of gh-vars-config.sh — the real file (with
# actual values) stays strictly local, never committed (see .gitignore).
#
# Copy this to gh-vars-config.sh (next to gh-repo-vars.sh, i.e. in
# <repo>/.github/) and replace every XXX with the real value.

PROJECT_ID="XXX"
REGION="XXX"
GAR_NAME="XXX"
REPOSITORY_DESCRIPTION="XXX" # used as TF_VAR_repository_description by the terraform job
WIF_PROVIDER="XXX" # output of setup/'s WIF bootstrap (PR1): workload_identity_provider
WIF_SERVICE_ACCOUNT="XXX" # deploy SA email
RUNTIME_SA_CREATE_DATASETS="XXX"
RUNTIME_SA_EXTRACT="XXX"
RUNTIME_SA_LOAD="XXX"
RUNTIME_SA_TRANSFORM="XXX"
