# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A data engineering pipeline that ingests MTA (NY State) employee payroll open data, anonymizes PII, loads it into BigQuery, and transforms it with dbt into a star schema for a Looker Studio dashboard. Pipeline: **Python (extract/anonymize) → GCS (bronze CSV lake) → BigQuery (load) → dbt (silver/gold/analytics transforms)**.

In production, the pipeline runs as 4 Cloud Run Jobs orchestrated by Cloud Workflows on a monthly Cloud Scheduler cron, deployed via GitHub Actions (WIF, no JSON keys). The same code also runs locally via Docker + `Makefile` for development. See `PROGRESS.md` (local, gitignored) for the full PR-by-PR history of the GCP deployment.

## Security

- Never print a live credential (access token, API key, service account key content, etc.) into any command output — command output becomes part of the conversation context, same exposure risk as leaking it into CI logs or terminal history. When verifying that authentication/impersonation works, check success/failure only (exit code, redirect stdout to `/dev/null`), never echo the actual secret value.
- Before every commit, scan the changed files for anything that looks like a real secret or credential (API keys, tokens, private key blocks, actual service account key content) or a placeholder (e.g. `CHANGE_ME`) that got accidentally filled with a real sensitive value. Flag anything suspicious and wait for confirmation before proceeding, rather than committing silently.
- `config.yaml`, `dbt_mta_payroll/profiles.yml`, and `scripts/schemas/mta_payroll_schema.json` are **committed** — none of them hold anything sensitive (project id, bucket name, region, BigQuery column schema; `profiles.yml` uses `method: oauth`, relying on Application Default Credentials, never a key file). There is no PII salt anywhere in this pipeline (see "PII hashing" below) — nothing to keep secret on the anonymization side.
- What IS gitignored and must never be committed: `setup/terraform.tfvars`, `setup/config.sh`, `infra/terraform.tfvars`, `.github/gh-vars-config.sh` (all hold real GCP project/resource identifiers — not secret values exactly, but kept local by convention, see `setup/*.template`/`*-template.sh` for their committed shape), plus anything matching `*.tfstate`, `*credentials*.json`, `*service-account*.json`.

## Commands

### Local development (Docker + Makefile)
```bash
make build            # docker build -t mta-pipeline .
make test             # run pytest inside the container
make create-datasets  # create bronze/silver/gold/analytics BQ datasets (scripts/create_datasets.py)
make extract          # download MTA CSV, hash PII, upload to GCS (scripts/extract_and_anonymize.py)
make load             # load new GCS CSVs into BigQuery raw table, idempotent (scripts/load_to_bq.py)
make transform        # run `dbt build --full-refresh` (scripts/transform.py)
make all              # build -> test -> create-datasets -> extract -> load -> transform
```
Each `make` target that talks to GCP mounts local `gcloud` Application Default Credentials into the container (`GOOGLE_APPLICATION_CREDENTIALS`), so `gcloud auth application-default login` must be run on the host first.

Running a single test (still requires the Docker image, or a local venv with `requirements.txt` installed — `venv/Scripts/dbt.exe`/`venv/Scripts/python.exe` on Windows):
```bash
pytest tests/test_extract_and_anonymize.py::test_hash_string
```

dbt commands (run from `dbt_mta_payroll/`, needs `profiles.yml` and BQ auth via ADC):
```bash
dbt deps
dbt build --full-refresh   # what scripts/transform.py invokes
dbt run --select stg_mta_payroll+
dbt test
```

### GCP infrastructure (Terraform)
Two separate Terraform roots, never mixed:
- `setup/` — WIF pool, deploy SA, 4 runtime SAs, workflow/scheduler SAs, the dedicated tfstate bucket. **Local state, applied manually** (impersonating the one-time bootstrap SA `wif-github-gcp-bootstrap`), never by CI. Any change here needs `resourcemanager.projectIamAdmin`/`iam.serviceAccountAdmin`, which only the bootstrap SA has.
- `infra/` — Artifact Registry repo, the data-lake GCS bucket (imported, pre-existed this Terraform setup) + its bucket-scoped IAM, the Cloud Workflows definition, the Cloud Scheduler job. **Remote GCS state** (`mta-payroll-nyc-tfstate`), applied by the `terraform` job in CI (deploy SA via WIF) on every manual pipeline run — see below.
```bash
cd setup && terraform init && terraform plan && terraform apply   # setup/ only — real GCP IAM changes, confirm before applying
cd infra && terraform init && terraform plan && terraform apply   # infra/ — safe to run locally too, same state CI uses
```
The 4 Cloud Run Jobs themselves are **never** Terraform-managed (deployed imperatively by CI, `gcloud run jobs deploy`) — by design, see `PROGRESS.md`.

### CI/CD (GitHub Actions)
`.github/workflows/ci-cd.yml` is **manual-trigger only** (`workflow_dispatch`, no `push:` trigger — this pipeline redeploys 4 real Cloud Run Jobs and can re-apply Terraform, so it never fires automatically):
```bash
gh workflow run ci-cd.yml                          # on main
gh workflow run ci-cd.yml --ref <branch>            # validate a branch's workflow before merging (only works once the workflow FILE already exists on the default branch)
```
Building/deploying steps: build one Docker image tagged `${{ github.sha }}` → push to Artifact Registry → `gcloud run jobs deploy` × 4 (one per script entrypoint, each with its own dedicated runtime SA — never the deploy SA).

### Running the production pipeline manually
```bash
gcloud workflows execute mta-pipeline-workflow --location=europe-west9 --project=mta-payroll-nyc
gcloud workflows executions describe <execution-id> --workflow=mta-pipeline-workflow --location=europe-west9   # check status/errors
gcloud scheduler jobs run mta-pipeline-scheduler-trigger --location=europe-west9 --project=mta-payroll-nyc      # exercises the real cron OAuth path
gcloud run jobs execute <job-name> --region=europe-west9 --project=mta-payroll-nyc   # run a single job directly (job names: mta-create-datasets, mta-extract, mta-load, mta-transform)
```
Cron: monthly, 1st at 3am Europe/Paris. Before re-running manually right after a previous run, check `gcloud workflows executions list mta-pipeline-workflow --location=europe-west9` for other still-`ACTIVE` executions first — two concurrent runs can both complete a real `extract`+upload before either is caught, leaving duplicate bronze files (harmless for correctness now that `fct_payroll` dedupes by snapshot, see below, but wasteful).

### One-time local setup
```bash
gcloud auth application-default login
```
`config.yaml`/`profiles.yml`/the BigQuery schema are already committed with real (non-secret) values — no `.example` copying needed for local dev, unlike earlier in this project's history.

## Architecture

### Medallion layers (BigQuery datasets, defined in `config.yaml` → `gcp.datasets`)
- **bronze**: raw ingested CSV data (`raw_mta_payroll` table), append-only, tagged with `source_file` + `raw_ingested_at` for lineage/idempotency. Never Terraform-managed — the 4 datasets (bronze/silver/gold/analytics) are created by the idempotent `create_datasets.py` script/Cloud Run Job, deliberately kept outside Terraform (a `google_bigquery_dataset` resource can force a destructive replace on some attribute changes — too risky for a dataset holding real data; see `PROGRESS.md`).
- **silver**: dbt staging views (`stg_mta_payroll`) — type casting, null handling, column renaming.
- **gold**: dbt warehouse tables — star schema (see below).
- **analytics**: dbt views/tables consumed directly by Looker Studio.

### Python ingestion (`scripts/`, run as `python -m scripts.<name>` inside the container — also the command each Cloud Run Job runs)
- `create_datasets.py` — idempotently creates the 4 BQ datasets. Also the first step of the production Workflow (runs every time, no-op after the first).
- `extract_and_anonymize.py` — downloads the source CSV, replaces `Name` with a plain SHA-256 hash (`name_hash`, first 16 hex chars) via `hash_string()`/`anonymize_data()` — **no salt**: the source (NY State Open Data) already publishes real names publicly, so a secret salt would add no real protection, only operational complexity of keeping it in sync everywhere forever (see `PROGRESS.md` "Decisions"). Missing names become `"UNKNOWN_EMPLOYEE"` before hashing, so all unknown employees intentionally collapse to one `name_hash`. Uploads timestamped CSV to `gs://<bucket>/<raw_prefix>`.
- `load_to_bq.py` — loads only GCS files not already present in BQ (`source_file` diff against a `_temp` staging table, then `INSERT ... SELECT` into the raw table), then runs `reconcile_load_integrity()` which raises if GCS file count != distinct `source_file` count in BQ. Schema comes from `scripts/schemas/mta_payroll_schema.json`.
- `transform.py` — shells out to `dbt build --full-refresh` in `dbt_project_dir`, streams dbt's stdout into the app logger, and appends `dbt_mta_payroll/logs/dbt.log` for audit.
- `utils/config.py` — Pydantic (frozen, non-empty-string-validated) config loaded from `./config.yaml` at repo root (relative path, so scripts must run with that as CWD — the Dockerfile's `WORKDIR /app` matches this, and so does Cloud Run's default).
- `utils/gcp_clients.py` — thin `bigquery.Client()`/`storage.Client()` wrappers. On Cloud Run, credentials resolve automatically via the attached runtime SA (no `GOOGLE_APPLICATION_CREDENTIALS` needed there — that env var is only for the local Docker/Makefile path, which mounts host ADC).
- `utils/gcs_logger.py` — every script logs to an in-memory `StringIO` first and uploads it to `gs://<bucket>/<gcs_log_folder>` at the end (even on failure, via `finally`), rather than writing local log files.

### dbt project (`dbt_mta_payroll/`)
- `models/staging/stg_mta_payroll.sql`: 1:1 cleaned view over the bronze `source('bronze_mta_payroll', 'raw_mta_payroll')` source (see `models/staging/src_mta.yml`). Casts types, uppercases/trims categorical text, parses `MM/DD/YYYY` dates with `SAFE.PARSE_DATE`, coalesces numerics to 0.
- `models/warehouse/` (gold, materialized as `table` by default):
  - `dim_employee.sql` — **incremental, merge on `employee_key`**. Dedupes staging rows with `ROW_NUMBER() OVER (PARTITION BY name_hash, agency_name, job_title, start_date ...)`, keeping the most recent by `separation_date`/`raw_ingested_at` ("golden record"). Surrogate key = `FARM_FINGERPRINT(name_hash || agency_name || job_title || start_date)`. Incremental filter compares `raw_ingested_at` to `MAX(dbt_updated_at)` in `{{ this }}`.
  - `fct_payroll.sql` — joins staging to the three dimensions on natural keys to resolve surrogate FKs, partitioned by `fiscal_year` (range 2025–2030) and clustered by `agency_key, job_title_key`. **Dedupes staging by the latest `record_updated_at` per (name_hash, agency_name, job_title, start_date, fiscal_year) before the join** — the source republishes cumulative payroll snapshots multiple times within an open fiscal year (confirmed on real data: same employee/year, two rows with different `record_updated_at` and different, larger cumulative pay), and without this dedup the fact table's surrogate key collides across snapshots, both failing `unique_fct_payroll_payroll_key` and silently double-counting pay in downstream `SUM(total_earnings)` aggregates. Same `ROW_NUMBER()` pattern as `dim_employee.sql`.
  - `dim_agency.sql`, `dim_job_title.sql`, `dim_calendar.sql` — supporting dimensions.
- `models/analytics/` (views by default): `obt_payroll.sql` (one-big-table for BI), `v_payroll_kpis.sql`, `v_payroll_composition.sql`, `v_hr_dynamics.sql`, `mv_overtime_by_job.sql` — these feed Looker Studio directly.
- `tests/reconcile_payroll_totals.sql`: custom dbt data test asserting `obt_payroll` totals match `v_payroll_kpis` totals within 0.01 — treat any change to either model as needing this reconciliation to stay green.
- `macros/generate_schema_name.sql` is overridden so dbt uses the **custom schema name directly** (not prefixed with the target dataset) — this is what makes `+schema: silver/gold/analytics` config in `dbt_project.yml` map to real dataset names instead of `<target_dataset>_silver` etc. Keep this in mind if adding new schemas/datasets.
- Uses packages `dbt_utils`, `dbt_expectations`, `dbt_date` (see `packages.yml`); run `dbt deps` after touching `packages.yml`.

### GCP infra (`setup/`, `infra/`, `.github/workflows/`)
- **`setup/`** (WIF bootstrap, `gcp-wif-bootstrap` skill): dedicated WIF pool (`mta-wif-pool`, condition scoped to `Kadiax/mta-payroll-nyc`), deploy SA (`mta-wif-deploy` — `run.admin`, `artifactregistry.admin`, `storage.admin`, `workflows.editor`, `cloudscheduler.admin`), 4 dedicated runtime SAs (`mta-rt-create-ds`, `mta-rt-extract`, `mta-rt-load`, `mta-rt-transform` — each granted only the BigQuery/Storage roles its own script actually needs, not a shared union), `mta-workflow`/`mta-scheduler` SAs, the tfstate bucket. Everything lives in `mta-payroll-nyc` itself (single-project setup, not a separate shared bootstrap project).
- **`infra/`**: Artifact Registry repo (`mta-pipeline`), the data-lake bucket (imported, bucket-scoped IAM per runtime SA), `google_workflows_workflow` (`mta-pipeline-workflow`, source is `infra/workflow-definition.yaml` — plain-text `file()`, no Terraform variable interpolation inside it, so Job names there must be kept in sync by hand with `workflows.tf`'s `google_cloud_run_v2_job_iam_member` blocks and the actual deployed Job names), `google_cloud_scheduler_job` (`mta-pipeline-scheduler-trigger`).
- **`.github/workflows/ci-cd.yml`**: `terraform` job (applies `infra/`) → `deploy` job (build/push image, deploy the 4 Jobs). `workflow_dispatch` only, see Commands above.

### Key conventions
- Surrogate keys throughout are deterministic `FARM_FINGERPRINT` hashes of natural-key columns (not `dbt_utils.generate_surrogate_key` / random UUIDs) — this keeps re-runs idempotent.
- PII (employee names) never reaches BigQuery in cleartext; only `name_hash` (plain SHA-256, no salt — see above) does.
- All GCP access uses Application Default Credentials — no service-account keys or hardcoded secrets in code. Locally: host ADC mounted into the container. On Cloud Run: the attached runtime SA, automatically.
- Runtime identities are least-privilege and per-script, not shared — see `setup/wif-setup.tf`'s comments for exactly which BigQuery/Storage roles each of the 4 runtime SAs has and why.
