# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A data engineering pipeline that ingests MTA (NY State) employee payroll open data, anonymizes PII, loads it into BigQuery, and transforms it with dbt into a star schema for a Looker Studio dashboard. Pipeline: **Python (extract/anonymize) → GCS (bronze CSV lake) → BigQuery (load) → dbt (silver/gold/analytics transforms)**.

## Security

- Never print a live credential (access token, API key, service account key content, etc.) into any command output — command output becomes part of the conversation context, same exposure risk as leaking it into CI logs or terminal history. When verifying that authentication/impersonation works, check success/failure only (exit code, redirect stdout to `/dev/null`), never echo the actual secret value.
- Before every commit, scan the changed files for anything that looks like a real secret or credential (API keys, tokens, private key blocks, actual service account key content) or a placeholder (e.g. `CHANGE_ME`, `REPLACE_WITH_YOUR_SECRET_SALT_FOR_ANONYMIZATION`) that got accidentally filled with a real sensitive value. Flag anything suspicious and wait for confirmation before proceeding, rather than committing silently.
- `config.yaml`, `dbt_mta_payroll/profiles.yml`, and `scripts/schemas/mta_payroll_schema.json` are gitignored and hold the GCP project id, bucket name, and the PII anonymization salt — only their `.example` counterparts should ever be committed.

## Commands

The pipeline is orchestrated via `Makefile` + Docker; there is no native (non-Docker) run path defined.

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

Running a single test (still requires the Docker image, or a local venv with `requirements.txt` installed):
```bash
pytest tests/test_extract_and_anonymize.py::test_hash_string
```

dbt commands (run from `dbt_mta_payroll/`, needs `profiles.yml` and `DBT_PROJECT_DIR`/BQ auth):
```bash
dbt deps
dbt build --full-refresh   # what scripts/transform.py invokes
dbt run --select stg_mta_payroll+
dbt test
```

### One-time local setup
```bash
cp config.yaml.example config.yaml
cp dbt_mta_payroll/profiles.yml.example dbt_mta_payroll/profiles.yml
cp scripts/schemas/mta_payroll_schema.json.example scripts/schemas/mta_payroll_schema.json
```
`config.yaml` and `dbt_mta_payroll/profiles.yml` are gitignored and contain the GCP project id, bucket name, and the PII anonymization salt — never commit real values or print them.

## Architecture

### Medallion layers (BigQuery datasets, defined in `config.yaml` → `gcp.datasets`)
- **bronze**: raw ingested CSV data (`raw_mta_payroll` table), append-only, tagged with `source_file` + `raw_ingested_at` for lineage/idempotency.
- **silver**: dbt staging views (`stg_mta_payroll`) — type casting, null handling, column renaming.
- **gold**: dbt warehouse tables — star schema (see below).
- **analytics**: dbt views/tables consumed directly by Looker Studio.

### Python ingestion (`scripts/`, run as `python -m scripts.<name>` inside the container)
- `create_datasets.py` — idempotently creates the 4 BQ datasets.
- `extract_and_anonymize.py` — downloads the source CSV, replaces `Name` with a salted SHA-256 hash (`name_hash`, first 16 hex chars) via `hash_string()`/`anonymize_data()`, uploads timestamped CSV to `gs://<bucket>/<raw_prefix>`. Missing names become `"UNKNOWN_EMPLOYEE"` before hashing, so all unknown employees intentionally collapse to one `name_hash`.
- `load_to_bq.py` — loads only GCS files not already present in BQ (`source_file` diff against a `_temp` staging table, then `INSERT ... SELECT` into the raw table), then runs `reconcile_load_integrity()` which raises if GCS file count != distinct `source_file` count in BQ. Schema comes from `scripts/schemas/mta_payroll_schema.json`.
- `transform.py` — shells out to `dbt build --full-refresh` in `dbt_project_dir`, streams dbt's stdout into the app logger, and appends `dbt_mta_payroll/logs/dbt.log` for audit.
- `utils/config.py` — Pydantic (frozen, non-empty-string-validated) config loaded from `./config.yaml` at repo root (relative path, so scripts must run with that as CWD — the Dockerfile's `WORKDIR /app` matches this).
- `utils/gcs_logger.py` — every script logs to an in-memory `StringIO` first and uploads it to `gs://<bucket>/<gcs_log_folder>` at the end (even on failure, via `finally`), rather than writing local log files.

### dbt project (`dbt_mta_payroll/`)
- `models/staging/stg_mta_payroll.sql`: 1:1 cleaned view over the bronze `source('bronze_mta_payroll', 'raw_mta_payroll')` source (see `models/staging/src_mta.yml`). Casts types, uppercases/trims categorical text, parses `MM/DD/YYYY` dates with `SAFE.PARSE_DATE`, coalesces numerics to 0.
- `models/warehouse/` (gold, materialized as `table` by default):
  - `dim_employee.sql` — **incremental, merge on `employee_key`**. Dedupes staging rows with `ROW_NUMBER() OVER (PARTITION BY name_hash, agency_name, job_title, start_date ...)`, keeping the most recent by `separation_date`/`raw_ingested_at` ("golden record"). Surrogate key = `FARM_FINGERPRINT(name_hash || agency_name || job_title || start_date)`. Incremental filter compares `raw_ingested_at` to `MAX(dbt_updated_at)` in `{{ this }}`.
  - `dim_agency.sql`, `dim_job_title.sql`, `dim_calendar.sql` — supporting dimensions.
  - `fct_payroll.sql` — joins staging to the three dimensions on natural keys to resolve surrogate FKs, partitioned by `fiscal_year` (range 2025–2030) and clustered by `agency_key, job_title_key`. Fact surrogate key is again a `FARM_FINGERPRINT` of the resolved FKs + fiscal year.
- `models/analytics/` (views by default): `obt_payroll.sql` (one-big-table for BI), `v_payroll_kpis.sql`, `v_payroll_composition.sql`, `v_hr_dynamics.sql`, `mv_overtime_by_job.sql` — these feed Looker Studio directly.
- `tests/reconcile_payroll_totals.sql`: custom dbt data test asserting `obt_payroll` totals match `v_payroll_kpis` totals within 0.01 — treat any change to either model as needing this reconciliation to stay green.
- `macros/generate_schema_name.sql` is overridden so dbt uses the **custom schema name directly** (not prefixed with the target dataset) — this is what makes `+schema: silver/gold/analytics` config in `dbt_project.yml` map to real dataset names instead of `<target_dataset>_silver` etc. Keep this in mind if adding new schemas/datasets.
- Uses packages `dbt_utils`, `dbt_expectations`, `dbt_date` (see `packages.yml`); run `dbt deps` after touching `packages.yml`.

### Key conventions
- Surrogate keys throughout are deterministic `FARM_FINGERPRINT` hashes of natural-key columns (not `dbt_utils.generate_surrogate_key` / random UUIDs) — this keeps re-runs idempotent.
- PII (employee names) never reaches BigQuery in cleartext; only `name_hash` does. The anonymization salt lives only in local `config.yaml` (gitignored).
- All GCP access uses Application Default Credentials — no service-account keys or hardcoded secrets in code.
