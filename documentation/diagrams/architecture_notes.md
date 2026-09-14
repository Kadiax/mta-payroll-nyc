# Architecture Overview (MTA Payroll Project)

This project implements a public sector payroll analytics platform using a modern ELT architecture on Google Cloud Platform. It demonstrates a full-scale data engineering pipeline from raw ingestion to BI consumption.

## 📊 Dataset Technical Specifications

- Data Source: New York State Open Data - Data.gov.

- Volume: 78,309 records (~11.8 MB raw CSV).

- Temporal Scope: Comprehensive payroll data for the fiscal year 2025.

- Data Integrity: Automated PII protection via SHA-256 hashing during the ingestion phase with Python/Pandas (no salt — the source is already public, real names included; see Security & Privacy below for why that's the right call here).

## 🏗️ High-Level Architecture

See the "🏛️ Architecture Diagram" section in the [root README](../../README.md) for the full CI/CD → orchestration → data → BI diagram — kept in one place only (GitHub Markdown has no include/transclusion mechanism, so a single canonical copy is more maintainable than syncing two).

## 🛠️ Technology Stack

- **Source Data**: MTA New York City Transit payroll and employment event datasets (CSV).
- **Data Landing**: **Google Cloud Storage (GCS)** for raw file persistence.
- **Data Warehouse**: **BigQuery** (Compute & Storage).
- **Infrastructure as Code**: **Terraform**, two separate state roots — a one-time manually-applied bootstrap (WIF pool, service accounts) and a CI-applied application layer (Artifact Registry, data-lake bucket, Workflow, Scheduler).
- **CI/CD**: **GitHub Actions**, keyless authentication to GCP via **Workload Identity Federation**.
- **Compute**: **Cloud Run Jobs** — 4 independent, least-privilege containers, one per pipeline stage.
- **Orchestration**: **Cloud Workflows** (sequential execution, built-in failure propagation) triggered by **Cloud Scheduler** (monthly cron).
- **Transformation Layer**: **dbt (Data Build Tool)** for modular SQL modeling.
- **BI & Visualization**: **Looker Studio** for interactive reporting.

---

## 💎 Data Layers (Medallion Architecture)

The pipeline follows the Medallion architecture to ensure data quality and traceability:

| Layer           | Description                                       | Implementation                        |
| :-------------- | :------------------------------------------------ | :------------------------------------ |
| **BRONZE**      | Raw, immutable data landed from GCS.              | External tables or `bq load`.         |
| **SILVER**      | Cleaned and standardized staging models.          | dbt models with casting and renaming. |
| **GOLD (Star)** | Business-ready Star Schema (Facts & Dimensions).  | `fct_payroll`, `dim_employee`.        |
| **Analytics**   | One Big Table optimized for BI performance (OBT). | Denormalized view for Looker Studio.  |

---

## 📈 Data Modeling & Governance

### 🔄 Staging Layer (Silver)

We implement Staging Layer as Views to ensure "Late Binding." This allows for immediate propagation of upstream schema changes without the cost of rebuilding tables for every run.

### 🔄 Dimensional Modeling (Gold):

#### dim_employee : Incremental (Merge)

To handle multi-year data ingestion (2025-2026) while maintaining data integrity, this model is materialized as Incremental using a Merge strategy on BigQuery.

- Deduplication Logic: Implements a window function (ROW_NUMBER) to ensure only the "freshest" record per employee is kept.

- Priority Ranking: The logic prioritizes records with explicit separation_date and uses raw_ingested_at as a tie-breaker to guarantee the most recent version of a contract is persisted.

- Performance Optimization: Uses is_incremental() filtering to process only new data since the last run, significantly reducing BigQuery scan costs and execution time.

- Clustering: Data is clustered by agency_name and job_title to optimize downstream query performance in the BI layer.

#### dim_calendar, dim_agency & dim_job_title : Table

These are reference dimensions with low volatility. They are materialized as **Tables**, ensuring that any updates in agency naming or job classifications are fully refreshed during each run without the overhead of incremental logic.

#### fct_payroll : Table

- Strategy: Full Refresh with Partitioning & Clustering

- Description: This table handles large volumes of payroll records. Instead of a basic incremental merge, we utilize BigQuery Native Partitioning by fiscal_year.

- Why this choice? - Cost Efficiency: By partitioning on the fiscal year, BigQuery only scans the relevant data blocks when filtering by date, drastically reducing slot usage and query costs.

- Performance: We apply Clustering on agency_key and job_title_key to speed up complex aggregations and joins within each partition.

- Reliability: Since historical payroll data can occasionally be updated, a full refresh strategy (re-calculating the partition) ensures 100% data consistency without the complexity of incremental state management at this stage.

### 🔄 Analytics Layer (BI):

Instead of connecting Looker Studio directly to the Star Schema, We implemente the One Big Table (OBT) pattern:

- Denormalization: Facts and Dimensions are pre-joined into wide, flat tables.

- Performance: This eliminates the need for Looker Studio to perform complex joins at runtime, significantly reducing dashboard latency.

- Cost Optimization: Fewer joins mean lower BigQuery slot consumption per user interaction.

- Data freshness: As soon as your Gold tables (Facts and Dimensions) are updated by dbt, the OBT immediately reflects the changes for your dashboards.

### 🔐 Security & Privacy

- **Data Privacy**: Applied SHA-256 hashing on PII (Personally Identifiable Information) such as employee names to avoid exposing real names in the downstream dashboard. No secret salt: the source (NY State Open Data) already publishes real names, so a salt would add operational complexity (keeping it identical everywhere, forever) without real protection against a determined party — the honest threat model here is casual re-identification, not a targeted attack.
- **GDPR Compliance**: Designed the pipeline to follow GDPR principles (data minimization and storage limitation) by only processing and storing fields strictly necessary for payroll analysis.
- **Access Management**: Implemented Google ADC (Application Default Credentials) to handle authentication securely, eliminating the need to store or hardcode sensitive JSON key files within the repository.
- **Scalable Architecture**: Leveraged a Medallion Architecture (Bronze/Silver/Gold) to transform raw, messy records into a clean, analytics-ready Star Schema.

### 🛠️ Data Quality

Data integrity is enforced through dbt tests:

- **Uniqueness & Non-Null**: Verified on all Primary Keys (`employee_key`, `payroll_key`).
- **Relationship Tests**: Ensuring Foreign Keys in Fact tables correctly point to existing Dimensions.

---

## ⚙️ Orchestration & Developer Experience

For this project, I deliberately chose a **"Keep It Simple, Stupid" (KISS)** approach for production orchestration — avoiding a managed workflow engine (Cloud Composer/Airflow, dbt Cloud) in favor of serverless GCP-native building blocks. The same containerized code runs identically in local dev and in production; only what triggers it and where it runs differs.

### 🐳 Dockerized Environment

The entire pipeline is containerized so the code runs the same way locally as it does in production.

- **Image**: Python 3.11-slim for lightness.
- **Credentials**: Application Default Credentials (ADC) everywhere — mounted via Docker volume for local `make` runs, automatically provided by the attached service account for Cloud Run Jobs. No JSON keys anywhere, local or CI.

### 🛠️ Local dev: Make as Orchestrator

For local development, a **Makefile** manages the project lifecycle:

1. **Standardize commands**: A single `make all` command to build, test, and launch the pipeline.
2. **Documentation through code**: The Makefile serves as living documentation on the execution order (dependencies).

### ☁️ Production: Cloud Run Jobs + Cloud Workflows + Cloud Scheduler

The same 4 pipeline stages run in production as independent **Cloud Run Jobs** — `create-datasets`, `extract`, `load`, `transform` — each deployed with its own dedicated, least-privilege service account (scoped to only the BigQuery/Storage roles that specific stage needs). **Cloud Workflows** chains them sequentially, relying on GCP's built-in execution-failure propagation instead of custom retry/error-handling code. **Cloud Scheduler** triggers the Workflow on a monthly cron. Deployment is entirely CI-driven: GitHub Actions authenticates to GCP via **Workload Identity Federation** (no service-account JSON keys), Terraform provisions the stable infrastructure (Artifact Registry, the data-lake bucket, the Workflow, the Scheduler), and a second job builds/pushes the Docker image and redeploys the 4 Jobs.

### 💡 Why not Airflow / dbt Cloud?

- **Cost & Complexity**: For this data volume, the cost and operational overhead of a Cloud Composer (Airflow) instance isn't justified — Cloud Workflows + Cloud Scheduler cover "run these steps in order, on a schedule, and fail loudly" with zero infrastructure to manage and effectively no idle cost.
- **Maintenance**: Fewer managed services means more focus on dbt transformation logic and data quality, not orchestrator upkeep.
- **Still "Ready-to-Cloud" if requirements grow**: if task dependencies ever get genuinely complex (branching, backfills, cross-pipeline dependencies), the same Cloud Run Jobs slot directly into Airflow/Dagster as tasks without rewriting the pipeline logic itself — only the orchestration layer would change.
