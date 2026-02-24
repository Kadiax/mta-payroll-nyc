# Architecture Overview (MTA Payroll Project)

This project implements a public sector payroll analytics platform using a modern ELT architecture on Google Cloud Platform. It demonstrates a full-scale data engineering pipeline from raw ingestion to BI consumption.

## 🏗️ High-Level Architecture

## 🛠️ Technology Stack

- **Source Data**: MTA New York City Transit payroll and employment event datasets (CSV).
- **Data Landing**: **Google Cloud Storage (GCS)** for raw file persistence.
- **Data Warehouse**: **BigQuery** (Compute & Storage).
- **Transformation Layer**: **dbt (Data Build Tool)** for modular SQL modeling.
- **BI & Visualization**: **Looker Studio** for interactive reporting.

---

## 💎 Data Layers (Medallion Architecture)

The pipeline follows the Medallion architecture to ensure data quality and traceability:

| Layer           | Description                                      | Implementation                        |
| :-------------- | :----------------------------------------------- | :------------------------------------ |
| **BRONZE**      | Raw, immutable data landed from GCS.             | External tables or `bq load`.         |
| **SILVER**      | Cleaned and standardized staging models.         | dbt models with casting and renaming. |
| **GOLD (Star)** | Business-ready Star Schema (Facts & Dimensions). | `fct_payroll`, `dim_employee`.        |
| **OBT**         | One Big Table optimized for BI performance.      | Denormalized view for Looker Studio.  |

---

## 📈 Data Modeling & Governance

### 🔄 Staging Layer (Silver)

We implement Staging Layer as Views to ensure "Late Binding." This allows for immediate propagation of upstream schema changes without the cost of rebuilding tables for every run.

### 🔄 Dimensional Modeling (Gold):

#### dim_employee : Slowly Changing Dimensions (SCD)

We implement **SCD Type 2** on `dim_employee` to track historical changes (e.g., department transfers). This is managed via dbt snapshots:

- `dbt_valid_from`: Start of the record's validity.
- `dbt_valid_to`: End of the record's validity (null if current).

#### dim_calendar : Seed

Unlike source-dependent dimensions, dim_calendar is managed as a dbt seed to ensure a continuous and enriched time-series reference, enabling accurate trend analysis regardless of gaps in the source payroll data.

#### dim_agency & dim_job_title : Table

These are reference dimensions with low volatility. They are materialized as **Tables**, ensuring that any updates in agency naming or job classifications are fully refreshed during each run without the overhead of incremental logic.

#### Fact Tables : Incremental vs. Table

- **fct_payroll & fct_employment_events (Incremental)**: These tables handle large volumes of event-based data. Using an **Incremental** strategy ensures we only process the latest payroll periods, drastically reducing BigQuery slot usage and processing costs.

- **fct_agency_stats (Table)**: To ensure aggregate consistency across historical periods, this summary table is fully refreshed. This prevents data drift when historical payroll records are updated or backfilled.

### 🔄 Reporting Layer (OBT):

Instead of connecting Looker Studio directly to the Star Schema, We implemente the One Big Table (OBT) pattern:

- Denormalization: Facts and Dimensions are pre-joined into wide, flat tables.

- Performance: This eliminates the need for Looker Studio to perform complex joins at runtime, significantly reducing dashboard latency.

- Cost Optimization: Fewer joins mean lower BigQuery slot consumption per user interaction.

- Data freshness: As soon as your Gold tables (Facts and Dimensions) are updated by dbt, the OBT immediately reflects the changes for your dashboards.

### 🔐 Security & Privacy

- **Hashing**: Sensitive identifying information is masked using SHA-256 (`name_hash`).
- **Access Control**: Looker Studio's service account is granted access only to the reporting dataset.

### 🛠️ Data Quality

Data integrity is enforced through dbt tests:

- **Uniqueness & Non-Null**: Verified on all Primary Keys (`employee_key`, `payroll_key`).
- **Relationship Tests**: Ensuring Foreign Keys in Fact tables correctly point to existing Dimensions.

---

## ⚙️ Orchestration & Developer Experience

For this project, I deliberately chose a **“Keep It Simple, Stupid” (KISS)** approach, avoiding Airflow or dbt Cloud. The goal is to minimize operational overhead while ensuring full reproducibility.

### 🐳 Dockerized Environment

The entire pipeline is containerized to ensure that the code runs the same way locally as it does on a Compute Engine or Cloud Run instance.

- **Image**: Python 3.11-slim for lightness.
- **Security**: Secure mounting of Google Cloud Application Default Credentials (ADC) via Docker volumes.

### 🛠️ Make as Orchestrator

Instead of a complex orchestrator, I use a **Makefile** to manage the project lifecycle. This allows us to:

1. **Standardize commands**: A single `make all` command to build, test, and launch the pipeline.
2. **Documentation through code**: The Makefile serves as living documentation on the execution order (Dependencies).
3. **Portability**: Facilitates future integration into CI/CD (GitHub Actions).

### 💡 Why not Airflow / dbt Cloud?

- **Cost & Complexity**: For a single MTA data volume, the cost of a Cloud Composer (Airflow) instance is not justified.
- **Maintenance**: Fewer managed services means more focus on dbt transformation logic and data quality.
- **Scalability**: This Docker structure is “Ready-to-Cloud.” Switching from Make to a cloud orchestrator can be done simply by moving the `docker run` commands to workflow tasks.
