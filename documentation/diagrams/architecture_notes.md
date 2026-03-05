# Architecture Overview (MTA Payroll Project)

This project implements a public sector payroll analytics platform using a modern ELT architecture on Google Cloud Platform. It demonstrates a full-scale data engineering pipeline from raw ingestion to BI consumption.

## 📊 Dataset Technical Specifications

- Data Source: New York State Open Data - Data.gov.

- Volume: 78,309 records (~11.8 MB raw CSV).

- Temporal Scope: Comprehensive payroll data for the fiscal year 2025.

- Data Integrity: Automated PII protection via Salted Hashing (SHA-256) during the ingestion phase with Python/Pandas.

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
