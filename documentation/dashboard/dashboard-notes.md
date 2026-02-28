# 📂 Project Context: MTA Employee Payroll Analysis

## 1. What is the MTA?

The **Metropolitan Transportation Authority (MTA)** is North America's largest transportation network, serving a population of 15.3 million people across New York City and its surrounding counties.

- **Scale**: It is one of the largest employers in the United States, managing the iconic NYC Subway, regional buses, and commuter rail lines such as the Long Island Rail Road (LIRR) and Metro-North.
- **Nature**: As a public benefit corporation, its financial operations are subject to strict transparency and public oversight.

## 2. Data Origin & Public Accountability

In accordance with the **New York State Public Authorities Law**, public agencies are mandated to disclose employee compensation. This project leverages **Open Data** to provide visibility into the allocation of public funds.

- **The "Overtime" Debate**: A primary focus of this analysis is **Overtime (OT) pay**, which is a frequent subject of public policy debate and fiscal scrutiny in New York.
- **Dataset Source**: [MTA Employee Payroll (Beginning 2025)](https://catalog.data.gov/dataset/mta-employee-payroll-beginning-2025) via Data.gov.

## 3. Understanding the "Beginning 2025" Series

This dataset represents a new reporting cycle or system migration initiated by the MTA.

- **Scope**: It contains granular payroll data starting from **Fiscal Year 2025**.
- **Key Metrics**: This includes base salaries, overtime earnings, retro-active pay, and Year-To-Date (YTD) totals for every active employee.
- **Update Frequency**: The data is refreshed periodically to reflect ongoing expenditures throughout the current fiscal year.

## 4. Agency Breakdown

The dataset identifies several distinct agencies under the MTA umbrella. Understanding these is key to analyzing the payroll distribution:

1.  **NYCTA (NYC Transit Authority)**: The largest component, operating the iconic New York City Subway and most local bus routes.
2.  **MTA BUS**: A separate division created to consolidate various private bus franchises under MTA management.
3.  **MABSTOA (Manhattan & Bronx Surface Transit Operating Authority)**: A subsidiary of NYCTA focused specifically on bus operations in Manhattan and the Bronx.
4.  **LIR (Long Island Rail Road)**: The busiest commuter railroad in North America, connecting Manhattan to Long Island.
5.  **MNR (Metro-North Railroad)**: The commuter rail service connecting NYC with its northern suburbs (Westchester, Connecticut).
6.  **BTA (Bridges and Tunnels)**: Officially the _Triborough Bridge and Tunnel Authority_, managing toll bridges and tunnels in NYC.
7.  **SIR (Staten Island Railway)**: The rapid transit line serving the borough of Staten Island.
8.  **MTA-P (MTA Police)**: They are dedicated to providing safe travel for the commuters of the LIRR, Metro-North, and Staten Island Railway.
9.  **MTA-C (MTA Construction & Development)**: Plans, develops, and executes critical infrastructure investments that help ensure reliable, accessible, and sustainable transit for the region

---

## 🛠️ Technical Implementation Highlights

To ensure the integrity of this financial data, the following Engineering principles were applied:

- **Robust Type Casting**: Utilized `SAFE_CAST` in **dbt** to handle raw CSV strings and ensure numeric precision.
- **NULL Handling Strategy**: Implemented strategic `COALESCE` logic to prevent "NULL contamination" during total earnings aggregations while preserving NULLs for accurate average (`AVG`) calculations.
- **Containerized Pipeline**: The entire ELT pipeline is orchestrated via **Docker**, ensuring a reproducible environment from ingestion to BigQuery.

## 🔗 Data Sources & Governance

To ensure transparency and accuracy, this project relies on official New York State and MTA resources:

- **Primary Dataset**: [MTA Employee Payroll (Beginning 2025)](https://catalog.data.gov/dataset/mta-employee-payroll-beginning-2025) via Data.gov.
- **Agency Definitions**: Descriptions and organizational structure are sourced from the [Official MTA Agency Overview](https://www.mta.info/agency).
- **Legal Mandate**: Public disclosure of this payroll data is governed by the [New York State Public Authorities Law § 2800](https://www.nysenate.gov/legislation/laws/PBA/2800), ensuring public oversight of authority operations.
