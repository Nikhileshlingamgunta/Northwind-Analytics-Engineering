# Northwind Analytics Engineering Capstone (dbt + DuckDB + Metabase)
## Architecture

```text
                ┌─────────────────────────────┐
                │        Raw CSV Seeds         │
                │  (Northwind extract files)   │
                └──────────────┬──────────────┘
                               │  dbt seed
                               ▼
                ┌─────────────────────────────┐
                │          DuckDB              │
                │         dev.duckdb           │
                └──────────────┬──────────────┘
                               │  dbt run
                               ▼
        ┌──────────────────────────────────────────────┐
        │                 dbt Models                   │
        │  Staging: stg_*   →   Marts: dim_*/fct_*     │
        │  KPI marts: mart_* (monthly_kpis, cohorts,   │
        │  customer_health, data_quality_scorecard)    │
        └──────────────┬───────────────────────────────┘
                       │  dbt test + documentation
                       ▼
        ┌──────────────────────────────────────────────┐
        │              Trust Layer (dbt)               │
        │  - not_null / unique / relationships tests   │
        │  - reproducible transformations              │
        │  - lineage & docs                            │
        └──────────────┬───────────────────────────────┘
                       │  BI connection (DuckDB driver)
                       ▼
        ┌──────────────────────────────────────────────┐
        │                  Metabase                    │
        │  4-page Executive Dashboard:                 │
        │  1) Executive Overview                       │
        │  2) Customer Inactivity                      │
        │  3) Cohort Retention                         │
        │  4) Data Quality Scorecard                   │
        └──────────────────────────────────────────────┘

Overview

This project demonstrates an end-to-end analytics engineering workflow using the classic Northwind dataset. I built a modern analytics stack on a local machine using:
	•	DuckDB as the analytical warehouse
	•	dbt for transformations, tests, and documentation
	•	Metabase for dashboarding and exploration

The final output is a 4-page executive dashboard that includes:
	1.	Executive Overview
	2.	Customer Inactivity
	3.	Cohort Retention
	4.	Data Quality Scorecard

⸻

Problem Statement

Build a lightweight analytics platform for Northwind to support executive decision-making with:
	•	Reliable curated models (staging + marts)
	•	Reusable KPI marts
	•	Quality checks for trust
	•	Clear dashboards for business stakeholders

⸻

Tech Stack
	•	dbt (core transformations + testing + documentation)
	•	DuckDB (local analytics warehouse stored as a .duckdb file)
	•	Metabase (BI dashboards + interactive exploration)
	•	SQL (metrics, marts, cohort logic)
	•	macOS-compatible local setup

⸻

Data Model (High-level)

Staging Layer
	•	stg_customer
	•	stg_product
	•	stg_order_detail
	•	stg_order_updated (order header / order updates)

Marts Layer
	•	dim_customer
	•	dim_product
	•	fct_order_items

KPI / Analytics Marts
	•	mart_monthly_kpis
	•	mart_customer_health
	•	mart_customer_cohorts
	•	mart_data_quality_scorecard

⸻
Dashboards (Metabase)

Page 1 — Executive Overview

Purpose: monthly business performance snapshot.
	•	Total Revenue
	•	Total Orders
	•	Average Order Value
	•	Repeat Rate
	•	Revenue trend by month
	•	Active customers trend

Page 2 — Customer Inactivity

Purpose: identify inactive / at-risk customers.
	•	Inactive 90+ days customers
	•	Inactivity age buckets
	•	Top at-risk customers table (days since last order, lifetime value, etc.)

Page 3 — Cohort Retention

Purpose: retention analysis by cohort month.
	•	Cohort size by month
	•	Average retention curve
	•	Cohort matrix (retention rates by months since first)

Page 4 — Data Quality

Purpose: transparency + trust in the model.
	•	Quality status breakdown (Good/Warning/Bad)
	•	Models by layer and status
	•	Avg PK completeness
	•	Row count vs PK completeness
	•	Data quality scorecard table
How to Run Locally (macOS)

1) Clone the repo
git clone <your_repo_url>
cd analytics-engineer-capstone/capstone_ae

2) Create a virtual environment and install dependencies
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

3) Run dbt seeds and models
dbt deps
dbt seed
dbt run
dbt test

4) Inspect DuckDB output (optional)
duckdb dev.duckdb "show tables;"
duckdb dev.duckdb "select count(*) from fct_order_items;"

5) Run dbt docs (optional)
dbt docs generate
dbt docs serve

Metabase Setup (Local)

1) Start Metabase
java -jar metabase.jar

Metabase runs at:
	•	http://localhost:3000
	
2) Add DuckDB database in Metabase
	•	Admin → Databases → Add database
	•	Choose DuckDB
	•	Point to your DuckDB file, e.g.
/Users/<you>/.../capstone_ae/dev.duckdb

3) Build dashboards

Use the marts:
	•	mart_monthly_kpis
	•	mart_customer_health
	•	mart_customer_cohorts
	•	mart_data_quality_scorecard

  Testing & Data Quality

This project uses dbt tests to validate core assumptions, including:
	•	Not null primary keys
	•	Uniqueness constraints
	•	Relationship integrity between facts and dimensions

A dedicated Data Quality page in Metabase helps stakeholders monitor trust signals.



