
# Databricks Financial Lakehouse (WSE Analysis)
End-to-End Medallion Pipeline with Automated Orchestration & Quality Gates

## 📌 Project Purpose
This project implements a complete Medallion Architecture (Bronze → Silver → Gold) within Databricks to process and analyze Warsaw Stock Exchange (WSE) data.

What makes this project unique:

- Automated Metadata Management: Syncs ticker lists from Google Drive to Databricks SQL.
- Hybrid Orchestration: Managed via Databricks Workflows with cross-task dependencies.
- Professional Testing Framework: Modular validation logic stored in .py modules and imported into Spark notebooks – simulating production-grade CI/CD patterns.

---

##🏗 Architecture & Orchestration

#### 1. Data Ingestion & Metadata Sync
- External Ingestion: A local Selenium-based scraper updates a Master Ticker List on Google Drive.
- 00_METADATA_SYNC: This notebook acts as the Control Plane. It fetches the CSV, validates its integrity via tests/meta_tests.py, and synchronizes the Databricks SQL metadata table.

#### 2. The Medallion Pipeline
- Bronze (Raw): Incremental ingestion from yfinance API. Uses Delta Merge to ensure idempotency.
- Silver (Refined): Schema enforcement, type optimization, and Feature Engineering (SMA 20/50/200, Daily Returns).
- Gold (Curated): Business-ready aggregations and monthly performance metrics.

#### 3. Workflow Orchestration
The entire pipeline is orchestrated using Databricks Workflows. The graph ensures that data only flows to the next layer if the previous one succeeded and passed its Quality Gates.


##🛡️ Reliability & Testing (CI/CD Approach)
Unlike simple scripts, this project uses a Modular Testing Framework:

- Decoupled Validation: Quality checks are stored in the /tests directory as Python modules.
- Quality Gates: Every layer (Meta, Bronze, Silver, Gold) imports these modules to perform Run-time Data Validation.
- CI-Ready Design: The separation of logic into .py files allows for easy integration with GitLab CI/CD / GitHub Actions for automated Unit Testing using mock data.
- Automated Logging: A custom logging framework records STARTED / SUCCESS / FAILED statuses into Delta Tables for full auditability.

## ⚙️ Tech Stack
- Platform: Databricks (Free Community Edition)
- Engine: Apache Spark (PySpark)
- Storage: Delta Lake (Lakehouse)
- Orchestration: Databricks Workflows (DAG)
- Language: Python (Modular OOP approach)
- Version Control: Git / GitHub / Databricks Repos

---

## 📂 Data Scope
- **Tickers**: All companies listed on GPW.
- **Date Range**: From the company's first listing on WSE to the current date.

---

## 🚀 How to Run
1. Local Setup: Run scrapers/gdrive_scraper.py to refresh the ticker list on your Google Drive.

2. Databricks Setup: - Connect your GitHub repo to Databricks Repos.
- Configure config/config.yaml for your environment.

3. Run Pipeline:
`- Execute the Databricks Workflow (Job) which automates: 00_METADATA_SYNC ➔ 01_BRONZE_INGEST ➔ 02_SILVER_TRANSFORM ➔ 03_GOLD_AGGREGATE.`



##📊 Sample Monitoring Query
You can monitor the health of your pipeline using SQL directly in Databricks:

SQL

SELECT timestamp, task_name, status, message 
FROM delta.`/Volumes/{catalog}/{schema}/{volume}/gold_execution_logs`
ORDER BY timestamp DESC
LIMIT 20;
