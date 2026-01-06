
# Databricks YFinance Lakehouse (WSE Market Analysis)

## 📌 Project Purpose
This project implements a Medallion Lakehouse architecture (Bronze → Silver → Gold) in Databricks using Delta Lake to process historical financial data from the Warsaw Stock Exchange (WSE).

Unlike standard ETL pipelines, this project includes a Quality Gate System designed to detect and handle financial data anomalies (e.g., unadjusted stock splits/resplits) common in providers like Yahoo Finance.

---

##🏗 Architecture & Data Flow

####Bronze (Raw):

Raw ingestion from yfinance API.

Format: Delta Lake (Append only).


####Silver (Refined):

Schema enforcement and type optimization (Float/Integer).

Feature Engineering: SMA (20, 50, 200), Daily Returns, Time Dimensions.

Quality Gate: Intelligent SMA check (handles new IPOs) and anomaly detection (>1000% return alerts).

####Gold (Curated):

Business-ready Monthly Aggregations.

Outlier Filtering: Automatic removal of technical data errors (e.g., Atlantis SA resplit errors).

Delta Constraints: Enforced month ranges and non-null tickets.

##🛡️ Data Quality & Monitoring (Latest Features)
The project features a custom logging and testing framework:

Execution Logs: Every run (STARTED/SUCCESS/FAILED) is logged into a dedicated Delta table with detailed error messages.

Anomaly Reporting: Technical data glitches (like the 2700% jump in Atlantis SA) are detected in the Silver layer and logged as warnings instead of breaking the pipeline.

Maintenance: Automated OPTIMIZE (Z-ORDER) and VACUUM processes for storage performance and cost-efficiency in Databricks.
## ⚙️ Technologies
- Databricks Free Edition
- Delta Lake
- PySpark
- yfinance
- GitHub Actions (CI)

---

## 📂 Data Scope
- **Tickers**: All companies listed on GPW.
- **Date Range**: From the company's first listing on WSE to the current date.

---

## 🚀 How to Run
1. Clone this repository.
2. Import notebooks into **Databricks**.
3. Configure paths in `config/` for Bronze, Silver, and Gold layers.
4. Run notebooks in the following order:
   - `notebooks/bronze_ingest`
   - `notebooks/silver_transform`
   - `notebooks/gold_aggregate`

> **Note:** This project is designed for **Databricks Free Edition**. No automated CD (Continuous Deployment) is included due to platform limitations.

##📊 Sample Monitoring Query
You can monitor the health of your pipeline using SQL directly in Databricks:

SQL

SELECT * 
FROM delta.`/Volumes/workspace/test_schema/yfinance_test_data/gold_execution_logs`

//change paths//




