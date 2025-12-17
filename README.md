
# Databricks YFinance Lakehouse

## 📌 Project Purpose
This project implements the **Medallion Lakehouse architecture** (Bronze → Silver → Gold) in the **Databricks environment** using **Delta Lake** to process historical financial data from the Warsaw Stock Exchange (WSE). Data is retrieved via the `yfinance` library.

The goal is to provide a clean, optimized, and query-ready dataset for analytics and reporting.

---

## 🏗 Architecture
- **Bronze**: Raw data ingested from `yfinance` (all GPW tickers).
- **Silver**: Cleaned and validated data with enforced schema.
- **Gold**: Aggregated and business-ready tables for analytics.

> Planned additions: Data Quality checks (freshness, schema validation) and CI integration.


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

---

## ✅ Current Features
- Medallion architecture (Bronze → Silver → Gold).
- Delta Lake tables for ACID transactions and time travel.
- GitHub CI (planned: lint, tests, Data Quality report).

---

## 🔮 Future Enhancements
- Data Quality checks (schema, freshness, duplicates).
- Monitoring and pipeline health metrics.



