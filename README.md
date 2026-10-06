# InvaSight — Multi-Cloud Automated Investment Data Pipeline

![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-3.0-017CEE?logo=Apache%20Airflow&logoColor=white)
![dbt Core](https://img.shields.io/badge/dbt-Core-FF694B?logo=dbt&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8?logo=snowflake&logoColor=white)
![AWS Secrets Manager](https://img.shields.io/badge/AWS-Secrets_Manager-232F3E?logo=amazon-aws&logoColor=white)
![Azure ADLS](https://img.shields.io/badge/Azure-Blob_Storage-0089D6?logo=microsoft-azure&logoColor=white)
![License](https://img.shields.io/badge/Status-Completed_Capstone-success)

**InvaSight** is an enterprise-grade investment data pipeline built for **wealth managers**. It automatically ingests multi-asset financial transaction ledgers and live global market data, standardizing all holdings and valuations into **Saudi Riyals (SAR)** within a consolidated analytical warehouse.

> 🎓 Built as a four-person capstone project for the **Saudi Digital Academy (SDA)** Data Engineering Bootcamp (*SDA × WeCloudData*).

---

## 🏗 System Architecture

![InvaSight architecture diagram](docs/images/architecture.png)

---

## ⚙️ How It Works

1. **Ingest:** **Apache Airflow** orchestrates scheduled ingestion. It fetches the internal client ledger **every 45 minutes** and pulls FX rates, precious metal prices, and equity data **daily**, enforcing payload schema validation.
2. **Land & Load:** Raw files land in **Azure Blob Storage** (or local landing storage). Runs are strictly idempotent—only new payloads for the current batch run are loaded into the warehouse, ensuring safe execution retries.
3. **Transform & Test:** **dbt Core** parses, types, and models raw entities into a **Galaxy Schema** denominated in SAR. Automated data quality tests run instantly upon modeling; any assertion failure halts downstream deployment.
4. **Serve:** A **Power BI** executive dashboard connects directly to Snowflake analytics views for real-time wealth oversight and asset allocation metrics.

---

## ✨ Key Features

- **Automated Airflow DAGs:** Independent, resilient DAG schedules for high-frequency internal ledgers and daily external market data APIs, complete with automated retry logic.
- **Single Currency Standard (SAR):** Automated currency conversion applying the exact FX rate active on or preceding each transaction date.
- **Strict Data Governance:** 13 dbt transformation models backed by **79 automated dbt data tests** verifying primary keys, referential integrity, accepted value bounds, and business logic constraints.
- **Zero-Cloud Local Mode:** Fully containerized setup allows end-to-end execution on sample data using **DuckDB** without needing cloud accounts.

---

## 📊 Analytics Data Model

The analytical layer (`analytics`) is structured as a **Galaxy Schema** consisting of three central fact tables supported by shared dimensional views:

* **Fact Tables:** `fact_holdings`, `fact_market_prices`, `fact_daily_portfolio_summary`
* **Dimension Tables:** `dim_asset`, `dim_client`, `dim_currency`, `dim_date`, `dim_source`

---

## 🛠 Tech Stack & Environment Specs

| Architectural Layer | Cloud / Production Deployment | Local Standalone Mode (Default) |
| :--- | :--- | :--- |
| **Orchestration** | Apache Airflow 3 (AWS EC2 / Docker / K8s) | Apache Airflow 3 (Docker Compose) |
| **Landing Storage** | Azure Blob Storage (ADLS Gen2) | `data/landing/` local filesystem |
| **Data Warehouse** | Snowflake | DuckDB (`data/warehouse/invasight.duckdb`) |
| **Data Transformation** | dbt Core + `dbt_utils` | dbt Core (`dbt-duckdb`) |
| **Market Data APIs** | ExchangeRatesAPI, MetalPriceAPI, Alpha Vantage | Sample local mock files / APIs |
| **BI & Analytics** | Power BI Desktop & Service | Power BI Dashboards / Query CLI |
| **CI/CD & Security** | GitHub Actions, AWS Secrets Manager | GitHub Actions |

---

## 🚀 Getting Started (Local Development)

### Prerequisites
* [Docker Desktop](https://www.docker.com/) or Docker Engine with `compose` v2.

### Running with Docker (Recommended)

1. **Clone the repository and prepare environment variables:**
   ```bash
   cp .env.example .env
   ```

2. **Spin up local Airflow and DuckDB infrastructure:**
   ```bash
   docker compose up -d --build
   ```

3. **Trigger Pipelines:**
   Open `http://localhost:8080` in your browser, unpause, and trigger both `daily_market_data_dag` and `internal_ledger_dag`.

4. **Query Analytical Output:**
   Stop the Airflow containers (to release DuckDB database write locks) and query directly:
   ```bash
   docker compose stop
   duckdb data/warehouse/invasight.duckdb "SELECT * FROM analytics.fact_daily_portfolio_summary LIMIT 10;"
   docker compose down
   ```

---

### Running Without Docker

```bash
# Initialize Python Virtual Environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install Dependencies & Execute Local Orchestration
pip install -r requirements-dev.txt
python scripts/run_local.py
```

---

### Switching to Cloud Production Mode

To connect to live cloud services:
1. Execute `warehouse/snowflake/setup.sql` in your Snowflake instance.
2. Set `PIPELINE_MODE=cloud` and `MARKET_DATA_SOURCE=api` in `.env`.
3. Fill in your AWS, Azure ADLS, and Snowflake service credentials.

---

## 🙋‍♀️ Individual Contributions & Leadership

As **Project Lead** and **Data Platform Architect**, I led the technical strategy, multi-cloud deployment, and DevSecOps security stance:

* **Project Management & Cost Optimization:** Authored project proposals and milestone plans. Conducted API quota/cost analysis to design a zero-cost pipeline entirely within free tier limits.
* **Multi-Cloud Infrastructure:** Architected the pipeline across AWS, Azure ADLS, and Snowflake. Successfully resolved cross-OS compatibility issues during deployment and deployed Airflow/dbt workloads on Kubernetes on AWS EC2.
* **DevSecOps & Incident Response:** Led containment during an isolated server security event—revoked exposed credentials, migrated environment variables into **AWS Secrets Manager**, implemented short-lived **Azure SAS tokens**, and established strict `.gitignore` patterns.
* **Transformation & Data Quality:** Configured dbt connections, engineered custom SQL macros, authored FX valuation models into SAR, and created comprehensive data quality test suites.

---

## 🤝 Project Team & Credits

InvaSight was developed as a capstone collaboration by a 4-person team:
* **Maryam Alotaibi** (*Project Lead & Data Platform Architect*)
* **Rawan**
* **Hanoof**
* **Fayha'a**

*Project Status: Completed.*
