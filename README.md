# Databricks ETL Pipeline — Medallion Architecture

![Databricks](https://img.shields.io/badge/Databricks-PySpark-FF3621?style=flat&logo=databricks)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-00ADD4?style=flat)
![Azure](https://img.shields.io/badge/Azure-Data%20Lake%20Gen2-0078D4?style=flat&logo=microsoftazure)
![Architecture](https://img.shields.io/badge/Architecture-Medallion-brightgreen?style=flat)
![Status](https://img.shields.io/badge/Status-Active-success?style=flat)

A production-grade, end-to-end batch ETL pipeline built with **PySpark and Delta Lake on Databricks**, following the **Medallion Architecture (Bronze → Silver → Gold)**. Designed to simulate real-world enterprise lakehouse patterns used in finance and analytics — including schema enforcement, ACID compliance, deduplication, and modular parameterised notebook execution.

---

## Highlights

- 🏅 Full **Medallion Architecture** implemented across three Delta Lake layers
- ⚡ Handles **multi-format ingestion** — CSV, JSON, and Parquet from Azure Data Lake Gen2
- 🔍 Silver layer enforces **schema validation, deduplication, and data quality checks**
- 📊 Gold layer produces **aggregated business metrics** ready for BI and dashboarding
- 🔁 **Parameterised notebooks** for reusable, environment-agnostic execution
- 🛡️ **ACID-compliant Delta tables** with schema enforcement and error logging
- ⏰ Supports dual orchestration — **Databricks Workflows** or **Azure Data Factory**

---

## Tech Stack

| Component | Technology |
|---|---|
| Compute | Azure Databricks |
| Language | Python / PySpark |
| Table Format | Delta Lake |
| Storage | Azure Data Lake Gen2 |
| Orchestration | Databricks Workflows / Azure Data Factory (optional) |
| Data Formats | CSV, JSON, Parquet |

---

## Architecture

```
Raw Sources (CSV / JSON / Parquet)
              │
              ▼
┌─────────────────────────┐
│      BRONZE LAYER       │  ← Raw ingestion, no transformation
│  Delta Lake (append)    │     Full audit trail preserved
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│      SILVER LAYER       │  ← Schema enforcement, deduplication,
│  Delta Lake (curated)   │     validation, incremental tracking
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│       GOLD LAYER        │  ← Aggregated KPIs, business metrics
│  Delta Lake (serving)   │     Ready for BI & downstream consumers
└─────────────────────────┘
            │
            ▼
   BI Dashboards / Reports / ML Models
```

---

## Project Structure

```
databricks-etl-pipeline/
│
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb       # Raw data ingestion from ADLS Gen2
│   ├── 02_silver_transform.ipynb       # Cleanse, validate, deduplicate
│   └── 03_gold_aggregation.ipynb       # Aggregate metrics for analytics
│
├── configs/
│   └── pipeline_config.json            # Parameterised environment config
│
├── docs/
│   └── medallion_architecture.png      # Architecture reference diagram
│
└── README.md
```

---

## Pipeline Details

### 🥉 Bronze — Raw Ingestion
- Ingests raw files (CSV, JSON, Parquet) from Azure Data Lake Gen2
- Writes data as-is into Delta tables — no transformation at this stage
- Preserves full audit trail, enabling reprocessing and time travel
- Supports append-only incremental loads

### 🥈 Silver — Transformation & Quality
- Schema normalisation and data type casting
- Deduplication based on configurable primary key columns
- Data quality validation with structured error logging for failed records
- Incremental load tracking via high-water mark pattern — avoids reprocessing historical data
- Outputs a clean, trusted dataset for downstream consumption

### 🥇 Gold — Aggregated Business Metrics
- Aggregates cleansed Silver data into business-ready reporting tables
- Designed for direct consumption by BI tools and dashboards
- Metrics include revenue summaries, performance KPIs, and trend analysis
- Optimised with Delta Lake Z-Ordering and partitioning for fast query performance

---

## How to Run

### Prerequisites
- Databricks workspace (Community Edition or Enterprise)
- Azure Data Lake Gen2 storage account
- Databricks Runtime 11.3+ with Delta Lake pre-installed

### Steps

1. **Clone the repository**
```bash
git clone https://github.com/JazyyBlackX/databricks-etl-pipeline.git
```

2. **Import notebooks** into your Databricks workspace:
   - Go to Databricks → Workspace → Import
   - Upload notebooks from the `notebooks/` folder

3. **Configure the pipeline** — update `configs/pipeline_config.json`:
```json
{
  "storage_account": "<your-adls-account>",
  "container": "<your-container>",
  "bronze_path": "bronze/",
  "silver_path": "silver/",
  "gold_path": "gold/"
}
```

4. **Run notebooks in order**:
```
01_bronze_ingestion.ipynb
02_silver_transform.ipynb
03_gold_aggregation.ipynb
```

5. **(Optional) Set up orchestration**:
   - **Databricks Workflows**: Create a multi-task job linking the three notebooks in sequence
   - **Azure Data Factory**: Use the ADF Databricks Notebook activity, chain tasks with dependencies

---

## Key Engineering Decisions

- **Delta Lake over plain Parquet** — ACID compliance, schema enforcement, and time travel without additional infrastructure
- **Parameterised notebooks** — all environment-specific values (paths, configs) are externalised, making the pipeline portable across dev/staging/prod
- **Incremental load pattern** — high-water mark tracking in Silver layer processes only new records, keeping compute costs efficient at scale
- **Modular design** — each layer is independently runnable and testable; failures in one layer don't cascade
- **Dual orchestration support** — works with both Databricks Workflows and ADF, giving flexibility depending on the Azure stack in use

---

## Author

**Avnish Sharma** — Senior Data Engineer  
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat&logo=linkedin)](https://linkedin.com/in/avnish-sharma-254103144)
[![GitHub](https://img.shields.io/badge/GitHub-JazyyBlackX-181717?style=flat&logo=github)](https://github.com/JazyyBlackX)
