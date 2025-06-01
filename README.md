# Databricks ETL Pipeline – Medallion Architecture

This project demonstrates a robust end-to-end ETL pipeline using PySpark and Delta Lake on Databricks, following the Medallion Architecture (Bronze, Silver, Gold layers). It is designed to showcase best practices in data ingestion, processing, and analytics automation.

## Tech Stack

- **Databricks Notebooks**
- **PySpark & Delta Lake**
- **Azure Data Factory** (optional for orchestration)
- **Azure Data Lake Gen2**

## Architecture Overview


- **Bronze**: Raw data ingestion from various sources (CSV, JSON, Parquet, etc).
- **Silver**: Cleansed and transformed data with validation and deduplication.
- **Gold**: Aggregated data ready for analytics and BI reporting.

## Features

- Automated ETL with modular notebooks
- Scalable, cloud-native processing
- Delta Lake ACID transactions and schema enforcement
- Parameterized notebook execution for reusability
- Optional orchestration with Azure Data Factory or Databricks Workflows
- Logging and error handling

## How to Run

1. Clone the repository and import notebooks into your Databricks workspace.
2. Update configuration parameters for your storage and environment.
3. Run each notebook in order:
   - `01_bronze_ingestion.ipynb`
   - `02_silver_transform.ipynb`
   - `03_gold_aggregation.ipynb`
4. (Optional) Set up Databricks Workflows or Azure Data Factory pipelines for automation.

## Folder Structure

```text
notebooks/
  01_bronze_ingestion.ipynb
  02_silver_transform.ipynb
  03_gold_aggregation.ipynb
configs/
docs/
  medallion_architecture.png
```


## Requirements

- Databricks workspace (Community or Enterprise)
- Azure subscription (for Data Lake and ADF)
- Python 3.8+ (for local testing, optional)
- PySpark, Delta Lake libraries


---

## About the Author

👤 Avnish Sharma 
Data Engineer | ETL Automation | Cloud Analytics
