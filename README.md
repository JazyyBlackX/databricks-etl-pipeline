# Databricks ETL Pipeline – Medallion Architecture

This project demonstrates a full ETL pipeline using PySpark and Delta Lake in Databricks.

## Tech Stack
- Databricks Notebooks
- PySpark & Delta Lake
- Azure Data Factory (optional)
- Azure Data Lake Gen2

## Layers
- Bronze: Raw ingestion
- Silver: Cleansed/transformed data
- Gold: Aggregated/reporting layer

## How to Run
Use Databricks notebooks to run each stage manually or schedule using Workflows.
