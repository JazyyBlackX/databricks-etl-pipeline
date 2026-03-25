"""
notebooks/01_ingest.py
----------------------
BRONZE LAYER — Raw Data Ingestion

Ingests raw source files (CSV, JSON, Parquet) from ADLS Gen2 into
Delta Lake Bronze tables. No transformation applied at this stage —
raw data is preserved for full audit trail and reprocessing capability.

Supports:
  - Multi-format ingestion (CSV, JSON, Parquet)
  - Schema inference
  - Append and overwrite write modes
  - Incremental load tracking via ingestion timestamp
"""

import sys
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from utils.config_loader import load_config
from utils.error_handler import handle_pipeline_error, with_retry

# ── Setup ────────────────────────────────────────────────────────────────────

logger = get_logger(__name__)
STAGE = "bronze_ingestion"


def create_spark_session(config: dict) -> SparkSession:
    """Creates and configures a Spark session."""
    spark_cfg = config.get("spark", {})
    spark = (
        SparkSession.builder
        .appName(spark_cfg.get("app_name", "DatabricksETLPipeline"))
        .config("spark.sql.shuffle.partitions", spark_cfg.get("shuffle_partitions", "200"))
        .config("spark.sql.adaptive.enabled", spark_cfg.get("adaptive_enabled", "true"))
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark session created: {spark.version}")
    return spark


@with_retry(max_attempts=3, delay_seconds=5)
def read_source_data(spark: SparkSession, config: dict):
    """
    Reads raw source data from configured path.
    Supports CSV, JSON, and Parquet formats.
    """
    storage = config["storage"]
    ingest_cfg = config["ingestion"]

    source_format = ingest_cfg.get("source_format", "csv")
    source_path = f"{storage['raw_path']}/{ingest_cfg['source_file']}"

    logger.info(f"Reading source data from: {source_path}")
    logger.info(f"Format: {source_format}")

    if source_format == "csv":
        df = (
            spark.read
            .option("header", ingest_cfg.get("header", True))
            .option("inferSchema", ingest_cfg.get("infer_schema", True))
            .option("multiLine", True)
            .option("escape", '"')
            .csv(source_path)
        )
    elif source_format == "json":
        df = spark.read.option("multiLine", True).json(source_path)
    elif source_format == "parquet":
        df = spark.read.parquet(source_path)
    else:
        raise ValueError(f"Unsupported source format: {source_format}")

    logger.info(f"Schema inferred: {len(df.columns)} columns")
    return df


def enrich_with_metadata(df):
    """
    Adds ingestion metadata columns for audit trail.
    Enables full reprocessing and lineage tracking.
    """
    return (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_ingested_date", lit(datetime.utcnow().strftime("%Y-%m-%d")))
        .withColumn("_source_file", input_file_name())
    )


@with_retry(max_attempts=3, delay_seconds=5)
def write_to_bronze(df, config: dict) -> int:
    """
    Writes enriched data to Bronze Delta table.
    Returns record count for logging.
    """
    storage = config["storage"]
    ingest_cfg = config["ingestion"]

    bronze_path = f"{storage['bronze_path']}/sample_data"
    write_mode = ingest_cfg.get("write_mode", "overwrite")

    logger.info(f"Writing to Bronze layer: {bronze_path}")
    logger.info(f"Write mode: {write_mode}")

    record_count = df.count()

    (
        df.write
        .format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .save(bronze_path)
    )

    logger.info(f"Bronze write successful: {record_count:,} records written")
    return record_count


def run():
    """Main entry point for Bronze ingestion stage."""
    log_pipeline_start(logger, STAGE)

    try:
        config = load_config()
        spark = create_spark_session(config)

        # Read raw source data
        df_raw = read_source_data(spark, config)

        # Add audit metadata
        df_enriched = enrich_with_metadata(df_raw)

        # Preview schema
        logger.info("Bronze schema:")
        df_enriched.printSchema()

        # Write to Delta
        record_count = write_to_bronze(df_enriched, config)

        log_pipeline_end(logger, STAGE, record_count)

    except Exception as e:
        handle_pipeline_error(STAGE, e)


if __name__ == "__main__":
    run()
