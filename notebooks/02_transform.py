"""
notebooks/02_transform.py
-------------------------
SILVER LAYER — Data Cleansing & Transformation

Reads raw Bronze Delta data and applies:
  - Schema normalisation and type casting
  - Null handling and critical column validation
  - Deduplication using high-water mark pattern
  - Date/timestamp standardisation
  - Incremental load tracking

Output is a clean, trusted Silver Delta table ready for
advanced transformations and business logic.
"""

import sys
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_date, to_timestamp, trim, upper,
    when, lit, current_timestamp, row_number
)
from pyspark.sql.window import Window

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from utils.config_loader import load_config
from utils.error_handler import handle_pipeline_error, with_retry

# ── Setup ────────────────────────────────────────────────────────────────────

logger = get_logger(__name__)
STAGE = "silver_transformation"


def create_spark_session(config: dict) -> SparkSession:
    spark_cfg = config.get("spark", {})
    spark = (
        SparkSession.builder
        .appName(spark_cfg.get("app_name", "DatabricksETLPipeline"))
        .config("spark.sql.shuffle.partitions", spark_cfg.get("shuffle_partitions", "200"))
        .config("spark.sql.adaptive.enabled", spark_cfg.get("adaptive_enabled", "true"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


@with_retry(max_attempts=3, delay_seconds=5)
def read_bronze(spark: SparkSession, config: dict) -> DataFrame:
    """Reads data from Bronze Delta table."""
    bronze_path = f"{config['storage']['bronze_path']}/sample_data"
    logger.info(f"Reading Bronze data from: {bronze_path}")
    df = spark.read.format("delta").load(bronze_path)
    logger.info(f"Bronze records loaded: {df.count():,}")
    return df


def validate_critical_columns(df: DataFrame, config: dict) -> DataFrame:
    """
    Validates critical columns are not null.
    Flags invalid records rather than dropping — preserves data lineage.
    """
    critical_cols = config.get("data_quality", {}).get(
        "critical_columns", ["user_id", "timestamp", "amount", "category"]
    )

    logger.info(f"Validating critical columns: {critical_cols}")

    # Build null check condition
    null_condition = None
    for c in critical_cols:
        if c in df.columns:
            check = col(c).isNull()
            null_condition = check if null_condition is None else null_condition | check

    if null_condition is not None:
        invalid_count = df.filter(null_condition).count()
        logger.warning(f"Records with null critical columns: {invalid_count:,}")
        df = df.withColumn(
            "_is_valid",
            when(null_condition, lit(False)).otherwise(lit(True))
        )
    else:
        df = df.withColumn("_is_valid", lit(True))

    return df


def normalise_schema(df: DataFrame, config: dict) -> DataFrame:
    """
    Applies schema normalisation:
    - Casts timestamp and date columns
    - Trims string whitespace
    - Standardises categorical columns
    """
    transform_cfg = config.get("transformation", {})
    ts_col = transform_cfg.get("timestamp_column", "timestamp")
    date_col = transform_cfg.get("date_column", "date")

    logger.info("Applying schema normalisation")

    # Cast timestamp
    if ts_col in df.columns:
        df = df.withColumn(ts_col, to_timestamp(col(ts_col)))

    # Derive date from timestamp if date column missing
    if date_col not in df.columns and ts_col in df.columns:
        df = df.withColumn(date_col, to_date(col(ts_col)))

    # Trim string columns
    string_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType()"]
    for c in string_cols:
        df = df.withColumn(c, trim(col(c)))

    # Standardise category to uppercase
    if "category" in df.columns:
        df = df.withColumn("category", upper(trim(col("category"))))

    # Cast amount to double if exists
    if "amount" in df.columns:
        df = df.withColumn("amount", col("amount").cast("double"))

    logger.info("Schema normalisation complete")
    return df


def deduplicate(df: DataFrame, config: dict) -> DataFrame:
    """
    Deduplicates records using high-water mark pattern.
    Keeps the latest record per dedup key based on timestamp ordering.
    """
    transform_cfg = config.get("transformation", {})
    dedup_key = transform_cfg.get("dedup_key", "user_id")
    order_col = transform_cfg.get("dedup_order_col", "timestamp")

    if dedup_key not in df.columns or order_col not in df.columns:
        logger.warning(
            f"Dedup columns not found ({dedup_key}, {order_col}). Skipping deduplication."
        )
        return df

    logger.info(f"Deduplicating on key: {dedup_key}, ordered by: {order_col}")

    before_count = df.count()

    window_spec = Window.partitionBy(dedup_key).orderBy(col(order_col).desc())
    df_dedup = (
        df.withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    after_count = df_dedup.count()
    logger.info(f"Deduplication: {before_count:,} → {after_count:,} records "
                f"({before_count - after_count:,} duplicates removed)")

    return df_dedup


def add_silver_metadata(df: DataFrame) -> DataFrame:
    """Adds Silver layer audit metadata."""
    return df.withColumn("_silver_processed_at", current_timestamp())


@with_retry(max_attempts=3, delay_seconds=5)
def write_to_silver(df: DataFrame, config: dict) -> int:
    """Writes cleansed data to Silver Delta table."""
    silver_path = f"{config['storage']['silver_path']}/sample_data"
    write_mode = config.get("ingestion", {}).get("write_mode", "overwrite")

    logger.info(f"Writing to Silver layer: {silver_path}")

    record_count = df.count()

    (
        df.write
        .format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .save(silver_path)
    )

    logger.info(f"Silver write successful: {record_count:,} records written")
    return record_count


def run():
    """Main entry point for Silver transformation stage."""
    log_pipeline_start(logger, STAGE)

    try:
        config = load_config()
        spark = create_spark_session(config)

        # Read Bronze
        df = read_bronze(spark, config)

        # Validate critical columns
        df = validate_critical_columns(df, config)

        # Normalise schema
        df = normalise_schema(df, config)

        # Deduplicate
        df = deduplicate(df, config)

        # Add metadata
        df = add_silver_metadata(df)

        # Write to Silver
        record_count = write_to_silver(df, config)

        log_pipeline_end(logger, STAGE, record_count)

    except Exception as e:
        handle_pipeline_error(STAGE, e)


if __name__ == "__main__":
    run()
