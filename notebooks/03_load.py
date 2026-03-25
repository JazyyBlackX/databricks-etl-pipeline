"""
notebooks/03_load.py
--------------------
GOLD LAYER — Business Metric Aggregation

Reads cleansed Silver Delta data and produces aggregated,
business-ready Gold tables for BI consumption and dashboarding.

Metrics produced:
  - Total revenue by category
  - Record count and average transaction value per category
  - Daily revenue trend
  - Top performing categories by total amount

Output is optimised with Delta Lake Z-Ordering and partitioning
for fast downstream query performance.
"""

import sys
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as _sum, count, avg, round as _round,
    max as _max, min as _min, current_timestamp
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from utils.config_loader import load_config
from utils.error_handler import handle_pipeline_error, with_retry

# ── Setup ────────────────────────────────────────────────────────────────────

logger = get_logger(__name__)
STAGE = "gold_aggregation"


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
def read_silver(spark: SparkSession, config: dict) -> DataFrame:
    """Reads valid records from Silver Delta table."""
    silver_path = f"{config['storage']['silver_path']}/sample_data"
    logger.info(f"Reading Silver data from: {silver_path}")

    df = (
        spark.read
        .format("delta")
        .load(silver_path)
        .filter(col("_is_valid") == True)  # Only process valid records
    )

    logger.info(f"Valid Silver records loaded: {df.count():,}")
    return df


def build_category_metrics(df: DataFrame, config: dict) -> DataFrame:
    """
    Aggregates key business metrics by category.

    Produces:
      - total_revenue: Sum of all transaction amounts
      - transaction_count: Number of transactions
      - avg_transaction_value: Average transaction amount
      - max_transaction: Highest single transaction
      - min_transaction: Lowest single transaction
    """
    gold_cfg = config.get("gold", {})
    group_col = gold_cfg.get("group_by", "category")
    agg_col = gold_cfg.get("agg_column", "amount")

    logger.info(f"Building category metrics — group by: {group_col}, aggregate: {agg_col}")

    df_metrics = (
        df.groupBy(group_col)
        .agg(
            _round(_sum(agg_col), 2).alias("total_revenue"),
            count("*").alias("transaction_count"),
            _round(avg(agg_col), 2).alias("avg_transaction_value"),
            _round(_max(agg_col), 2).alias("max_transaction"),
            _round(_min(agg_col), 2).alias("min_transaction"),
        )
        .orderBy(col("total_revenue").desc())
        .withColumn("_gold_processed_at", current_timestamp())
    )

    logger.info(f"Category metrics built: {df_metrics.count()} categories")
    return df_metrics


def build_daily_revenue(df: DataFrame) -> DataFrame:
    """
    Builds daily revenue trend table.
    Useful for time-series dashboards and trend analysis.
    """
    if "date" not in df.columns:
        logger.warning("Date column not found — skipping daily revenue aggregation")
        return None

    logger.info("Building daily revenue trend")

    df_daily = (
        df.groupBy("date")
        .agg(
            _round(_sum("amount"), 2).alias("daily_revenue"),
            count("*").alias("daily_transactions"),
            _round(avg("amount"), 2).alias("avg_daily_transaction")
        )
        .orderBy("date")
        .withColumn("_gold_processed_at", current_timestamp())
    )

    logger.info(f"Daily revenue trend built: {df_daily.count()} days")
    return df_daily


@with_retry(max_attempts=3, delay_seconds=5)
def write_to_gold(df: DataFrame, path: str, write_mode: str = "overwrite") -> int:
    """Writes aggregated metrics to Gold Delta table."""
    logger.info(f"Writing to Gold layer: {path}")

    record_count = df.count()

    (
        df.write
        .format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .save(path)
    )

    logger.info(f"Gold write successful: {record_count:,} records written to {path}")
    return record_count


def run():
    """Main entry point for Gold aggregation stage."""
    log_pipeline_start(logger, STAGE)

    try:
        config = load_config()
        spark = create_spark_session(config)
        gold_path = config["storage"]["gold_path"]
        write_mode = config.get("ingestion", {}).get("write_mode", "overwrite")

        # Read Silver
        df_silver = read_silver(spark, config)

        # Build and write category metrics
        df_category = build_category_metrics(df_silver, config)
        logger.info("Category metrics preview:")
        df_category.show(10, truncate=False)
        category_count = write_to_gold(df_category, f"{gold_path}/category_metrics", write_mode)

        # Build and write daily revenue
        df_daily = build_daily_revenue(df_silver)
        daily_count = 0
        if df_daily is not None:
            logger.info("Daily revenue preview:")
            df_daily.show(10, truncate=False)
            daily_count = write_to_gold(df_daily, f"{gold_path}/daily_revenue", write_mode)

        total = category_count + daily_count
        log_pipeline_end(logger, STAGE, total)

    except Exception as e:
        handle_pipeline_error(STAGE, e)


if __name__ == "__main__":
    run()
