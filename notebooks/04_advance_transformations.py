"""
notebooks/04_advance_transformations.py
---------------------------------------
SILVER → GOLD — Advanced Transformations

Applies complex analytical transformations on Silver data:
  - Window functions for ranking and running totals
  - Sessionisation and user-level aggregations
  - Revenue share calculations per category
  - Rolling window metrics

These enriched datasets feed advanced dashboards and
ML feature engineering pipelines.
"""

import sys
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, row_number, rank, dense_rank,
    sum as _sum, avg, count, round as _round,
    lag, lead, current_timestamp, lit,
    percent_rank, ntile
)
from pyspark.sql.window import Window

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from utils.config_loader import load_config
from utils.error_handler import handle_pipeline_error, with_retry

# ── Setup ────────────────────────────────────────────────────────────────────

logger = get_logger(__name__)
STAGE = "advanced_transformations"


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
        .filter(col("_is_valid") == True)
    )
    logger.info(f"Records loaded: {df.count():,}")
    return df


def apply_user_ranking(df: DataFrame) -> DataFrame:
    """
    Ranks users by total spend within each category.
    Uses RANK, DENSE_RANK, and PERCENT_RANK window functions.
    """
    logger.info("Applying user ranking by category spend")

    if "user_id" not in df.columns or "amount" not in df.columns:
        logger.warning("Required columns missing for user ranking — skipping")
        return df

    # Aggregate spend per user per category
    df_spend = (
        df.groupBy("user_id", "category")
        .agg(_round(_sum("amount"), 2).alias("total_spend"))
    )

    # Window spec partitioned by category, ordered by spend descending
    window_cat = Window.partitionBy("category").orderBy(col("total_spend").desc())

    df_ranked = (
        df_spend
        .withColumn("spend_rank", rank().over(window_cat))
        .withColumn("spend_dense_rank", dense_rank().over(window_cat))
        .withColumn("spend_percentile", _round(percent_rank().over(window_cat), 4))
        .withColumn("spend_quartile", ntile(4).over(window_cat))
        .withColumn("_processed_at", current_timestamp())
    )

    logger.info(f"User ranking complete: {df_ranked.count():,} records")
    return df_ranked


def apply_running_totals(df: DataFrame) -> DataFrame:
    """
    Computes running totals and lag/lead features per user.
    Useful for trend detection and sequential pattern analysis.
    """
    logger.info("Computing running totals and lag features")

    required = {"user_id", "timestamp", "amount"}
    if not required.issubset(set(df.columns)):
        logger.warning(f"Required columns missing: {required - set(df.columns)} — skipping")
        return df

    window_user_time = (
        Window.partitionBy("user_id")
        .orderBy("timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    window_user_lag = Window.partitionBy("user_id").orderBy("timestamp")

    df_running = (
        df
        .withColumn("running_total", _round(_sum("amount").over(window_user_time), 2))
        .withColumn("prev_transaction", _round(lag("amount", 1).over(window_user_lag), 2))
        .withColumn("next_transaction", _round(lead("amount", 1).over(window_user_lag), 2))
        .withColumn("transaction_seq", row_number().over(window_user_lag))
        .withColumn("_processed_at", current_timestamp())
    )

    logger.info(f"Running totals complete: {df_running.count():,} records")
    return df_running


def apply_category_revenue_share(df: DataFrame) -> DataFrame:
    """
    Calculates each category's percentage share of total revenue.
    Enables contribution analysis for business reporting.
    """
    logger.info("Computing category revenue share")

    if "category" not in df.columns or "amount" not in df.columns:
        logger.warning("Required columns missing for revenue share — skipping")
        return df

    # Total revenue across all categories
    total_revenue = df.agg(_sum("amount").alias("total")).collect()[0]["total"]
    logger.info(f"Total revenue base: {total_revenue:,.2f}")

    df_share = (
        df.groupBy("category")
        .agg(
            _round(_sum("amount"), 2).alias("category_revenue"),
            count("*").alias("transaction_count")
        )
        .withColumn(
            "revenue_share_pct",
            _round((col("category_revenue") / lit(total_revenue)) * 100, 2)
        )
        .orderBy(col("revenue_share_pct").desc())
        .withColumn("_processed_at", current_timestamp())
    )

    logger.info(f"Revenue share computed: {df_share.count()} categories")
    return df_share


@with_retry(max_attempts=3, delay_seconds=5)
def write_to_gold(df: DataFrame, path: str, write_mode: str = "overwrite") -> int:
    """Writes advanced metric table to Gold Delta layer."""
    logger.info(f"Writing advanced metrics to: {path}")
    record_count = df.count()
    (
        df.write
        .format("delta")
        .mode(write_mode)
        .option("overwriteSchema", "true")
        .save(path)
    )
    logger.info(f"Write successful: {record_count:,} records")
    return record_count


def run():
    """Main entry point for advanced transformations."""
    log_pipeline_start(logger, STAGE)

    try:
        config = load_config()
        spark = create_spark_session(config)
        gold_path = config["storage"]["gold_path"]
        write_mode = config.get("ingestion", {}).get("write_mode", "overwrite")

        df_silver = read_silver(spark, config)

        # User ranking
        df_ranked = apply_user_ranking(df_silver)
        logger.info("User ranking preview:")
        df_ranked.show(5, truncate=False)
        write_to_gold(df_ranked, f"{gold_path}/user_rankings", write_mode)

        # Running totals
        df_running = apply_running_totals(df_silver)
        logger.info("Running totals preview:")
        df_running.show(5, truncate=False)
        write_to_gold(df_running, f"{gold_path}/user_running_totals", write_mode)

        # Revenue share
        df_share = apply_category_revenue_share(df_silver)
        logger.info("Revenue share preview:")
        df_share.show(truncate=False)
        total = write_to_gold(df_share, f"{gold_path}/category_revenue_share", write_mode)

        log_pipeline_end(logger, STAGE, total)

    except Exception as e:
        handle_pipeline_error(STAGE, e)


if __name__ == "__main__":
    run()
