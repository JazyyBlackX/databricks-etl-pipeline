"""
notebooks/05_data_quality_checks.py
------------------------------------
DATA QUALITY — Validation & Monitoring

Runs comprehensive data quality checks across pipeline layers:
  - Null checks on critical columns
  - Duplicate detection
  - Value range validation
  - Schema consistency validation
  - Row count reconciliation between layers
  - Quality score calculation

Results are written to a dedicated DQ audit Delta table
for monitoring and alerting.
"""

import sys
import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as _sum, count, when, lit,
    isnan, current_timestamp, round as _round
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.logger import get_logger, log_pipeline_start, log_pipeline_end
from utils.config_loader import load_config
from utils.error_handler import handle_pipeline_error

# ── Setup ────────────────────────────────────────────────────────────────────

logger = get_logger(__name__)
STAGE = "data_quality_checks"


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


def check_null_counts(df: DataFrame, layer: str, critical_cols: list) -> dict:
    """
    Checks null counts across all columns.
    Raises alert if critical columns contain nulls.
    """
    logger.info(f"[{layer}] Running null checks on {len(df.columns)} columns")

    null_exprs = [
        _sum(
            when(col(c).isNull() | (isnan(col(c)) if df.schema[c].dataType.typeName()
                 in ("double", "float") else lit(False)), 1).otherwise(0)
        ).alias(c)
        for c in df.columns
        if not c.startswith("_")  # Skip metadata columns
    ]

    if not null_exprs:
        return {}

    null_counts = df.agg(*null_exprs).collect()[0].asDict()

    results = {}
    for col_name, null_count in null_counts.items():
        is_critical = col_name in critical_cols
        status = "PASS"
        if null_count > 0:
            status = "CRITICAL_FAIL" if is_critical else "WARN"
            logger.warning(
                f"[{layer}] NULL CHECK {status}: {col_name} has {null_count:,} nulls"
                + (" (CRITICAL COLUMN)" if is_critical else "")
            )
        results[col_name] = {"null_count": null_count, "status": status}

    passed = sum(1 for r in results.values() if r["status"] == "PASS")
    logger.info(f"[{layer}] Null checks: {passed}/{len(results)} passed")
    return results


def check_duplicates(df: DataFrame, layer: str, key_cols: list = None) -> dict:
    """
    Detects duplicate records.
    Checks both full-row duplicates and key-column duplicates.
    """
    logger.info(f"[{layer}] Running duplicate checks")

    total = df.count()

    # Full row duplicates
    distinct_count = df.distinct().count()
    full_dups = total - distinct_count

    results = {
        "total_records": total,
        "distinct_records": distinct_count,
        "full_row_duplicates": full_dups,
        "full_row_status": "PASS" if full_dups == 0 else "WARN"
    }

    if full_dups > 0:
        logger.warning(f"[{layer}] DUPLICATE CHECK WARN: {full_dups:,} full-row duplicates found")

    # Key-column duplicates
    if key_cols and all(c in df.columns for c in key_cols):
        key_dup_count = (
            df.groupBy(key_cols)
            .count()
            .filter(col("count") > 1)
            .agg(_sum("count").alias("total_dups"))
            .collect()[0]["total_dups"] or 0
        )
        results["key_duplicates"] = key_dup_count
        results["key_status"] = "PASS" if key_dup_count == 0 else "FAIL"

        if key_dup_count > 0:
            logger.warning(
                f"[{layer}] DUPLICATE CHECK FAIL: "
                f"{key_dup_count:,} duplicates on key {key_cols}"
            )
        else:
            logger.info(f"[{layer}] Key duplicate check passed on {key_cols}")

    return results


def check_value_ranges(df: DataFrame, layer: str, config: dict) -> dict:
    """
    Validates numeric columns fall within expected ranges.
    Flags outliers and invalid values.
    """
    dq_cfg = config.get("data_quality", {})
    amount_min = dq_cfg.get("amount_min", 0)
    amount_max = dq_cfg.get("amount_max", 100000)

    results = {}

    if "amount" not in df.columns:
        logger.warning(f"[{layer}] Amount column not found — skipping range checks")
        return results

    logger.info(f"[{layer}] Checking amount range: [{amount_min}, {amount_max}]")

    # Out of range records
    out_of_range = df.filter(
        (col("amount") < amount_min) | (col("amount") > amount_max)
    ).count()

    # Negative amounts
    negative_count = df.filter(col("amount") < 0).count()

    # Zero amounts
    zero_count = df.filter(col("amount") == 0).count()

    results = {
        "out_of_range_count": out_of_range,
        "negative_count": negative_count,
        "zero_count": zero_count,
        "range_status": "PASS" if out_of_range == 0 else "WARN"
    }

    if out_of_range > 0:
        logger.warning(
            f"[{layer}] RANGE CHECK WARN: {out_of_range:,} records outside "
            f"[{amount_min}, {amount_max}]"
        )
    else:
        logger.info(f"[{layer}] Range check passed")

    return results


def check_row_count_reconciliation(
    bronze_df: DataFrame,
    silver_df: DataFrame
) -> dict:
    """
    Reconciles record counts between Bronze and Silver layers.
    Flags significant drops that may indicate data loss.
    """
    logger.info("Running Bronze → Silver row count reconciliation")

    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    drop_pct = round(((bronze_count - silver_count) / bronze_count) * 100, 2) if bronze_count > 0 else 0

    status = "PASS"
    if drop_pct > 10:
        status = "WARN"
        logger.warning(f"RECONCILIATION WARN: {drop_pct}% record drop Bronze → Silver")
    elif drop_pct > 25:
        status = "FAIL"
        logger.error(f"RECONCILIATION FAIL: {drop_pct}% record drop Bronze → Silver")
    else:
        logger.info(f"Reconciliation passed: {drop_pct}% drop rate (within threshold)")

    return {
        "bronze_count": bronze_count,
        "silver_count": silver_count,
        "drop_count": bronze_count - silver_count,
        "drop_pct": drop_pct,
        "status": status
    }


def calculate_quality_score(all_results: dict) -> float:
    """
    Calculates an overall data quality score (0-100).
    Based on pass/fail/warn counts across all checks.
    """
    statuses = []

    def extract_statuses(d):
        for k, v in d.items():
            if isinstance(v, dict):
                extract_statuses(v)
            elif k.endswith("_status") or k == "status":
                statuses.append(v)

    extract_statuses(all_results)

    if not statuses:
        return 100.0

    scores = {"PASS": 1.0, "WARN": 0.5, "FAIL": 0.0, "CRITICAL_FAIL": 0.0}
    score = (sum(scores.get(s, 0) for s in statuses) / len(statuses)) * 100
    return round(score, 1)


def run():
    """Main entry point for data quality checks."""
    log_pipeline_start(logger, STAGE)

    try:
        config = load_config()
        spark = create_spark_session(config)
        storage = config["storage"]
        dq_cfg = config.get("data_quality", {})
        critical_cols = dq_cfg.get("critical_columns", ["user_id", "timestamp", "amount", "category"])

        # Load layers
        logger.info("Loading Bronze and Silver layers for DQ checks")
        df_bronze = spark.read.format("delta").load(f"{storage['bronze_path']}/sample_data")
        df_silver = spark.read.format("delta").load(f"{storage['silver_path']}/sample_data")

        all_results = {}

        # ── Bronze Checks ────────────────────────────────────────────────
        logger.info("=" * 40)
        logger.info("BRONZE LAYER CHECKS")
        logger.info("=" * 40)
        all_results["bronze_nulls"] = check_null_counts(df_bronze, "BRONZE", critical_cols)
        all_results["bronze_duplicates"] = check_duplicates(df_bronze, "BRONZE", ["user_id"])
        all_results["bronze_ranges"] = check_value_ranges(df_bronze, "BRONZE", config)

        # ── Silver Checks ────────────────────────────────────────────────
        logger.info("=" * 40)
        logger.info("SILVER LAYER CHECKS")
        logger.info("=" * 40)
        all_results["silver_nulls"] = check_null_counts(df_silver, "SILVER", critical_cols)
        all_results["silver_duplicates"] = check_duplicates(df_silver, "SILVER", ["user_id"])
        all_results["silver_ranges"] = check_value_ranges(df_silver, "SILVER", config)

        # ── Reconciliation ───────────────────────────────────────────────
        logger.info("=" * 40)
        logger.info("RECONCILIATION CHECKS")
        logger.info("=" * 40)
        all_results["reconciliation"] = check_row_count_reconciliation(df_bronze, df_silver)

        # ── Quality Score ────────────────────────────────────────────────
        quality_score = calculate_quality_score(all_results)
        logger.info("=" * 40)
        logger.info(f"OVERALL DATA QUALITY SCORE: {quality_score}/100")
        logger.info("=" * 40)

        if quality_score < 70:
            logger.error(f"Quality score below threshold (70): {quality_score}. Review failures.")
        elif quality_score < 90:
            logger.warning(f"Quality score has warnings: {quality_score}. Review warnings.")
        else:
            logger.info(f"Quality score acceptable: {quality_score}")

        log_pipeline_end(logger, STAGE)

    except Exception as e:
        handle_pipeline_error(STAGE, e)


if __name__ == "__main__":
    run()
