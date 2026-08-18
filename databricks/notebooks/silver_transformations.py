"""
Pure PySpark transformation functions for the Silver layer.

All functions here accept a Spark DataFrame and return a DataFrame or a plain
Python value. No Databricks-specific APIs (dbutils, DeltaTable, spark.read,
spark.write) are used, which makes these functions unit-testable with a local
SparkSession and inline test data.

03_silver.py imports and calls these functions. The notebook is responsible for
all I/O (reading Bronze, writing Silver/DQ log/quarantine). This module is
responsible for all transformation logic.
"""

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import BooleanType, IntegerType

# ── Constants ─────────────────────────────────────────────────────────────────

DQ_THRESHOLDS = {
    "null_rate_unique_key":    0.05,
    "null_rate_created_date":  0.05,
    "duplicate_rate":          0.10,
    "invalid_resolution_days": 0.01,
    "unrecognized_borough":    0.05,
}

# All borough strings (uppercase, trimmed) that the standardization map handles.
# Any value NOT in this set that is non-null and non-empty is unrecognized and
# counts toward the unrecognized_borough DQ metric.
KNOWN_BOROUGH_VARIANTS = {
    "BROOKLYN", "BKLYN", "BK", "KINGS", "KINGS COUNTY",
    "MANHATTAN", "MN", "NEW YORK", "NEW YORK CITY", "NYC", "NY",
    "QUEENS", "QN", "QNS", "QUEENS COUNTY",
    "BRONX", "THE BRONX", "BX", "BRONX COUNTY",
    "STATEN ISLAND", "SI", "S.I.", "STATEN IS", "RICHMOND",
    "UNSPECIFIED",
}


# ── Transformation functions ──────────────────────────────────────────────────

def standardize_borough(df: DataFrame) -> DataFrame:
    """Collapse all known borough spelling variants to the five canonical names.

    Any non-null, non-empty borough value not in the mapping becomes UNSPECIFIED.
    Null values also map to UNSPECIFIED via the CASE WHEN ... ELSE branch.
    """
    return df.withColumn(
        "borough",
        F.when(
            F.upper(F.trim(F.col("borough"))).isin(
                "BROOKLYN", "BKLYN", "BK", "KINGS", "KINGS COUNTY"
            ),
            F.lit("BROOKLYN"),
        )
        .when(
            F.upper(F.trim(F.col("borough"))).isin(
                "MANHATTAN", "MN", "NEW YORK", "NEW YORK CITY", "NYC", "NY"
            ),
            F.lit("MANHATTAN"),
        )
        .when(
            F.upper(F.trim(F.col("borough"))).isin(
                "QUEENS", "QN", "QNS", "QUEENS COUNTY"
            ),
            F.lit("QUEENS"),
        )
        .when(
            F.upper(F.trim(F.col("borough"))).isin(
                "BRONX", "THE BRONX", "BX", "BRONX COUNTY"
            ),
            F.lit("BRONX"),
        )
        .when(
            F.upper(F.trim(F.col("borough"))).isin(
                "STATEN ISLAND", "SI", "S.I.", "STATEN IS", "RICHMOND"
            ),
            F.lit("STATEN ISLAND"),
        )
        .otherwise(F.lit("UNSPECIFIED")),
    )


def compute_resolution_days(df: DataFrame) -> DataFrame:
    """Add resolution_days (integer) and is_resolved (boolean) to the DataFrame.

    resolution_days is null for open requests (no closed_date) — not zero.
    Zero would incorrectly imply same-day resolution for open tickets.
    Negative values (closed_date < created_date) are preserved here and
    quarantined by the caller.
    """
    return (
        df.withColumn(
            "resolution_days",
            F.when(
                F.col("closed_date").isNotNull() & F.col("created_date").isNotNull(),
                F.datediff(F.col("closed_date"), F.col("created_date")),
            ).otherwise(F.lit(None).cast(IntegerType())),
        )
        .withColumn(
            "is_resolved",
            (F.col("status") == "Closed").cast(BooleanType()),
        )
    )


def deduplicate_on_unique_key(df: DataFrame) -> DataFrame:
    """Keep one row per unique_key, retaining the most recent _ingest_timestamp.

    Uses a Window rank rather than dropDuplicates so the surviving row is
    deterministic: when a unique_key appears more than once (e.g. due to
    API pagination overlap or a force-reload), the row with the largest
    _ingest_timestamp is kept. Rows with null unique_key all share the same
    Window partition and are collapsed to the most recent one; they are
    quarantined downstream regardless.
    """
    w = Window.partitionBy("unique_key").orderBy(F.col("_ingest_timestamp").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def quarantine_partition_predicate(run_date: str) -> str:
    """replaceWhere predicate for the per-run overwrite of the quarantine table.

    Must reference `_run_date` — the Bronze audit column that quarantined
    records actually carry. The record DataFrames have no bare `run_date`
    column; a predicate naming one makes the first genuinely quarantined
    record fail the whole Silver job with an unresolved-column error.
    """
    return f"_run_date = '{run_date}'"


def select_quarantine(df: DataFrame) -> DataFrame:
    """Rows failing critical checks, labeled with quarantine_reason.

    Critical checks (quarantine, not just warn):
      - null unique_key: cannot be MERGEd without a natural key
      - negative resolution_days: closed_date < created_date; corrupts metrics
    """
    df_null_key = df.filter(
        F.col("unique_key").isNull()
    ).withColumn("quarantine_reason", F.lit("null_unique_key"))

    df_neg_resolution = df.filter(
        F.col("unique_key").isNotNull()
        & F.col("resolution_days").isNotNull()
        & (F.col("resolution_days") < 0)
    ).withColumn("quarantine_reason", F.lit("negative_resolution_days"))

    return df_null_key.union(df_neg_resolution)


def failure_rate(failed: int, checked: int) -> float:
    """Compute failure rate as a proportion, safe against zero denominators."""
    return round(failed / checked, 6) if checked > 0 else 0.0


def compute_unique_key_checksum(df: DataFrame, key_col: str = "unique_key") -> tuple:
    """Return (checksum, key_count) for the non-null unique_key values in df.

    The hash is computed over a sort_array of key values so that two runs with
    identical key sets produce identical checksums regardless of Spark's
    non-deterministic row ordering. Without sort_array, partition order
    differences across cluster configurations or retries would produce hash
    mismatches for the same data — causing false DATA CONTRACT VIOLATION alerts
    on legitimate replays.

    Returns:
        checksum (str): SHA-256 hex digest, or "" if df is empty or all keys null.
        key_count (int): Number of non-null keys that entered the hash.
    """
    row = (
        df.filter(F.col(key_col).isNotNull())
          .agg(
              F.sha2(
                  F.array_join(
                      F.sort_array(F.collect_list(F.col(key_col))),
                      "\n",
                  ),
                  256,
              ).alias("checksum"),
              F.count(F.col(key_col)).alias("n"),
          )
          .first()
    )
    if row is None or row["n"] == 0:
        return ("", 0)
    return (row["checksum"] or "", int(row["n"]))


def compute_dq_metrics(
    df_bronze: DataFrame,
    df_deduped: DataFrame,
    df_derived: DataFrame,
    run_date: str,
) -> list:
    """Compute the five data quality metrics for one Silver pipeline run.

    Returns a list of Row objects suitable for writing to SILVER.data_quality_log.
    All DataFrame.count() calls happen inside this function so the caller does
    not need to maintain count variables separately.

    Args:
        df_bronze:  Raw Bronze records for this run_date (before dedup).
                    Used for null_rate checks — null rates are measured on the
                    full Bronze population, not the deduplicated one.
        df_deduped: After dedup, before borough standardization.
                    Used for the unrecognized_borough check so we count raw
                    values, not already-standardized ones.
        df_derived: After all transformations including resolution_days.
                    Used for the invalid_resolution_days check.
        run_date:   Pipeline run date string (YYYY-MM-DD).
    """
    n_bronze            = df_bronze.count()
    n_null_unique_key   = df_bronze.filter(F.col("unique_key").isNull()).count()
    n_null_created_date = df_bronze.filter(F.col("created_date").isNull()).count()
    n_deduped           = df_deduped.count()
    n_dedup_dropped     = n_bronze - n_deduped
    n_unrecognized      = df_deduped.filter(
        F.col("borough").isNotNull()
        & (F.trim(F.col("borough")) != "")
        & ~F.upper(F.trim(F.col("borough"))).isin(*KNOWN_BOROUGH_VARIANTS)
    ).count()
    n_invalid_res       = df_derived.filter(
        F.col("resolution_days").isNotNull() & (F.col("resolution_days") < 0)
    ).count()

    return [
        Row(
            run_date=run_date,
            check_name="null_rate_unique_key",
            records_checked=n_bronze,
            records_failed=n_null_unique_key,
            failure_rate=failure_rate(n_null_unique_key, n_bronze),
            pipeline_stage="silver",
        ),
        Row(
            run_date=run_date,
            check_name="null_rate_created_date",
            records_checked=n_bronze,
            records_failed=n_null_created_date,
            failure_rate=failure_rate(n_null_created_date, n_bronze),
            pipeline_stage="silver",
        ),
        Row(
            run_date=run_date,
            check_name="duplicate_rate",
            records_checked=n_bronze,
            records_failed=n_dedup_dropped,
            failure_rate=failure_rate(n_dedup_dropped, n_bronze),
            pipeline_stage="silver",
        ),
        Row(
            run_date=run_date,
            check_name="invalid_resolution_days",
            records_checked=n_deduped,
            records_failed=n_invalid_res,
            failure_rate=failure_rate(n_invalid_res, n_deduped),
            pipeline_stage="silver",
        ),
        Row(
            run_date=run_date,
            check_name="unrecognized_borough",
            records_checked=n_deduped,
            records_failed=n_unrecognized,
            failure_rate=failure_rate(n_unrecognized, n_deduped),
            pipeline_stage="silver",
        ),
    ]
