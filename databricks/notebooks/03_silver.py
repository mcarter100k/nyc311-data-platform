# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver Layer
# MAGIC
# MAGIC Reads the Bronze Delta table for the given `run_date`, applies data quality and
# MAGIC conformance rules, and writes a clean, deduplicated record set to the Silver Delta
# MAGIC table using a `MERGE` statement for idempotency.
# MAGIC
# MAGIC **Single responsibility of Silver:** quality and conformance. Silver applies:
# MAGIC - Deduplication on `unique_key` (the natural key from NYC Open Data)
# MAGIC - Borough name standardization (BROOKLYN, Bklyn, BK → Brooklyn)
# MAGIC - Null filling for mandatory fields
# MAGIC - Derived metrics: `resolution_days`, `is_resolved`
# MAGIC - Data quality filter: records with `resolution_days < 0` are logged and dropped
# MAGIC
# MAGIC **What Silver does not do:** Silver does not apply business logic (complaint
# MAGIC categories, overdue flags, dimensional surrogate keys). Those belong in dbt Gold.
# MAGIC Mixing cleaning with business logic makes bugs harder to isolate — a wrong
# MAGIC complaint category might be a Silver cleaning error or a Gold SQL error, and
# MAGIC without a clean Silver checkpoint you cannot distinguish them.
# MAGIC
# MAGIC **MERGE idempotency:** matching on `unique_key` means rerunning this notebook
# MAGIC for the same `run_date` updates existing records and inserts new ones — no
# MAGIC duplicates, no gaps, safe for Airflow retry.
# MAGIC
# MAGIC **SCD Type 2 note:** Agency name changes are tracked in a commented block below
# MAGIC showing the effective/expiry date pattern. In this pipeline, dbt Gold handles
# MAGIC the agency dimension — the pattern is documented at Silver for completeness and
# MAGIC as an interview reference.

"""
03_silver.py
Silver layer: Bronze Delta → cleaned, deduplicated, typed Silver Delta table.
Uses MERGE for idempotency. Logs data quality metrics for observability.
"""

# ── Standard library ──────────────────────────────────────────────────────────
import json

# ── PySpark ───────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, BooleanType

# ── Delta Lake ────────────────────────────────────────────────────────────────
from delta.tables import DeltaTable

# COMMAND ----------

# ── Widget ────────────────────────────────────────────────────────────────────
dbutils.widgets.text("run_date", "2024-01-01", "Run date (YYYY-MM-DD)")
run_date = dbutils.widgets.get("run_date")
print(f"run_date={run_date}")

# COMMAND ----------

# ── Secrets and ADLS config ───────────────────────────────────────────────────
STORAGE_ACCOUNT = dbutils.secrets.get(scope="nyc311", key="adls-storage-account")
ADLS_ACCESS_KEY = dbutils.secrets.get(scope="nyc311", key="adls-access-key")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    ADLS_ACCESS_KEY,
)

BRONZE_TABLE = "bronze.service_requests"
SILVER_TABLE = "silver.service_requests"

# COMMAND ----------

# ── Read Bronze for run_date ──────────────────────────────────────────────────
# Filter to the specific partition to avoid full-table scans on large Bronze tables.
# _run_date is a Delta partition column — this predicate enables partition pruning.

df_bronze = spark.read.table(BRONZE_TABLE).filter(F.col("_run_date") == run_date)

count_bronze = df_bronze.count()
print(f"Bronze records for run_date={run_date}: {count_bronze:,}")

if count_bronze == 0:
    raise RuntimeError(
        f"No Bronze records found for run_date={run_date}. "
        f"Ensure 02_bronze.py completed successfully before running Silver."
    )

# COMMAND ----------

# ── Step 1: Deduplication ────────────────────────────────────────────────────
# unique_key is the natural primary key assigned by NYC Open Data.
# A single service request may appear multiple times in the Bronze table if
# ingest_raw was run more than once for the same date (e.g. after a force reload).
#
# dropDuplicates(['unique_key']): when multiple rows share the same unique_key,
# Spark retains one arbitrarily. To retain the most-recently-created row
# (last-write-wins), use a Window function on _ingest_timestamp instead.
# For this pipeline, last-write-wins is correct: Bronze re-runs write the
# same API payload, so all duplicates have identical data.

df_deduped = df_bronze.dropDuplicates(["unique_key"])

count_after_dedup = df_deduped.count()
count_dedup_dropped = count_bronze - count_after_dedup
print(f"Deduplication: {count_bronze:,} → {count_after_dedup:,} "
      f"(dropped {count_dedup_dropped:,} duplicates)")

# COMMAND ----------

# ── Step 2: Borough name standardization ──────────────────────────────────────
# The Socrata dataset contains at least 15 distinct borough spellings across
# the historical dataset. All variants are collapsed to the five canonical
# borough names used in the Gold dimensional model.
#
# 'UNSPECIFIED' is the canonical sentinel for null, empty, or unrecognized
# borough values — this matches the accepted_values test in dbt/models/marts/
# marts.yml (borough: [BROOKLYN, MANHATTAN, QUEENS, BRONX, STATEN ISLAND, UNSPECIFIED]).

df_borough = df_deduped.withColumn(
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

# COMMAND ----------

# ── Step 3: Null handling ─────────────────────────────────────────────────────
# Mandatory fields that cannot be null in the Silver contract.
# Sentinel values are chosen to be queryable without IS NULL checks:
#   agency → UNKNOWN (prevents join failures in Gold dim_agency)
#   complaint_type → UNKNOWN (prevents null in Gold fct_service_requests)
# borough is already handled above (UNSPECIFIED sentinel).

df_nulls = (
    df_borough
    .withColumn(
        "agency",
        F.when(
            F.col("agency").isNull() | (F.trim(F.col("agency")) == ""),
            F.lit("UNKNOWN"),
        ).otherwise(F.col("agency")),
    )
    .withColumn(
        "agency_name",
        F.when(
            F.col("agency_name").isNull() | (F.trim(F.col("agency_name")) == ""),
            F.lit("UNKNOWN"),
        ).otherwise(F.col("agency_name")),
    )
    .withColumn(
        "complaint_type",
        F.when(F.col("complaint_type").isNull(), F.lit("UNKNOWN"))
        .otherwise(F.col("complaint_type")),
    )
)

# COMMAND ----------

# ── Step 4: Derived columns ───────────────────────────────────────────────────
# resolution_days: null-safe. An open request (no closed_date) gets null,
# not zero — zero would incorrectly imply same-day resolution.
#
# is_resolved: true only for status='Closed'. Pending and In Progress are
# deliberately false — they represent active work, not resolved outcomes.

df_derived = (
    df_nulls
    .withColumn(
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
    .withColumn(
        "_silver_timestamp",
        F.current_timestamp(),
    )
)

# COMMAND ----------

# ── Step 5: Data quality filter ───────────────────────────────────────────────
# Records where resolution_days < 0 indicate closed_date precedes created_date.
# This is a data entry error in the source system (~0.02% of records historically).
# They are logged as a data quality metric and dropped — they must never appear
# in Gold where the dbt test `assert_resolution_days_nonnegative` would catch them.
#
# The count is tracked for the metrics block at the end. If count_dq_dropped
# spikes above 100 for a single day, investigate the source data before retrying.

df_quality   = df_derived.filter(
    F.col("resolution_days").isNull() | (F.col("resolution_days") >= 0)
)
df_dq_dropped = df_derived.filter(
    F.col("resolution_days").isNotNull() & (F.col("resolution_days") < 0)
)

count_dq_dropped = df_dq_dropped.count()
count_silver     = count_after_dedup - count_dq_dropped

if count_dq_dropped > 0:
    print(
        f"DATA QUALITY: Dropped {count_dq_dropped} records with resolution_days < 0. "
        f"Sample unique_keys: "
        + str([r["unique_key"] for r in df_dq_dropped.limit(5).collect()])
    )

print(f"Records entering Silver MERGE: {count_silver:,}")

# COMMAND ----------

# ── Step 6: MERGE into Silver Delta table ─────────────────────────────────────
# MERGE on unique_key provides idempotency: rerunning this notebook for the same
# run_date updates existing records (e.g. status changed from Open to Closed since
# last load) and inserts new records. No duplicates, no gaps.
#
# DeltaTable.forName() requires the Silver Delta table to exist. On first run,
# create it with:
#   spark.sql("CREATE TABLE IF NOT EXISTS silver.service_requests USING DELTA LOCATION '...'")
# or via the Terraform databricks_sql_table resource.

silver_delta = DeltaTable.forName(spark, SILVER_TABLE)

(
    silver_delta.alias("target")
    .merge(
        df_quality.alias("source"),
        "target.unique_key = source.unique_key",
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# Retrieve merge metrics from Delta operation history.
merge_metrics = (
    spark.sql(f"DESCRIBE HISTORY {SILVER_TABLE} LIMIT 1")
    .select("operationMetrics")
    .first()["operationMetrics"]
)

num_updated  = int(merge_metrics.get("numTargetRowsUpdated",  0))
num_inserted = int(merge_metrics.get("numTargetRowsInserted", 0))
print(f"MERGE complete: {num_updated:,} updated, {num_inserted:,} inserted.")

# COMMAND ----------

# ── SCD Type 2 pattern — agency dimension (reference only) ────────────────────
#
# If agency names change over time and historical accuracy is required, Silver
# would track an agency SCD Type 2 table. The pattern is shown here; in this
# pipeline dbt Gold handles the agency dimension (dim_agency.sql), which is
# SCD Type 1 (latest name wins).
#
# To implement SCD Type 2 for agency at Silver:
#
#   from pyspark.sql.window import Window
#
#   df_agencies_today = df_quality.select("agency", "agency_name").distinct()
#
#   agency_delta = DeltaTable.forName(spark, "silver.dim_agency_scd2")
#
#   # Step 1: expire rows where agency_name changed
#   (
#       agency_delta.alias("target")
#       .merge(
#           df_agencies_today.alias("source"),
#           """
#           target.agency = source.agency
#           AND target.is_current = true
#           AND target.agency_name != source.agency_name
#           """,
#       )
#       .whenMatchedUpdate(set={
#           "is_current":   F.lit(False),
#           "expiry_date":  F.date_sub(F.current_date(), 1),
#       })
#       .execute()
#   )
#
#   # Step 2: insert new current records for changed agencies
#   df_new_versions = df_agencies_today  # (filter to only changed agencies in prod)
#   (
#       agency_delta.alias("target")
#       .merge(
#           df_new_versions.alias("source"),
#           "target.agency = source.agency AND target.is_current = true",
#       )
#       .whenNotMatchedInsert(values={
#           "agency":         "source.agency",
#           "agency_name":    "source.agency_name",
#           "effective_date": F.current_date(),
#           "expiry_date":    F.lit("9999-12-31").cast("date"),
#           "is_current":     F.lit(True),
#       })
#       .execute()
#   )

# COMMAND ----------

# ── OPTIMIZE Silver table ─────────────────────────────────────────────────────
# Compact small files introduced by the MERGE. Scope to recent data to avoid
# rewriting the entire Silver table on every run.

spark.sql(f"OPTIMIZE {SILVER_TABLE} WHERE _run_date = '{run_date}'")
print(f"OPTIMIZE complete for Silver run_date={run_date}.")

# COMMAND ----------

# ── Final metrics ─────────────────────────────────────────────────────────────
# These numbers surface in the Databricks Jobs UI run output and in Airflow
# task logs via the DatabricksRunNowOperator polling output.

metrics = {
    "run_date":            run_date,
    "bronze_records":      count_bronze,
    "after_dedup":         count_after_dedup,
    "dedup_dropped":       count_dedup_dropped,
    "dq_dropped":          count_dq_dropped,
    "silver_inserted":     num_inserted,
    "silver_updated":      num_updated,
    "silver_total":        num_inserted + num_updated,
}

print("─" * 60)
print("Silver layer complete — metrics summary")
print("─" * 60)
for key, val in metrics.items():
    print(f"  {key:<25} {val:>10,}" if isinstance(val, int) else f"  {key:<25} {val}")
print("─" * 60)

dbutils.jobs.taskValues.set(key="silver_metrics", value=json.dumps(metrics))
dbutils.jobs.taskValues.set(key="status",         value="success")

dbutils.notebook.exit(json.dumps({"status": "success", **metrics}))
