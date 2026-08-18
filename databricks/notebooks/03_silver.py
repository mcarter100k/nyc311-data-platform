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
# MAGIC - Borough name standardization (BROOKLYN, Bklyn, BK → BROOKLYN)
# MAGIC - Null filling for mandatory fields
# MAGIC - Derived metrics: `resolution_days`, `is_resolved`
# MAGIC - Data quality measurement: null rates, duplicate rate, invalid resolution days,
# MAGIC   unrecognized boroughs — written to SILVER.data_quality_log before the MERGE
# MAGIC - Records failing critical checks are quarantined to SILVER.service_requests_quarantine
# MAGIC   with a quarantine_reason column rather than being silently dropped
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
# MAGIC **Warn, don't fail on quality issues:** the quality check block raises warnings
# MAGIC but never fails the job when thresholds are breached. Failing the job would block
# MAGIC all downstream BI consumers. Warning + quarantine means good records still reach
# MAGIC Gold while the operations team has a concrete table to investigate. Silent drops
# MAGIC are the worst outcome — they corrupt metrics without any observable signal.

"""
03_silver.py
Silver layer: Bronze Delta → cleaned, deduplicated, typed Silver Delta table.
Quality metrics written to SILVER.data_quality_log before the MERGE.
Records failing critical checks written to SILVER.service_requests_quarantine.

Transformation logic is in silver_transformations.py. This notebook handles
all Databricks-specific I/O: reading tables, writing Delta, MERGE, secrets.
"""

# ── Standard library ──────────────────────────────────────────────────────────
import json

# ── PySpark ───────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)

# ── Delta Lake ────────────────────────────────────────────────────────────────
from delta.tables import DeltaTable

# ── Silver transformation functions ──────────────────────────────────────────
from silver_transformations import (
    DQ_THRESHOLDS,
    compute_dq_metrics,
    compute_resolution_days,
    compute_unique_key_checksum,
    deduplicate_on_unique_key,
    standardize_borough,
)

# COMMAND ----------

# ── Widget ────────────────────────────────────────────────────────────────────
dbutils.widgets.text("run_date", "2024-01-01", "Run date (YYYY-MM-DD)")
run_date = dbutils.widgets.get("run_date")
print(f"run_date={run_date}")

# COMMAND ----------

# ── Secrets and ADLS config ───────────────────────────────────────────────────
STORAGE_ACCOUNT = dbutils.secrets.get(scope="nyc311", key="adls-storage-account")
ADLS_ACCESS_KEY  = dbutils.secrets.get(scope="nyc311", key="adls-access-key")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    ADLS_ACCESS_KEY,
)

BRONZE_TABLE     = "bronze.service_requests"
SILVER_TABLE     = "silver.service_requests"
DQ_LOG_TABLE     = "silver.data_quality_log"
QUARANTINE_TABLE = "silver.service_requests_quarantine"
CHECKSUMS_TABLE  = "silver.run_checksums"

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
# Keep the most recent _ingest_timestamp row per unique_key. A service request
# may appear more than once in Bronze if the ingest notebook was run more than
# once for the same date (e.g. after a force reload or API retry).

df_deduped = deduplicate_on_unique_key(df_bronze)

# COMMAND ----------

# ── Step 2: Borough name standardization ──────────────────────────────────────
# Collapses 15+ raw spelling variants to the five canonical borough names.

df_borough = standardize_borough(df_deduped)

# COMMAND ----------

# ── Step 3: Null handling ─────────────────────────────────────────────────────
# Mandatory fields that cannot be null in the Silver contract.

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
# _silver_timestamp: audit column used as the dbt incremental watermark.

df_derived = (
    compute_resolution_days(df_nulls)
    .withColumn("_silver_timestamp", F.current_timestamp())
)

# COMMAND ----------

# ── Step 5: Compute and write data quality metrics ────────────────────────────
# One row per check, written before the MERGE so that a MERGE failure still
# preserves the quality snapshot for this run_date.
#
# compute_dq_metrics performs all count() operations internally.
# The notebook extracts individual counts from the returned rows to avoid
# computing them a second time for the final metrics summary.

dq_rows = compute_dq_metrics(df_bronze, df_deduped, df_derived, run_date)
dq_by_check = {r.check_name: r for r in dq_rows}

count_null_unique_key      = dq_by_check["null_rate_unique_key"].records_failed
count_null_created_date    = dq_by_check["null_rate_created_date"].records_failed
count_dedup_dropped        = dq_by_check["duplicate_rate"].records_failed
count_after_dedup          = count_bronze - count_dedup_dropped
count_unrecognized_borough = dq_by_check["unrecognized_borough"].records_failed
count_invalid_resolution_days = dq_by_check["invalid_resolution_days"].records_failed

dq_schema = StructType([
    StructField("run_date",        StringType(), False),
    StructField("check_name",      StringType(), False),
    StructField("records_checked", LongType(),   False),
    StructField("records_failed",  LongType(),   False),
    StructField("failure_rate",    DoubleType(), False),
    StructField("pipeline_stage",  StringType(), False),
])

df_dq = spark.createDataFrame(dq_rows, schema=dq_schema)

(
    df_dq.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"run_date = '{run_date}'")
    .saveAsTable(DQ_LOG_TABLE)
)
print(f"Data quality log written to {DQ_LOG_TABLE} for run_date={run_date}.")

# COMMAND ----------

# ── Step 6: Idempotency checksum ──────────────────────────────────────────────
# Hash the sorted set of non-null unique_keys from df_deduped to detect replays
# that return a different key population for the same run_date.
#
# sort_array is applied inside compute_unique_key_checksum before sha2 because
# Spark makes no row-ordering guarantees across runs or cluster configurations.
# An unsorted hash would differ between two runs of identical data, producing
# false DATA CONTRACT VIOLATION alerts on legitimate replays.

checksums_schema = StructType([
    StructField("run_date",     StringType(), False),
    StructField("key_checksum", StringType(), False),
    StructField("key_count",    LongType(),   False),
])

current_checksum, current_key_count = compute_unique_key_checksum(df_deduped)

try:
    prev_row = (
        spark.table(CHECKSUMS_TABLE)
        .filter(F.col("run_date") == run_date)
        .select("key_checksum")
        .first()
    )
    prev_checksum = prev_row["key_checksum"] if prev_row else None
except Exception:
    prev_checksum = None

if prev_checksum is None:
    print(
        f"Checksum first run for run_date={run_date}: "
        f"{current_checksum[:16]}... ({current_key_count:,} keys)"
    )
elif prev_checksum == current_checksum:
    print(f"Idempotency confirmed for run_date={run_date}: checksum matches previous run.")
else:
    print(
        f"DATA CONTRACT VIOLATION — run_date={run_date}: "
        f"key_checksum changed ({prev_checksum[:16]}... → {current_checksum[:16]}...). "
        f"The unique_key population differs from the previous run. "
        f"Investigate whether the API returned different records or run_date filtering changed."
    )

df_checksum = (
    spark.createDataFrame(
        [(run_date, current_checksum, current_key_count)],
        schema=checksums_schema,
    )
    .withColumn("computed_at", F.current_timestamp())
)

(
    df_checksum.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"run_date = '{run_date}'")
    .saveAsTable(CHECKSUMS_TABLE)
)
print(f"Checksum written to {CHECKSUMS_TABLE} for run_date={run_date}.")

# COMMAND ----------

# ── Step 7: Evaluate thresholds and emit warnings ─────────────────────────────
# Warn but never raise. The full rationale is in the notebook header.
# Warnings surface in Databricks job logs and Airflow task output — they are
# observable without failing the pipeline.

dq_warnings = []
for row in dq_rows:
    threshold = DQ_THRESHOLDS.get(row.check_name)
    if threshold and row.failure_rate > threshold:
        msg = (
            f"DATA QUALITY WARNING [{row.check_name}]: "
            f"failure_rate={row.failure_rate:.4%} exceeds threshold={threshold:.4%} "
            f"({row.records_failed:,} / {row.records_checked:,} records). "
            f"run_date={run_date}."
        )
        dq_warnings.append(msg)
        print(msg)

if not dq_warnings:
    print("All data quality checks within thresholds.")

# COMMAND ----------

# ── Step 8: Quarantine records failing critical checks ────────────────────────
# The quarantine table is intentionally outside the dbt lineage graph. It is an
# operational table for data quality investigation, not an analyst-facing dataset.
# Engineers query it directly via Snowflake when investigating DQ failures.
#
# Records that fail critical checks are written to a quarantine table with a
# quarantine_reason column rather than being silently dropped.
#
# Critical checks (quarantine, not just warn):
#   - null unique_key: cannot be MERGEd without a natural key
#   - negative resolution_days: closed_date < created_date; corrupts Gold metrics

df_null_key = df_derived.filter(
    F.col("unique_key").isNull()
).withColumn("quarantine_reason", F.lit("null_unique_key"))

df_neg_resolution = df_derived.filter(
    F.col("unique_key").isNotNull()
    & F.col("resolution_days").isNotNull()
    & (F.col("resolution_days") < 0)
).withColumn("quarantine_reason", F.lit("negative_resolution_days"))

df_quarantine = df_null_key.union(df_neg_resolution)
count_quarantined = df_quarantine.count()

if count_quarantined > 0:
    (
        df_quarantine.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"run_date = '{run_date}'")
        .option("mergeSchema", "true")
        .saveAsTable(QUARANTINE_TABLE)
    )
    print(
        f"Quarantined {count_quarantined:,} records to {QUARANTINE_TABLE} "
        f"for run_date={run_date}. "
        f"({df_null_key.count():,} null_unique_key, "
        f"{df_neg_resolution.count():,} negative_resolution_days)"
    )
else:
    print(f"No records quarantined for run_date={run_date}.")

# COMMAND ----------

# ── Step 9: Filter to records that pass all critical checks ───────────────────
# df_quality is the population that enters the Silver MERGE.
# Records removed here were quarantined above — nothing is silently discarded.

df_quality = df_derived.filter(
    F.col("unique_key").isNotNull()
    & (F.col("resolution_days").isNull() | (F.col("resolution_days") >= 0))
)

count_silver = df_quality.count()
print(f"Records entering Silver MERGE: {count_silver:,}")

# COMMAND ----------

# ── Step 10: MERGE into Silver Delta table ────────────────────────────────────
# MERGE on unique_key provides idempotency: rerunning this notebook for the same
# run_date updates existing records (e.g. status changed from Open to Closed since
# last load) and inserts new records. No duplicates, no gaps.
#
# ── Dynamic column resolution ─────────────────────────────────────────────────
# The MERGE SET and VALUES expressions are built at runtime from the actual
# columns present in df_quality, rather than being hardcoded. This means a new
# Bronze column — added by the Socrata API and detected by Auto Loader — flows
# into Silver automatically on the next pipeline run without any code change.
#
# autoMerge is required alongside this: it instructs Delta Lake to extend the
# Silver table's schema when the source DataFrame contains columns not yet in
# the target. Without it, a new column in df_quality would cause the MERGE to
# fail with a schema mismatch error.
#
# Tradeoff — where we draw the line:
#   Bronze and Silver are intentionally schema-permissive. New source columns
#   are expected, backwards-compatible, and low-risk (they are nullable with no
#   downstream code relying on them yet). Propagating them automatically reduces
#   pipeline downtime and manual intervention.
#
#   Gold (dbt) is intentionally schema-explicit. Every column that flows into a
#   Gold mart model must be referenced by name in a .sql file and documented in
#   a .yml file. New Silver columns do NOT automatically appear in Gold marts —
#   they require a dbt model change, a PR, and review. This is the contract that
#   protects BI consumers: analysts can trust that the columns they query will
#   not change shape under them without a deliberate, reviewed change.
#
#   The boundary: Silver MERGE accepts anything Bronze sends. dbt staging models
#   (`stg_service_requests`) use SELECT *, but Gold marts use explicit column
#   lists. A new column becomes visible in Gold only when someone adds it to a
#   mart .sql file and bumps schema_version in dbt_project.yml.

# Build MERGE expressions from the runtime schema of df_quality.
# df_quality already contains all Bronze source columns plus Silver-derived
# columns (resolution_days, is_resolved, _silver_timestamp) added in Steps 2–4.
_silver_cols = df_quality.columns

# On MATCHED rows: update every column except the join key. Updating unique_key
# to its own value is harmless but semantically misleading — the join predicate
# guarantees it is already equal.
_update_set = {col: f"source.{col}" for col in _silver_cols if col != "unique_key"}

# On NOT MATCHED rows: insert all columns including unique_key.
_insert_values = {col: f"source.{col}" for col in _silver_cols}

# Enable Delta schema evolution for the MERGE target. This adds any columns
# present in the source but absent from the Silver table, rather than erroring.
# Scoped to this session — does not affect other notebooks running concurrently.
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

silver_delta = DeltaTable.forName(spark, SILVER_TABLE)

(
    silver_delta.alias("target")
    .merge(
        df_quality.alias("source"),
        "target.unique_key = source.unique_key",
    )
    .whenMatchedUpdate(set=_update_set)
    .whenNotMatchedInsert(values=_insert_values)
    .execute()
)

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
# In this pipeline, agency SCD Type 2 is implemented in dbt Gold: the
# agency_snapshot (check strategy on agency_name) feeds dim_agency, and
# fct_service_requests joins point-in-time on the version validity window.
# See ADR 007. The Delta MERGE pattern below is the REJECTED alternative
# (Option B in ADR 007) — kept as reference for what a Silver-side
# implementation would look like, and why it was not chosen: custom MERGE
# code for a ~60-row dimension, mixed into the fact-processing notebook.
#
# The rejected Silver-side SCD Type 2 pattern:
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
spark.sql(f"OPTIMIZE {SILVER_TABLE} WHERE _run_date = '{run_date}'")
print(f"OPTIMIZE complete for Silver run_date={run_date}.")

# COMMAND ----------

# ── Final metrics ─────────────────────────────────────────────────────────────
metrics = {
    "run_date":              run_date,
    "bronze_records":        count_bronze,
    "after_dedup":           count_after_dedup,
    "dedup_dropped":         count_dedup_dropped,
    "null_unique_key":       count_null_unique_key,
    "null_created_date":     count_null_created_date,
    "unrecognized_borough":  count_unrecognized_borough,
    "invalid_resolution":    count_invalid_resolution_days,
    "quarantined":           count_quarantined,
    "checksum_key_count":    current_key_count,
    "dq_warnings":           len(dq_warnings),
    "silver_inserted":       num_inserted,
    "silver_updated":        num_updated,
    "silver_total":          num_inserted + num_updated,
}

print("─" * 60)
print("Silver layer complete — metrics summary")
print("─" * 60)
for key, val in metrics.items():
    print(f"  {key:<25} {val:>10,}" if isinstance(val, int) else f"  {key:<25} {val}")
if dq_warnings:
    print("─" * 60)
    print(f"  {len(dq_warnings)} DATA QUALITY WARNING(S) — see {DQ_LOG_TABLE}")
print("─" * 60)

dbutils.jobs.taskValues.set(key="silver_metrics", value=json.dumps(metrics))
dbutils.jobs.taskValues.set(key="status",         value="success")

dbutils.notebook.exit(json.dumps({"status": "success", **metrics}))
