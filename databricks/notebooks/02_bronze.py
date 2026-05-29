# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze Layer
# MAGIC
# MAGIC Reads raw JSON files written by `01_ingest_raw` from the ADLS raw zone and registers
# MAGIC them as a partitioned Delta table in the Bronze schema. This notebook applies **structural
# MAGIC typing only** — no business logic, no deduplication, no borough cleaning. Bronze is
# MAGIC a faithful, append-only mirror of what arrived from the API.
# MAGIC
# MAGIC **Auto Loader** is used instead of `spark.read.json()` for two reasons:
# MAGIC 1. Schema inference is checkpointed — new API fields are detected automatically
# MAGIC    without breaking existing reads (`cloudFiles.inferColumnTypes = true`).
# MAGIC 2. The schema checkpoint prevents repeated schema-merge overhead on incremental runs.
# MAGIC
# MAGIC **Write mode:** `append`. Bronze is never overwritten or truncated. If a partition
# MAGIC needs to be corrected, delete the Bronze rows for that `_run_date` and rerun the
# MAGIC ingest notebook — the audit trail is preserved in the raw zone.
# MAGIC
# MAGIC **Audit columns** added here (`_ingest_timestamp`, `_source_file`, `_run_date`) are
# MAGIC the only Bronze-layer transformations. They exist to make lineage queries possible.

"""
02_bronze.py
Bronze layer: raw ADLS JSON → Delta table with audit columns and structural type casts.
Append-only. Never apply business logic here — Bronze is a raw record, not a clean one.
"""

# ── Standard library ──────────────────────────────────────────────────────────
import json

# ── PySpark ───────────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import (
    TimestampType,
    DoubleType,
    StringType,
)

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

# COMMAND ----------

# ── Path configuration ────────────────────────────────────────────────────────
RAW_PATH = (
    f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    f"/nyc311/ingest_date={run_date}"
)

# Auto Loader schema checkpoint: persists the inferred schema across runs.
# Without this, Auto Loader re-infers schema every run — expensive and fragile
# when new API fields appear.
SCHEMA_CHECKPOINT = (
    f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    f"/nyc311/_checkpoints/bronze_schema"
)

# Streaming checkpoint for Auto Loader writeStream state.
STREAM_CHECKPOINT = (
    f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    f"/nyc311/_checkpoints/bronze_stream"
)

BRONZE_TABLE = "bronze.service_requests"

print(f"Reading from : {RAW_PATH}")
print(f"Writing to   : {BRONZE_TABLE}")

# COMMAND ----------

# ── Auto Loader read ──────────────────────────────────────────────────────────
# cloudFiles.format=json          — source files are JSONL (one record per line).
# cloudFiles.schemaLocation       — persists schema inference across runs; enables
#                                   safe schema evolution when the Socrata API adds
#                                   new columns.
# cloudFiles.inferColumnTypes     — infers data types rather than defaulting all
#                                   columns to StringType. This is acceptable at
#                                   Bronze because we immediately re-cast key columns
#                                   below for safety.

df_raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format",           "json")
    .option("cloudFiles.schemaLocation",   SCHEMA_CHECKPOINT)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(RAW_PATH)
)

# COMMAND ----------

# ── Audit columns ─────────────────────────────────────────────────────────────
# Added at Bronze — these columns never appear in the source data and are the
# only Bronze-layer "transformations". They enable lineage queries:
#   - Which run_date populated this row?
#   - Which ADLS file did it come from?
#   - When was it written?

df_with_audit = (
    df_raw
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source_file",      F.input_file_name())
    .withColumn("_run_date",         F.lit(run_date))
)

# COMMAND ----------

# ── Structural type casts — Bronze-safe ───────────────────────────────────────
# Only timestamp and coordinate columns are cast here. The cast is permissive:
# if a value does not parse, the column becomes null rather than failing the job.
# ALL other columns remain StringType — business-level casting (borough
# standardization, status normalization) belongs in Silver, not Bronze.
#
# Design rationale: casting dates at Bronze allows partition pruning on
# created_date in the Silver job without re-parsing raw strings. Casting
# coordinates allows spatial queries directly on the Bronze table for debugging
# without Silver processing.

df_typed = (
    df_with_audit
    # Timestamps — try_to_timestamp is Snowflake syntax; in Spark, cast() returns
    # null on parse failure (permissive behavior, correct for Bronze).
    .withColumn("created_date",                   F.col("created_date").cast(TimestampType()))
    .withColumn("closed_date",                    F.col("closed_date").cast(TimestampType()))
    .withColumn("due_date",                       F.col("due_date").cast(TimestampType()))
    .withColumn("resolution_action_updated_date",
                F.col("resolution_action_updated_date").cast(TimestampType()))
    # Coordinates
    .withColumn("latitude",                       F.col("latitude").cast(DoubleType()))
    .withColumn("longitude",                      F.col("longitude").cast(DoubleType()))
    # All other columns are already StringType from Auto Loader inference.
    # Explicit cast here is a no-op but documents the Bronze contract:
    # every non-timestamp, non-coordinate field is a raw string until Silver.
    .withColumn("unique_key",                     F.col("unique_key").cast(StringType()))
    .withColumn("agency",                         F.col("agency").cast(StringType()))
    .withColumn("complaint_type",                 F.col("complaint_type").cast(StringType()))
    .withColumn("borough",                        F.col("borough").cast(StringType()))
    .withColumn("status",                         F.col("status").cast(StringType()))
)

# COMMAND ----------

# ── Write to Delta (append-only) ──────────────────────────────────────────────
# Bronze is append-only: new runs add new rows partitioned by _run_date.
# mergeSchema=true allows new Socrata API fields to be added without manual
# schema migration — Auto Loader handles this automatically.
#
# trigger(availableNow=True): process all files currently available in the
# path, then stop. This is the batch-oriented Auto Loader pattern —
# micro-batch semantics without a continuously running stream. Introduced
# in Spark 3.3; use trigger(once=True) for older Databricks Runtimes.

write_query = (
    df_typed
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", STREAM_CHECKPOINT)
    .option("mergeSchema",        "true")
    .partitionBy("_run_date")
    .trigger(availableNow=True)
    .toTable(BRONZE_TABLE)
)

write_query.awaitTermination()
print(f"Delta write complete for run_date={run_date}.")

# COMMAND ----------

# ── Row count assertion — fail fast on silent empty loads ─────────────────────
# A zero-row write is always wrong (01_ingest_raw either wrote records or
# exited with 'skipped' status). Raise here to prevent 02_bronze from
# succeeding silently while the Bronze table has a gap for this date.

row_count = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {BRONZE_TABLE} WHERE _run_date = '{run_date}'"
).first()["cnt"]

if row_count == 0:
    raise RuntimeError(
        f"Bronze write produced zero rows for run_date={run_date}. "
        f"Verify that 01_ingest_raw wrote files to {RAW_PATH} before retrying."
    )

print(f"Row count assertion passed: {row_count:,} rows in Bronze for run_date={run_date}.")

# COMMAND ----------

# ── OPTIMIZE and ZORDER ───────────────────────────────────────────────────────
# Run after write, scoped to today's partition.
# ZORDER BY (unique_key) co-locates files by unique_key so Silver's dedup merge
# reads fewer files when matching on unique_key.
#
# Note: OPTIMIZE is a Delta Lake command. On large tables (>10M rows), run this
# as a separate maintenance job on a schedule rather than in the ingestion path
# to avoid holding up the pipeline. At NYC 311 scale (~50k rows/day), the cost
# is negligible per run.

spark.sql(f"""
    OPTIMIZE {BRONZE_TABLE}
    WHERE _run_date = '{run_date}'
    ZORDER BY (unique_key)
""")
print(f"OPTIMIZE complete for run_date={run_date}.")

# COMMAND ----------

# ── Completion summary ────────────────────────────────────────────────────────
print(f"Bronze layer complete.")
print(f"  table     : {BRONZE_TABLE}")
print(f"  run_date  : {run_date}")
print(f"  row_count : {row_count:,}")

dbutils.jobs.taskValues.set(key="bronze_row_count", value=int(row_count))
dbutils.jobs.taskValues.set(key="status",           value="success")

dbutils.notebook.exit(
    json.dumps({
        "status":     "success",
        "table":      BRONZE_TABLE,
        "run_date":   run_date,
        "row_count":  row_count,
    })
)
