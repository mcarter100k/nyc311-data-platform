# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Raw Ingestion
# MAGIC
# MAGIC Pulls NYC 311 service request records — created OR updated since the run date —
# MAGIC from the NYC Open Data Socrata API and writes raw JSON to Azure Data Lake Storage
# MAGIC Gen2, partitioned by ingest date. This notebook is the first stage of the medallion
# MAGIC pipeline: no transformations, no type coercion — data is written exactly as
# MAGIC received so that any upstream changes are auditable.
# MAGIC
# MAGIC **Idempotency:** if the target partition already exists and `force_reload = false`,
# MAGIC the notebook exits early without re-ingesting. Airflow reruns are safe.
# MAGIC
# MAGIC **Pagination:** the Socrata API enforces a maximum of 50,000 rows per request.
# MAGIC This notebook paginates using `$offset` until an empty page is returned.
# MAGIC
# MAGIC **Secrets:** all credentials are retrieved from a Databricks secret scope named
# MAGIC `nyc311`. Configure via `databricks secrets put-secret --scope nyc311 --key ...`

"""
01_ingest_raw.py
Ingestion layer: Socrata API → ADLS Bronze zone (raw JSON, partitioned by ingest_date).
"""

# ── Standard library ──────────────────────────────────────────────────────────
import json
import time
from datetime import datetime

# ── Third-party ───────────────────────────────────────────────────────────────
import requests  # available on Databricks Runtime >= 9.1

# ── Request-building logic (pure, unit-tested) ────────────────────────────────
from ingest_config import PAGE_SIZE, SOCRATA_URL, build_page_params

# COMMAND ----------

# ── Widgets ───────────────────────────────────────────────────────────────────
# run_date default is overridden by Airflow DatabricksRunNowOperator via
# notebook_params: {"run_date": "{{ ds }}"}.
# The literal default below is used for manual runs in the Databricks UI.

dbutils.widgets.text("run_date",    "2024-01-01",   "Run date (YYYY-MM-DD)")
dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"], "Load type")
dbutils.widgets.text("force_reload", "false",        "Force overwrite existing partition")

run_date    = dbutils.widgets.get("run_date")
load_type   = dbutils.widgets.get("load_type")
force_reload = dbutils.widgets.get("force_reload").lower() == "true"

print(f"run_date={run_date}  load_type={load_type}  force_reload={force_reload}")

# COMMAND ----------

# ── Secrets — zero hardcoded values ──────────────────────────────────────────
# Scope `nyc311` must be pre-created with the following keys:
#   adls-storage-account  — ADLS Gen2 storage account name
#   adls-access-key       — Storage account access key (or use managed identity)
#   socrata-app-token     — NYC Open Data app token (increases rate limit to 1,000 req/s)

STORAGE_ACCOUNT   = dbutils.secrets.get(scope="nyc311", key="adls-storage-account")
ADLS_ACCESS_KEY   = dbutils.secrets.get(scope="nyc311", key="adls-access-key")
SOCRATA_APP_TOKEN = dbutils.secrets.get(scope="nyc311", key="socrata-app-token")

# Configure Spark to authenticate against ADLS Gen2.
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    ADLS_ACCESS_KEY,
)

# COMMAND ----------

# ── Path config ───────────────────────────────────────────────────────────────
# Hive-style partition: ingest_date=YYYY-MM-DD
# Auto Loader in 02_bronze.py can discover new partitions automatically.

RAW_BASE    = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net/nyc311"
OUTPUT_PATH = f"{RAW_BASE}/ingest_date={run_date}"

# COMMAND ----------

# ── Idempotency check ─────────────────────────────────────────────────────────
# Skip the API call entirely if the partition is already populated and
# force_reload is not set. This makes Airflow retries safe — rerunning the
# task after a downstream failure does not re-pull from the API.

if not force_reload:
    try:
        existing_files = dbutils.fs.ls(OUTPUT_PATH)
        if existing_files:
            print(
                f"Partition {OUTPUT_PATH} already exists "
                f"({len(existing_files)} files). Skipping ingestion."
            )
            dbutils.jobs.taskValues.set(key="row_count",  value=0)
            dbutils.jobs.taskValues.set(key="status",     value="skipped")
            dbutils.notebook.exit(json.dumps({"status": "skipped", "partition": OUTPUT_PATH}))
    except Exception:
        # Path does not exist — proceed with ingestion.
        pass

# COMMAND ----------

# ── Socrata API configuration ─────────────────────────────────────────────────
# Dataset: NYC 311 Service Requests (erm2-nwe9)
# Docs:    https://dev.socrata.com/foundry/data.cityofnewyork.us/erm2-nwe9
# URL, page size, and per-page query parameters come from ingest_config.py
# (pure module, unit-tested in tests/test_ingest_config.py).

headers = {
    "X-App-Token": SOCRATA_APP_TOKEN,
    "Accept":      "application/json",
}

# Incremental filter: fetch records created OR UPDATED on/after run_date, via
# the Socrata `:updated_at` system field. Filtering on created_date instead
# would fetch each request exactly once and never see later status changes —
# leaving the Silver MERGE's update path dead and undercounting resolutions.
# See ingest_config.build_page_params for the full rationale.
if load_type == "incremental":
    print(f"Incremental mode: fetching records created or updated since {run_date}.")
else:
    print("Full load mode: fetching entire dataset. This may take 10–20 minutes.")

# COMMAND ----------

# ── Paginated API fetch ───────────────────────────────────────────────────────

all_records  = []
page         = 0
retry_budget = 3  # retries per page on transient failures

while True:
    params          = build_page_params(load_type, run_date, page)
    attempt         = 0
    response        = None

    while attempt < retry_budget:
        try:
            response = requests.get(
                SOCRATA_URL, headers=headers, params=params, timeout=60
            )
            break
        except requests.exceptions.RequestException as exc:
            attempt += 1
            if attempt >= retry_budget:
                raise RuntimeError(
                    f"API request failed after {retry_budget} attempts on page {page}: {exc}"
                ) from exc
            wait = 2 ** attempt
            print(f"  Transient error on page {page} attempt {attempt}: {exc}. Retrying in {wait}s.")
            time.sleep(wait)

    if response.status_code != 200:
        raise RuntimeError(
            f"Socrata API returned HTTP {response.status_code} on page {page}. "
            f"Body: {response.text[:400]}"
        )

    records = response.json()

    if not records:
        # Empty page means pagination is complete.
        if page == 0:
            # No records at all — warn but do not fail. Quiet days (e.g. holidays)
            # can legitimately produce zero new 311 complaints.
            print(
                f"WARNING: API returned zero records for run_date={run_date}, "
                f"load_type={load_type}. This may be legitimate. "
                f"Verify against NYC Open Data before investigating."
            )
        else:
            print(f"Pagination complete. Last page was page {page - 1}.")
        break

    all_records.extend(records)
    page += 1
    print(f"  Page {page:>4}: fetched {len(records):>6} records  "
          f"(cumulative: {len(all_records):>8})")

    # For full loads, write intermediate batches to ADLS every 500k records
    # to avoid accumulating the full dataset in driver memory.
    if load_type == "full" and len(all_records) >= 500_000:
        batch_path = f"{OUTPUT_PATH}/batch_{page:05d}"
        df_batch   = spark.createDataFrame(all_records)
        df_batch.coalesce(4).write.mode("overwrite").json(batch_path)
        print(f"  Flushed {len(all_records):,} records to {batch_path}")
        all_records = []  # reset buffer

# COMMAND ----------

# ── Write final batch to ADLS ─────────────────────────────────────────────────
# For incremental loads (all records in memory), write as a single partition.
# For full loads, this writes the final partial batch after the loop above.

total_row_count = 0

if all_records:
    df_final = spark.createDataFrame(all_records)

    # coalesce(4): 4 files of ~12MB each for a typical daily load (~50k records).
    # Avoids the small-file problem downstream while keeping parallelism reasonable.
    (
        df_final
        .coalesce(4)
        .write
        .mode("overwrite")
        .json(OUTPUT_PATH)
    )
    total_row_count += len(all_records)
    print(f"Wrote final batch of {len(all_records):,} records to {OUTPUT_PATH}")

# For full loads, query the full partition count across all batch sub-directories.
if load_type == "full":
    df_verify       = spark.read.json(OUTPUT_PATH)
    total_row_count = df_verify.count()

# COMMAND ----------

# ── Completion metrics ────────────────────────────────────────────────────────
# taskValues are readable by downstream Databricks tasks (02_bronze, 03_silver).
# The Airflow DatabricksRunNowOperator surfaces run output in the task log.

print(f"Ingestion complete.")
print(f"  partition : {OUTPUT_PATH}")
print(f"  load_type : {load_type}")
print(f"  run_date  : {run_date}")
print(f"  row_count : {total_row_count:,}")

dbutils.jobs.taskValues.set(key="row_count",  value=total_row_count)
dbutils.jobs.taskValues.set(key="status",     value="success")
dbutils.jobs.taskValues.set(key="partition",  value=OUTPUT_PATH)

# Store in Airflow-visible Variable format (picked up by notify_success task).
# Requires the Databricks job to have permission to write to the Airflow metadata DB
# via the Airflow REST API — wire via a dbutils.secrets call to the Airflow API token.
# Simplified here: the value is logged to stdout and picked up by Airflow task logs.
print(f"AIRFLOW_VAR:LAST_INGEST_ROW_COUNT={total_row_count}")

dbutils.notebook.exit(
    json.dumps({
        "status":     "success",
        "row_count":  total_row_count,
        "partition":  OUTPUT_PATH,
        "run_date":   run_date,
        "load_type":  load_type,
    })
)
