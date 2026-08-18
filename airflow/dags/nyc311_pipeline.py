"""
nyc311_pipeline
===============

Orchestrates the full medallion ETL pipeline for NYC 311 service request data,
from source API availability check through dimensional model delivery in Snowflake.

Pipeline architecture
---------------------

  NYC Open Data (Socrata API — data.cityofnewyork.us)
           │
   ┌───────▼──────────────────────────────────────────────┐
   │  check_api_availability  (HttpSensor)                 │
   │  Gate task. If the API returns non-200, the entire    │
   │  pipeline stays in POKING state and reschedules.      │
   │  Nothing downstream runs until the source is up.      │
   └───────┬──────────────────────────────────────────────┘
           │
   ┌───────▼──────────────────────────────────────────────┐
   │  ingest_raw  (DatabricksRunNowOperator)               │
   │  Pulls raw JSON from Socrata API for execution date,  │
   │  writes to ADLS Bronze partition (year=/month=/day=). │
   └───────┬──────────────────────────────────────────────┘
           │
   ┌───────▼──────────────────────────────────────────────┐
   │  load_bronze  (DatabricksRunNowOperator)              │
   │  Registers the raw JSON as a Delta table in the       │
   │  Bronze schema. Schema-on-read; no type coercion.     │
   └───────┬──────────────────────────────────────────────┘
           │
   ┌───────▼──────────────────────────────────────────────┐
   │  load_silver  (DatabricksRunNowOperator)              │
   │  PySpark cleaning: deduplication on unique_key,       │
   │  type casting, borough standardization, partition     │
   │  pruning. Writes to Silver Delta; syncs to Snowflake. │
   └───────┬──────────────────────────────────────────────┘
           │
   ┌───────▼──────────────────────────────────────────────┐
   │  dbt_build  (BashOperator)     — WRITE + AUDIT        │
   │  dbt build with audit_suffix=_audit: snapshots,       │
   │  models, and tests run in DAG order (snapshot →       │
   │  staging → intermediate → marts), all writing to      │
   │  GOLD_AUDIT. Each model's tests run right after it    │
   │  builds; a failure stops downstream models and the    │
   │  task. GOLD is never touched by this step.            │
   └───────┬──────────────────────────────────────────────┘
           │
   ┌───────▼──────────────────────────────────────────────┐
   │  dbt_publish  (BashOperator)   — PUBLISH              │
   │  dbt run-operation publish_gold: atomically swaps     │
   │  the fully-audited GOLD_AUDIT schema into GOLD        │
   │  (ALTER SCHEMA ... SWAP WITH). Only runs if every     │
   │  model built and every test passed — BI consumers     │
   │  never see unvalidated data.                          │
   └───────┬──────────────────────────────────────────────┘
           │
   ┌───────▼──────────────────────────────────────────────┐
   │  notify_success  (BashOperator)                       │
   │  Logs completion with execution date and row counts.  │
   │  Only fires if every upstream task succeeded.         │
   └──────────────────────────────────────────────────────┘

Schedule:   0 6 * * *   (06:00 UTC daily — after NYC midnight data cutoff)
SLA:        Pipeline must complete by 08:00 UTC (sla=timedelta(hours=2) on notify_success).
Owner:      data-engineering
Catchup:    False — backfills must be triggered manually per-date.

Airflow prerequisites
---------------------
Connections (Admin → Connections):
  databricks_default   Databricks workspace URL + personal access token (PAT)
  socrata_api          HTTP connection: host=data.cityofnewyork.us, schema=https
  slack_webhook        HTTP connection: host=hooks.slack.com (optional; see slack_alert())

Variables (Admin → Variables or `airflow variables set <key> <value>`):
  DATABRICKS_INGEST_JOB_ID   Integer Databricks job ID for the raw ingest notebook
  DATABRICKS_BRONZE_JOB_ID   Integer Databricks job ID for the Bronze Delta job
  DATABRICKS_SILVER_JOB_ID   Integer Databricks job ID for the Silver PySpark job
  ALERT_EMAIL                Comma-separated recipient list for failure emails
  LAST_INGEST_ROW_COUNT      (Set by ingest_raw job) Row count for notify_success logging
"""

# ── Standard library ──────────────────────────────────────────────────────────
import logging
from datetime import datetime, timedelta

# ── Airflow core ──────────────────────────────────────────────────────────────
from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# ── Airflow operators ─────────────────────────────────────────────────────────
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# ── Airflow sensors ───────────────────────────────────────────────────────────
from airflow.providers.http.sensors.http import HttpSensor


log = logging.getLogger(__name__)


# ── Constants — pulled from Airflow Variables at parse time ───────────────────
# All secret and environment-specific values live in Airflow Admin → Variables.
# Never hardcode job IDs, credentials, or email addresses in this file.
# If a Variable is not set, default_var prevents an ImportError at DAG parse time;
# the pipeline will fail at runtime with a descriptive error from the operator.

DATABRICKS_CONN_ID       = "databricks_default"
SOCRATA_CONN_ID          = "socrata_api"

DATABRICKS_INGEST_JOB_ID = int(Variable.get("DATABRICKS_INGEST_JOB_ID", default_var=0))
DATABRICKS_BRONZE_JOB_ID = int(Variable.get("DATABRICKS_BRONZE_JOB_ID", default_var=0))
DATABRICKS_SILVER_JOB_ID = int(Variable.get("DATABRICKS_SILVER_JOB_ID", default_var=0))
ALERT_EMAIL               = Variable.get("ALERT_EMAIL", default_var="data-team@example.com")

# dbt invocation — all dbt commands use the same profile/project directory path.
# The profiles.yml inside this directory must have a `prod` target configured
# with TRANSFORMER role credentials (see dbt/profiles.yml.example).
_DBT_FLAGS = (
    "--profiles-dir /opt/airflow/dbt "
    "--project-dir /opt/airflow/dbt "
    "--target prod"
)

# Write-audit-publish: the build step routes every model into GOLD_AUDIT via
# the audit_suffix var (see dbt/macros/generate_schema_name.sql). Snapshots are
# exempt — SCD2 state lives in one schema regardless of the suffix.
# Single braces are safe here: Airflow only templates double-brace {{ }} fields.
_AUDIT_VARS = '--vars \'{"audit_suffix": "_audit"}\''


# ── Callbacks ─────────────────────────────────────────────────────────────────

def slack_alert(context: dict) -> None:
    """
    Called by Airflow when any task in this DAG transitions to FAILED state.

    To wire a real Slack alert:
      1. Create an Airflow HTTP Connection named 'slack_webhook':
           Host:   hooks.slack.com
           Schema: https
           Extra:  {"webhook_token": "/services/T.../B.../..."}
      2. Uncomment the requests.post block below.
      3. Ensure the 'requests' package is available in the Airflow environment.

    The context dict contains: dag, task_instance, execution_date, exception,
    log_url, and other task metadata.
    """
    task_instance = context.get("task_instance")
    dag_id        = context.get("dag").dag_id
    task_id       = task_instance.task_id if task_instance else "unknown"
    exec_date     = context.get("execution_date")
    log_url       = context.get("task_instance").log_url if task_instance else ""

    message = (
        f":red_circle: *NYC 311 Pipeline Failure*\n"
        f"*DAG:* `{dag_id}`\n"
        f"*Task:* `{task_id}`\n"
        f"*Execution date:* {exec_date}\n"
        f"*Log:* {log_url}"
    )

    log.error("Pipeline failure alert: dag=%s task=%s exec_date=%s", dag_id, task_id, exec_date)

    # Uncomment to send to Slack once the webhook connection is configured:
    # import requests
    # conn = BaseHook.get_connection("slack_webhook")
    # webhook_url = f"https://{conn.host}{conn.extra_dejson.get('webhook_token', '')}"
    # requests.post(webhook_url, json={"text": message}, timeout=10)


def sla_miss_alert(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """
    Called by Airflow when a task misses its SLA window.

    The SLA for this pipeline is 2 hours from the 06:00 UTC scheduled start
    (i.e. the full pipeline must complete by 08:00 UTC). SLA misses are
    recorded in the Airflow metadata database and surfaced in the Browse →
    SLA Misses UI.

    Wire a real notification here (PagerDuty, Slack, email) following the
    same pattern as slack_alert() above. `task_list` contains the tasks that
    missed their SLA; `blocking_task_list` contains tasks that are blocking
    the SLA-missing tasks.
    """
    missed_tasks = ", ".join(str(t) for t in task_list) if task_list else "unknown"
    log.warning(
        "SLA MISS on DAG '%s'. Tasks that missed SLA: %s",
        dag.dag_id,
        missed_tasks,
    )


# ── Default task arguments ────────────────────────────────────────────────────

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email":            [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry":   False,
    "start_date":       datetime(2024, 1, 1),
}


# ── DAG-level documentation (rendered in Airflow UI → DAG details) ────────────

_DAG_DOC = """\
## NYC 311 Medallion Pipeline

Orchestrates daily ingestion of NYC 311 service request data through the full
medallion architecture: Bronze (raw) → Silver (cleaned) → Gold (dimensional model).

### Schedule
Runs at **06:00 UTC daily**. NYC Open Data updates at approximately 00:00–02:00 ET
(05:00–07:00 UTC), so the 06:00 start gives a 1–4 hour buffer for upstream latency.

### SLA
The full pipeline must complete within **2 hours** (by 08:00 UTC). Late completion
triggers the `sla_miss_callback` and should be investigated if it persists.

### Layer contracts
| Layer  | Tool        | Responsibility                              |
|--------|-------------|---------------------------------------------|
| Bronze | Databricks  | Raw JSON write to ADLS; no transformation   |
| Silver | Databricks  | Dedup, type cast, borough standardization   |
| Gold   | dbt         | Dimensional model; schema-tested SQL        |

### Failure policy
All tasks retry 2× with a 5-minute delay. The `on_failure_callback` logs a
structured error and (when wired) posts a Slack alert with the task log URL.
Rerun a failed DAG run from the failed task using the Airflow UI
**Clear → Current task only** — upstream Databricks jobs are idempotent on
re-execution for the same date.
"""


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id              = "nyc311_pipeline",
    description         = "Daily medallion ETL: Socrata API → Bronze → Silver → dbt Gold",
    schedule_interval   = "0 6 * * *",
    default_args        = default_args,
    catchup             = False,
    max_active_runs     = 1,
    tags                = ["nyc311", "medallion", "production"],
    doc_md              = _DAG_DOC,
    on_failure_callback = slack_alert,
    sla_miss_callback   = sla_miss_alert,
) as dag:

    # ── Task 1: API availability gate ─────────────────────────────────────────

    check_api_availability = HttpSensor(
        task_id         = "check_api_availability",
        http_conn_id    = SOCRATA_CONN_ID,
        endpoint        = "/resource/erm2-nwe9.json?$limit=1",
        method          = "GET",
        # Accept the response only if the API returns HTTP 200 with at least
        # one record. An empty response may indicate a partial dataset refresh.
        response_check  = lambda response: (
            response.status_code == 200
            and len(response.json()) > 0
        ),
        poke_interval   = 60,    # seconds between probes
        timeout         = 300,   # total seconds before FAILED (5 probes max)
        mode            = "reschedule",  # releases worker slot between pokes
        doc_md          = """\
## check_api_availability

Probes the NYC Open Data Socrata API for the 311 service request dataset
(`erm2-nwe9`) before any downstream work begins.

**Why this task exists:** Starting an ingestion run against an unavailable
or partially-refreshed source produces incomplete Bronze partitions that
must be manually replayed. Failing fast here is cheaper than a partial load.

**Failure means:** The Socrata API is down or returning unexpected responses.
Check [status.data.cityofnewyork.us](https://status.data.cityofnewyork.us)
and the Airflow task log for the HTTP status code.
The sensor will retry for up to 300 seconds before marking the task as FAILED,
which triggers the on_failure_callback.

**Connection:** `socrata_api` (Admin → Connections)
Host: `data.cityofnewyork.us`, Schema: `https`
""",
    )

    # ── Task 2: Raw ingest to Bronze ─────────────────────────────────────────

    ingest_raw = DatabricksRunNowOperator(
        task_id                = "ingest_raw",
        databricks_conn_id     = DATABRICKS_CONN_ID,
        job_id                 = DATABRICKS_INGEST_JOB_ID,
        # Execution date is passed as notebook parameters so the job processes
        # exactly the correct day and the run is idempotent on retry.
        notebook_params        = {
            "run_date":     "{{ ds }}",
            "force_reload": "false",
        },
        polling_period_seconds = 30,
        doc_md                 = """\
## ingest_raw

Triggers the Databricks **raw ingest** job (notebook `01_ingest_raw.py`).
Pulls NYC 311 service request records for the execution date from the
Socrata API and writes them as raw JSON to the ADLS Bronze partition
`year=YYYY/month=MM/day=DD`. Skips the partition if it already exists
(idempotent unless `force_reload=true`).

**Failure means:** The Databricks job failed — check the run log in the
Databricks Jobs UI (linked in the Airflow task log). Common causes:
ADLS permission errors, Socrata API rate limiting, or cluster bootstrap
failures.

**Variable required:** `DATABRICKS_INGEST_JOB_ID` (integer job ID)
**Connection required:** `databricks_default`
""",
    )

    # ── Task 3: Register Bronze Delta table ───────────────────────────────────

    load_bronze = DatabricksRunNowOperator(
        task_id                = "load_bronze",
        databricks_conn_id     = DATABRICKS_CONN_ID,
        job_id                 = DATABRICKS_BRONZE_JOB_ID,
        notebook_params        = {"execution_date": "{{ ds }}"},
        polling_period_seconds = 30,
        doc_md                 = """\
## load_bronze

Triggers the Databricks **Bronze** job (notebook `02_bronze.py`).
Registers the raw JSON files written by `ingest_raw` as a partitioned
Delta table in the Bronze schema. No type coercion or cleaning occurs —
Bronze is append-only and schema-on-read.

**Failure means:** The Delta registration failed. Likely cause: the
`ingest_raw` partition for this date is missing or malformed. Verify
that `ingest_raw` wrote the expected files to ADLS before rerunning.

**Variable required:** `DATABRICKS_BRONZE_JOB_ID`
""",
    )

    # ── Task 4: Silver cleaning and sync ──────────────────────────────────────

    load_silver = DatabricksRunNowOperator(
        task_id                = "load_silver",
        databricks_conn_id     = DATABRICKS_CONN_ID,
        job_id                 = DATABRICKS_SILVER_JOB_ID,
        notebook_params        = {"execution_date": "{{ ds }}"},
        polling_period_seconds = 30,
        doc_md                 = """\
## load_silver

Triggers the Databricks **Silver** job (notebook `03_silver.py`).
Reads the Bronze Delta partition for this date and applies:
deduplication on `unique_key`, type casting, borough name standardization,
and null handling. Writes the result to the Silver Delta table and syncs
to the Snowflake SILVER schema so dbt can read it in the next step.

**Failure means:** A data quality issue in Bronze that Silver cleaning
cannot handle (e.g. a new complaint type that breaks a schema constraint),
or a Snowflake sync failure. Investigate the Databricks run log.

**Variable required:** `DATABRICKS_SILVER_JOB_ID`
""",
    )

    # ── Task 5: dbt build — the WRITE + AUDIT half of write-audit-publish ─────
    # One `dbt build` resolves the whole DAG in dependency order: the
    # agency_snapshot (SCD Type 2) runs before dim_agency, each model's tests
    # run immediately after the model builds, and a failure skips everything
    # downstream. Crucially, the audit_suffix var routes every model into
    # GOLD_AUDIT — production GOLD is untouched until dbt_publish swaps the
    # audited schema in. Testing after publishing (the previous design) meant
    # bad data was already live in GOLD by the time a test caught it.

    dbt_build = BashOperator(
        task_id      = "dbt_build",
        bash_command = f"dbt build {_DBT_FLAGS} {_AUDIT_VARS}",
        doc_md       = """\
## dbt_build

Write + audit step. Runs `dbt build` with `audit_suffix=_audit`, which:

1. Resolves the full DAG in one command — `agency_snapshot` (SCD Type 2)
   runs before `dim_agency`, then `stg_service_requests` →
   `int_service_requests_cleaned` → dims → `fct_service_requests` →
   `fct_daily_volume`.
2. Builds every model into **GOLD_AUDIT** (see
   `dbt/macros/generate_schema_name.sql`). Snapshots are exempt from the
   suffix — SCD2 state lives in one schema across all runs.
3. Runs each model's tests immediately after the model builds. A model or
   test failure halts everything downstream of it and fails this task, so
   nothing unvalidated ever reaches the publish step.

Reads from Snowflake SILVER schema. Uses the `prod` dbt target
(TRANSFORMER role, NYC311_WH warehouse).

**Failure means:** either broken SQL (compile/execute error) or a data
contract violation (test failure) — the dbt log names the failing node
either way. In both cases production GOLD still holds the last published
build; no remediation urgency for BI consumers.

**Incremental note:** GOLD_AUDIT always holds the previous production
build (the publish swap ping-pongs the schemas), so the incremental
watermark in `fct_service_requests` picks up everything Silver touched
since that state. The merge is idempotent on `service_request_id`.
""",
    )

    # ── Task 6: dbt publish — the PUBLISH half of write-audit-publish ─────────

    dbt_publish = BashOperator(
        task_id      = "dbt_publish",
        bash_command = f"dbt run-operation publish_gold {_DBT_FLAGS}",
        doc_md       = """\
## dbt_publish

Publish step. Runs the `publish_gold` run-operation
(`dbt/macros/publish_gold.sql`), which executes:

    ALTER SCHEMA GOLD_AUDIT SWAP WITH GOLD

— a single atomic metadata operation in Snowflake. BI consumers switch
from the previous build to the fully-audited new build instantaneously;
there is no window where GOLD holds untested data.

Only reachable when `dbt_build` succeeded, i.e. every model built and
every declared test passed against GOLD_AUDIT.

**Failure means:** a Snowflake permission or metadata error (e.g. the
TRANSFORMER role lacks OWNERSHIP of one of the schemas). Data is safe:
GOLD still serves the previous published build. Fix the grant and clear
this task — GOLD_AUDIT still holds the validated build, so no rebuild is
needed.
""",
    )

    # ── Task 8: Success notification ──────────────────────────────────────────

    notify_success = BashOperator(
        task_id      = "notify_success",
        bash_command = (
            "echo 'NYC 311 pipeline completed successfully. "
            "Execution date: {{ ds }}. "
            "DAG run ID: {{ run_id }}. "
            "Row count (from Variable): "
            "{{ var.value.get(\"LAST_INGEST_ROW_COUNT\", \"not set\") }}.'"
        ),
        trigger_rule = TriggerRule.ALL_SUCCESS,
        # SLA: the full pipeline (including this final task) must complete
        # within 2 hours of the 06:00 UTC scheduled start. Airflow measures
        # the SLA from data_interval_start, so the deadline is 08:00 UTC.
        sla          = timedelta(hours=2),
        doc_md       = """\
## notify_success

Final task — logs a structured completion message with the execution date
and row count (set by the ingest_raw Databricks job in Variable
`LAST_INGEST_ROW_COUNT`).

Only fires when **all upstream tasks** succeeded (`TriggerRule.ALL_SUCCESS`).
If any upstream task is skipped, failed, or upstream-failed, this task
is skipped automatically.

**SLA:** This task carries the 2-hour SLA. If it does not complete by
08:00 UTC, `sla_miss_callback` fires. Persistent SLA misses indicate
the pipeline is too slow for the window — investigate warehouse cold-start
time or Databricks cluster startup latency.

**Extend this task** to send a real notification (Slack, PagerDuty,
email) by replacing the echo with a curl call or by adding a
`SlackWebhookOperator` as a parallel downstream task.
""",
    )

    # ── Dependency chain ──────────────────────────────────────────────────────

    (
        check_api_availability
        >> ingest_raw
        >> load_bronze
        >> load_silver
        >> dbt_build
        >> dbt_publish
        >> notify_success
    )
