# ADR 005: Orchestration Strategy

**Status:** Accepted
**Date:** 2026-05-27

## Context

The medallion pipeline has four sequential phases that must be coordinated:
1. Source API availability check (gate before any compute starts)
2. Bronze ingestion (Databricks job)
3. Silver cleaning and Snowflake sync (Databricks job)
4. Gold dimensional model build and test (dbt CLI)

An orchestration tool is needed to: schedule the daily run, enforce task dependencies,
handle retries with configurable backoff, surface failure alerts, and provide a UI for
inspecting historical run state. The tool must have first-class support for both the
Databricks Jobs API and dbt CLI invocation, and it must be code-based (DAG-as-code)
so the pipeline definition is version-controlled alongside the transformation logic.

A specific architectural requirement shaped this decision: the pipeline must not start
if the source API (Socrata) is unavailable. An HTTP availability sensor before any
compute task is a hard requirement, not an optional enhancement. This eliminated tools
that cannot express sensor-gated dependency chains natively.

## Options Considered

### Apache Airflow

Industry-standard Python-native workflow orchestrator. DAGs are defined as Python files
in version control — the full pipeline specification, including retry policy, SLA config,
connection references, and task dependencies, is a reviewable, diffable artifact. The
`apache-airflow-providers-databricks` package provides `DatabricksRunNowOperator`, which
calls the Databricks Jobs API and polls for completion with configurable `polling_period_seconds`.
The `apache-airflow-providers-http` package provides `HttpSensor` with `mode=reschedule`
(releases the worker slot between pokes — critical for production deployments with limited
worker capacity). `BashOperator` invokes the dbt CLI cleanly, with `--profiles-dir` and
`--target prod` flags for environment isolation. Airflow's metadata database records every
task instance state, enabling full historical run inspection without external logging.
The `on_failure_callback` and `sla_miss_callback` hooks on the DAG provide a structured
path to Slack, PagerDuty, or email alerts without bespoke monitoring infrastructure.
Airflow appears in the requirements for the overwhelming majority of senior data engineering
roles — it is the most portfolio-impactful orchestration tool to demonstrate.

### Azure Data Factory (ADF)

ADF is a fully managed, GUI-driven ETL orchestration service native to Azure. It can trigger
Databricks Notebooks through the Databricks linked service and can make HTTP calls. However:

- Pipeline definitions are stored as JSON in Azure, not as code in the repository. The
  pipeline spec is not reviewable in a pull request — the only source of truth is the
  ADF portal. Recreating the pipeline in a new environment requires clicking through a GUI.
- dbt integration is limited to `BashActivity` calling the dbt CLI from a self-hosted
  integration runtime — this works but is significantly more operationally complex than
  an Airflow BashOperator and requires a dedicated compute host for the integration runtime.
- There is no equivalent of Airflow's `HttpSensor` with configurable poke interval and
  timeout. The closest ADF primitive is a `Until` loop with a `Web` activity — functional
  but verbose and not natively observable.

ADF is the correct choice for teams that are already Azure-native, do not have Python
engineering resources for Airflow DAG maintenance, and whose pipelines are primarily
Databricks or Azure-service-to-service. Rejected for this project because DAG-as-code
and the Airflow operator ecosystem are higher-priority requirements than managed
infrastructure.

### Databricks Workflows

Databricks Workflows is a native job orchestration layer inside the Databricks platform.
It supports multi-task jobs with notebook or Python script tasks, configurable retry
policies, and Databricks notification integrations. For pipelines that run entirely inside
Databricks, it is the correct zero-ops choice.

The limitation for this pipeline: dbt runs outside Databricks (it targets Snowflake, not
a Databricks SQL warehouse). Invoking dbt from a Databricks Notebook job is possible but
unnatural — dbt would need to be installed as a Databricks notebook library, the Snowflake
credentials would need to be available inside the Databricks secret scope, and the dbt
CLI invocation would be wrapped in a `%sh` magic or `subprocess` call. The result is
operational complexity comparable to Airflow but with Databricks-specific lock-in and
no Airflow operator ecosystem benefit. There is also no equivalent of `HttpSensor` in
Databricks Workflows. Rejected.

### dbt Cloud (Orchestration feature)

dbt Cloud includes a built-in scheduler that can trigger dbt jobs on a cron schedule.
For pipelines where the only orchestrated step is a dbt run, it is the simplest path.
The problems for this pipeline:

- dbt Cloud orchestrates only dbt jobs. Databricks Bronze and Silver jobs must be
  orchestrated separately (by ADF, Workflows, or another tool). This creates a
  split orchestration story with two schedulers, two monitoring UIs, and no unified
  dependency chain between the Databricks steps and the dbt step.
- The `check_api_availability` HttpSensor cannot be expressed in dbt Cloud. The gate
  task that holds the pipeline until the source is available has no equivalent in
  dbt Cloud's scheduling model.
- dbt Cloud adds a per-seat subscription cost on top of Snowflake and Databricks spend.

Rejected because split orchestration is architecturally worse than a single unified DAG.

## Decision

**Apache Airflow** with a single DAG (`nyc311_pipeline`) in `airflow/dags/nyc311_pipeline.py`.

The DAG implements the `check_api_availability >> ingest_raw >> load_bronze >> load_silver
>> dbt_build >> dbt_publish >> notify_success` dependency chain using:
- `HttpSensor` (mode=reschedule) as the pipeline gate
- `DatabricksRunNowOperator` for all three Databricks jobs, with `notebook_params`
  passing the execution date for idempotent replay
- `BashOperator` for the dbt stage, structured as write-audit-publish: `dbt build`
  (with the `audit_suffix` var) builds and tests every model in the `GOLD_AUDIT`
  schema — snapshots, models, and tests resolve in one command in DAG order — then
  the `publish_gold` run-operation atomically swaps the audited schema into `GOLD`
  (`ALTER SCHEMA ... SWAP WITH`). A failed model or test halts before publish, so
  BI consumers never see unvalidated data.
- `on_failure_callback` for structured alerting
- `sla_miss_callback` for SLA monitoring against the 2-hour completion window

### The HttpSensor gate is a deliberate architectural pattern

Starting a Databricks cluster and pulling from the Socrata API costs real money and time.
If the API is down or serving a partial dataset refresh, the Bronze partition written by
`ingest_raw` will be incomplete — and replaying it later requires re-running the full
ingestion job, not just the failed Silver or Gold step. The `check_api_availability`
sensor holding the pipeline in POKING state until the API returns a valid response is
not defensive programming — it is fail-fast at the cheapest possible point in the
dependency chain. A 5-minute sensor timeout is significantly cheaper than a 30-minute
Databricks job that produces garbage data.

## Consequences

**Airflow adds operational infrastructure.** The scheduler, webserver, and metadata
database must be deployed. At portfolio scale, the recommended setup is Astro CLI
(`astro dev start`) — a Docker Compose wrapper that spins up a local Airflow 2.x
environment with no manual configuration. The `astro` CLI is the fastest path from
zero to a running Airflow instance for development and code review.

**Production deployment path.** The natural evolution from local Astro CLI to production is:
- **MWAA (Amazon Managed Workflows for Apache Airflow)**: managed Airflow on AWS, zero ops.
  Limitation: AWS lock-in may conflict with the Azure-native data platform choice.
- **Astronomer (Astro Cloud)**: managed Airflow on any cloud, strong Databricks operator support.
  Recommended for this stack given Azure + Snowflake + Databricks footprint.
- **Self-hosted on AKS**: full control, maximum ops burden. Appropriate for large platform
  engineering teams with Kubernetes expertise.

This evolution is documented as a known future decision, not deferred ambiguity. The DAG
code is identical across all three deployment targets — only the Airflow infrastructure
changes, not the pipeline logic.

**Monitoring.** The Airflow UI provides task-level state, log access, and run history
out of the box. The `on_failure_callback` in the DAG is wired to a `slack_alert` stub —
connecting it to a real Slack webhook requires adding a single Airflow Connection
(`slack_webhook`) and uncommenting four lines in the callback function. The `sla_miss_callback`
is wired similarly. Both callbacks are version-controlled in the DAG file, not configured
through a GUI.


---

## Amendment 2026-08-20 — the cloud DAG was removed; the local DAG runs

The single-DAG, write-audit-publish design recorded here was written for a
Databricks + Snowflake deployment. That DAG (`nyc311_pipeline.py`) has been
deleted along with the Databricks path. `airflow/dags/nyc311_local.py` remains
and actually executes: the same gate-then-build-then-verify shape, seven tasks,
smoke-tested end to end. The HttpSensor reasoning, the sequencing, and the
write-audit-publish argument are unchanged and still describe the intent; only
the operators changed from Databricks jobs to the local runner's stages.
