"""
nyc311_local
============

The local DuckDB pipeline as an Airflow DAG. Every task shells out to the local
runner, so it runs end to end on a laptop with no cloud credentials:

    check_source  ->  fetch_live  ->  load_bronze  ->  load_silver
                                                            |
                        check_slos  <-  dbt_build  <--------+
                             |
                     upstream_stall_check

Scope — read this before believing the schedule
-----------------------------------------------
This is a DEMONSTRATION of orchestration, NOT the production scheduler. The
Airflow scheduler only fires while its process is alive, so on a laptop a
scheduled run is missed whenever the machine is asleep.
`.github/workflows/daily-run.yml` operates this pipeline every day (ADR 010).

catchup=False because the fetcher pulls a trailing window: backfilling missed
intervals would re-fetch the same rows, and the next run's window covers the
gap anyway.

Running it
----------
    export AIRFLOW_HOME="$(pwd)/airflow/home"
    source .venv-airflow/bin/activate
    airflow db migrate            # first time only
    airflow dags test nyc311_local        # run once, synchronously, no scheduler
    airflow standalone                    # or: full UI on localhost:8080

Airflow lives in `.venv-airflow` and the pipeline in `.venv`, because Airflow's
pins break dbt when installed together. The tasks therefore call `.venv`'s
interpreter explicitly.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, UTC

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# The repo root: NYC311_REPO_ROOT (set by scripts/airflow_local.sh), else derived
# from this file's location (airflow/dags/).
REPO_ROOT = os.environ.get(
    "NYC311_REPO_ROOT",
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
)
PIPELINE_PY = os.path.join(REPO_ROOT, ".venv", "bin", "python")
RUNNER = os.path.join(REPO_ROOT, "local", "local_runner.py")
DUCKDB = os.path.join(REPO_ROOT, "local", "data", "nyc311_local.duckdb")

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
}

with DAG(
    dag_id="nyc311_local",
    description="Local DuckDB medallion pipeline — the one that actually runs",
    default_args=default_args,
    # 06:00 UTC. Arbitrary; the daily run is GitHub Actions at 10:00 UTC.
    schedule="0 6 * * *",
    start_date=datetime(2026, 8, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["nyc311", "local", "duckdb", "demo"],
    doc_md=__doc__,
) as dag:

    # Fail fast if the city API is down. Plain curl, so no HTTP provider is needed.
    check_source = BashOperator(
        task_id="check_source",
        bash_command=(
            "curl -fsS -o /dev/null -w '%{http_code}\\n' "
            "'https://data.cityofnewyork.us/resource/erm2-nwe9.json?$limit=1'"
        ),
    )

    fetch_live = BashOperator(
        task_id="fetch_live",
        bash_command=f"{PIPELINE_PY} {RUNNER} --only 1 --live",
    )

    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=f"{PIPELINE_PY} {RUNNER} --only 2",
    )

    load_silver = BashOperator(
        task_id="load_silver",
        bash_command=f"{PIPELINE_PY} {RUNNER} --only 3",
    )

    # Builds models, snapshot, seeds and runs every dbt test in DAG order.
    # A failing test fails this task and stops the run.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{PIPELINE_PY} {RUNNER} --only 4",
    )

    # SLO-1 freshness + SLO-2 source reconciliation. Exit 1 on breach, so a
    # breach is a red DAG run.
    check_slos = BashOperator(
        task_id="check_slos",
        bash_command=(
            f"cd {REPO_ROOT} && {PIPELINE_PY} scripts/check_slos.py {DUCKDB}"
        ),
    )

    # Warning only (exits 0): a city publishing stall stays visible without
    # reddening our run, as in daily-run.yml.
    upstream_stall_check = BashOperator(
        task_id="upstream_stall_check",
        bash_command=(
            f"cd {REPO_ROOT} && {PIPELINE_PY} scripts/check_upstream_stall.py {DUCKDB}"
        ),
    )

    (
        check_source
        >> fetch_live
        >> load_bronze
        >> load_silver
        >> dbt_build
        >> check_slos
        >> upstream_stall_check
    )
