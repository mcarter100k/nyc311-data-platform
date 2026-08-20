"""
nyc311_local
============

Orchestrates the pipeline that ACTUALLY RUNS — the local DuckDB one — as a real
Airflow DAG.

What this DAG is
----------------
The medallion pipeline as a real Airflow DAG. A cloud counterpart
(`nyc311_pipeline.py`, Databricks operators) previously sat beside it as an
unexecutable specification; it was removed rather than carried as a claim
nothing could verify (ADR 005, ADR 008). Every task here shells out to the
local runner, so it runs end to end on a laptop with no cloud credentials:

    check_source  ->  fetch_live  ->  load_bronze  ->  load_silver
                                                            |
                        check_slos  <-  dbt_build  <--------+
                             |
                     upstream_stall_check

Scope — read this before believing the schedule
-----------------------------------------------
This is a DEMONSTRATION of orchestration, NOT the production scheduler. The
Airflow scheduler only fires while its process is alive, so on a laptop a 06:00
run is missed whenever the machine is asleep. `.github/workflows/daily-run.yml`
remains the thing that actually operates this pipeline every day (ADR 010).

catchup=False is deliberate and not merely conventional: the fetcher pulls a
TRAILING 7-DAY window, so backfilling three missed intervals would re-fetch the
same rows three times. Missed runs are simply missed; the next run's window
covers the gap anyway.

Running it
----------
    export AIRFLOW_HOME="$(pwd)/airflow/home"
    source .venv-airflow/bin/activate
    airflow db migrate            # first time only
    airflow dags test nyc311_local        # run once, synchronously, no scheduler
    airflow standalone                    # or: full UI on localhost:8080

Note the two virtualenvs. Airflow lives in `.venv-airflow`; the pipeline lives
in `.venv`. They are kept apart on purpose — Airflow pins many shared
dependencies and installing it alongside dbt is a known way to break dbt. The
tasks below therefore invoke `.venv`'s interpreter explicitly rather than
whatever python happens to be on PATH.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ── Paths ─────────────────────────────────────────────────────────────────────
# AIRFLOW_HOME is <repo>/airflow/home, so the repo root is two levels up. Falls
# back to the DAG file's own location, which is how `airflow dags test` and the
# scheduler both resolve it.
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
    # 06:00 UTC, matching the cloud spec's cadence. See the scope note above:
    # on a laptop this fires only while the scheduler process is running.
    schedule="0 6 * * *",
    # Explicitly UTC. Airflow assumes UTC for a naive datetime, so this changes
    # nothing today — it states the assumption the `schedule` above depends on
    # rather than inheriting it.
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["nyc311", "local", "duckdb", "demo"],
    doc_md=__doc__,
) as dag:

    # Gate. Mirrors the cloud DAG's HttpSensor: if NYC's API is not answering,
    # fail here rather than part-way through a load. Kept as a plain curl so the
    # DAG needs no HTTP provider or Airflow Connection to run.
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

    # WARNING path, never a gate: exits 0 either way. Mirrors daily-run.yml —
    # a city publishing stall must stay visible without reddening our run.
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
