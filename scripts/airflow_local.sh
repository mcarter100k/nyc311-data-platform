#!/usr/bin/env bash
# airflow_local.sh — start (or test) the local Airflow that orchestrates the
# DuckDB pipeline. See airflow/dags/nyc311_local.py for scope: this is a
# DEMONSTRATION of orchestration, not the production scheduler. The scheduled
# daily run remains .github/workflows/daily-run.yml (ADR 010).
#
#   ./scripts/airflow_local.sh init   — create the metadata DB (first time only)
#   ./scripts/airflow_local.sh test   — run the DAG once, synchronously, no server
#   ./scripts/airflow_local.sh ui     — full scheduler + webserver on :8080
#
# Airflow lives in its OWN virtualenv (.venv-airflow). It is kept apart from
# .venv on purpose: Airflow pins many shared dependencies and installing it
# beside dbt is a known way to break dbt. The DAG's tasks invoke .venv's
# interpreter explicitly.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export AIRFLOW_HOME="${REPO_ROOT}/airflow/home"
export AIRFLOW__CORE__DAGS_FOLDER="${REPO_ROOT}/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export NYC311_REPO_ROOT="${REPO_ROOT}"

if [ ! -x "${REPO_ROOT}/.venv-airflow/bin/airflow" ]; then
  echo "Airflow venv missing. Create it with:" >&2
  echo "  python3 -m venv .venv-airflow" >&2
  echo "  .venv-airflow/bin/pip install 'apache-airflow==3.3.1'" >&2
  exit 1
fi
# shellcheck disable=SC1091
source "${REPO_ROOT}/.venv-airflow/bin/activate"

case "${1:-test}" in
  init) airflow db migrate ;;
  test) airflow dags test nyc311_local ;;
  ui)   echo "UI at http://localhost:8080 (credentials printed below)"
        airflow standalone ;;
  *)    echo "usage: $0 {init|test|ui}" >&2; exit 2 ;;
esac
