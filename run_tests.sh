#!/usr/bin/env bash
# run_tests.sh — NYC 311 Pipeline Test Runner
#
# Runs the full structural and behavioral test suite without requiring
# live cloud credentials (Snowflake).
#
# Usage:
#   ./run_tests.sh            — run all tests
#   ./run_tests.sh dbt        — run only dbt architecture tests
#   ./run_tests.sh pipeline   — run only pipeline component tests
#
# Prerequisites (the README quickstart installs exactly this):
#   pip install -r local/requirements.txt -r dbt/requirements.txt -r requirements-dev.txt
#
# dbt/requirements.txt (dbt-snowflake) is what parses the dbt/ project below.
# local/requirements.txt (dbt-duckdb) powers the behavioral tier, which skips
# cleanly when absent. requirements-dev.txt carries pytest and pyyaml.

set -e

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"

# Resolve python/pytest: prefer an active virtualenv, fall back to PATH.
PYTHON="${PYTHON:-python3}"
PYTEST="${PYTEST:-pytest}"
command -v "$PYTEST" >/dev/null 2>&1 || PYTEST="$PYTHON -m pytest"

echo "═══════════════════════════════════════════════════════"
echo "  NYC 311 Pipeline — Structural & Behavioral Tests"
echo "═══════════════════════════════════════════════════════"
echo ""

# Step 1: Rebuild the manifest so tests run against current code.
# Find dbt via local/dbt_exec.py (`python -m dbt` does not work). Without dbt,
# reuse an existing manifest with a warning, or fail: dbt/target/ is gitignored.
echo "► Step 1/2: Rebuilding dbt manifest..."
cd "$REPO_ROOT/dbt"

# `|| true` so a missing dbt does not trip `set -e` before the branches below.
DBT_BIN="$("$PYTHON" "$REPO_ROOT/local/dbt_exec.py" 2>/dev/null || true)"

# The committed, credential-free CI profile, passed as --profiles-dir so a
# developer's real (gitignored) dbt/profiles.yml is never touched.
PROFILES_DIR="$REPO_ROOT/dbt/ci-profile"

if [ -n "$DBT_BIN" ]; then
  # packages.yml pins dbt_utils, which `parse` needs resolved. dbt_packages/ is
  # gitignored, so on a clean clone it must be installed before parsing.
  if [ ! -d dbt_packages ]; then
    echo "  Installing dbt packages (dbt deps)..."
    "$DBT_BIN" deps --profiles-dir "$PROFILES_DIR" --project-dir . --quiet
  fi
  "$DBT_BIN" parse --profiles-dir "$PROFILES_DIR" --project-dir . --target ci \
    --no-partial-parse --quiet
  echo "  manifest.json refreshed."
elif [ -f target/manifest.json ]; then
  echo "  WARNING: dbt is not installed for $PYTHON — reusing existing"
  echo "  target/manifest.json, which may be stale relative to the models."
else
  echo "  ERROR: dbt is not installed and dbt/target/manifest.json does not exist."
  echo "  The architecture tests need a compiled manifest. Install dbt first:"
  echo "      pip install -r dbt/requirements.txt"
  exit 1
fi
echo ""

# Step 2: Run pytest
echo "► Step 2/2: Running test suite..."
cd "$REPO_ROOT"

FILTER="${1:-}"

case "$FILTER" in
  dbt)
    $PYTEST tests/test_dbt_architecture.py -v
    ;;
  pipeline)
    $PYTEST tests/test_pipeline_components.py -v
    ;;
  *)
    $PYTEST tests/ -v
    ;;
esac
