#!/usr/bin/env bash
# run_tests.sh — NYC 311 Pipeline Test Runner
#
# Runs the full structural and behavioral test suite without requiring
# live cloud credentials (Snowflake, Databricks, Azure).
#
# Usage:
#   ./run_tests.sh            — run all tests
#   ./run_tests.sh dbt        — run only dbt architecture tests
#   ./run_tests.sh pipeline   — run only pipeline component tests
#
# Prerequisites:
#   pip install -r dbt/requirements.txt pytest pyyaml
#   Optional: pyspark (Silver unit tests), dbt-duckdb (local gold tests) —
#   both tiers skip cleanly when their dependency is absent.

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
# dbt is resolved the same way as pytest: import check on the current python.
# Failure policy (deliberate): if dbt is unavailable and no manifest exists,
# FAIL LOUDLY with the reason — there is no committed manifest to fall back to
# (dbt/target/ is gitignored). If a previously built manifest exists, reuse it
# with a staleness warning rather than blocking the run.
echo "► Step 1/2: Rebuilding dbt manifest..."
cd "$REPO_ROOT/dbt"
if "$PYTHON" -c "import dbt.cli.main" >/dev/null 2>&1; then
  "$PYTHON" -m dbt parse --profiles-dir . --project-dir . --target ci --quiet
  echo "  manifest.json refreshed."
elif [ -f target/manifest.json ]; then
  echo "  WARNING: dbt is not importable by $PYTHON — reusing existing"
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
