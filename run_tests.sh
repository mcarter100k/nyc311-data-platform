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
#   - dbt parse must have been run first (populates dbt/target/manifest.json)
#   - Python venv at /tmp/dbt-venv-17 with pytest installed
#     OR: pip install pytest pyyaml

set -e

REPO_ROOT="$(cd "$(dirname "$0")" && pwd)"
PYTHON="/tmp/dbt-venv-17/bin/python3"
PYTEST="/tmp/dbt-venv-17/bin/pytest"

# Fall back to system python/pytest if venv doesn't exist
if [ ! -f "$PYTEST" ]; then
  PYTHON="python3"
  PYTEST="pytest"
fi

echo "═══════════════════════════════════════════════════════"
echo "  NYC 311 Pipeline — Structural & Behavioral Tests"
echo "═══════════════════════════════════════════════════════"
echo ""

# Step 1: Rebuild the manifest so tests always run against current code
echo "► Step 1/2: Rebuilding dbt manifest..."
cd "$REPO_ROOT/dbt"
/tmp/dbt-venv-17/bin/dbt parse \
  --profiles-dir . \
  --project-dir . \
  --target ci \
  --quiet
echo "  manifest.json refreshed."
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
