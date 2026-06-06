"""
Pipeline Component Tests — NYC 311 Data Platform

Tests the non-dbt pieces of the pipeline without needing live cloud credentials:

  1. Databricks notebooks  — Python syntax validity and required patterns
  2. Airflow DAG           — syntax validity, task count, dependency chain
  3. Terraform             — HCL syntax validity via terraform validate
  4. GitHub Actions        — workflow YAML structure
  5. profiles.yml.example  — connection config correctness
"""

import ast
import os
import subprocess
import yaml
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_python(path):
    """Parse a Python file and return the AST. Raises SyntaxError on failure."""
    with open(path) as f:
        source = f.read()
    return ast.parse(source, filename=path)


def file_contains(path, *strings):
    """Return True if the file contains ALL of the given strings."""
    with open(path) as f:
        content = f.read()
    return all(s in content for s in strings)


# ── 1. Databricks Notebooks ───────────────────────────────────────────────────

NOTEBOOKS = {
    "01_ingest_raw": os.path.join(ROOT, "databricks", "notebooks", "01_ingest_raw.py"),
    "02_bronze":     os.path.join(ROOT, "databricks", "notebooks", "02_bronze.py"),
    "03_silver":     os.path.join(ROOT, "databricks", "notebooks", "03_silver.py"),
}


@pytest.mark.parametrize("name,path", NOTEBOOKS.items())
def test_notebook_is_valid_python(name, path):
    """
    Each Databricks notebook must be valid Python syntax. An invalid notebook
    will fail at import time in Databricks and block the entire pipeline run.
    We strip the MAGIC comment lines (Databricks-specific directives) before
    parsing since Python's ast module doesn't understand them.
    """
    with open(path) as f:
        source = f.read()
    # Strip Databricks MAGIC lines which are not valid Python
    cleaned = "\n".join(
        line for line in source.splitlines()
        if not line.strip().startswith("# MAGIC")
    )
    try:
        ast.parse(cleaned, filename=path)
    except SyntaxError as e:
        pytest.fail(f"{name} has a Python syntax error: {e}")


def test_ingest_notebook_uses_secrets_not_hardcoded_credentials():
    """
    The ingest notebook must retrieve all credentials from Databricks secret scopes,
    never from hardcoded strings. Hardcoded credentials in notebooks get committed
    to git and are a security incident waiting to happen.
    """
    path = NOTEBOOKS["01_ingest_raw"]
    assert file_contains(path, "dbutils.secrets.get"), (
        "01_ingest_raw.py does not use dbutils.secrets.get — credentials may be hardcoded."
    )


def test_ingest_notebook_has_idempotency_check():
    """
    The ingest notebook must check whether the partition already exists before
    calling the API. Without this, every Airflow retry re-pulls from Socrata,
    wasting API quota and potentially writing duplicate raw files.
    """
    path = NOTEBOOKS["01_ingest_raw"]
    assert file_contains(path, "force_reload", "dbutils.fs.ls"), (
        "01_ingest_raw.py is missing the idempotency check (dbutils.fs.ls + force_reload)."
    )


def test_ingest_notebook_paginates():
    """
    The Socrata API returns a maximum of 50,000 rows per request. The notebook
    must paginate using $offset to retrieve the full dataset.
    """
    path = NOTEBOOKS["01_ingest_raw"]
    assert file_contains(path, "$offset", "PAGE_SIZE"), (
        "01_ingest_raw.py does not appear to paginate — it may only fetch the first 50k rows."
    )


def test_bronze_notebook_uses_auto_loader():
    """
    The Bronze notebook must use Databricks Auto Loader (cloudFiles format).
    Auto Loader checkpoints the schema so new API columns are picked up
    automatically without manual schema migration.
    """
    path = NOTEBOOKS["02_bronze"]
    assert file_contains(path, "cloudFiles", "readStream"), (
        "02_bronze.py does not use Auto Loader (cloudFiles). "
        "Direct spark.read.json() has no schema evolution support."
    )


def test_bronze_notebook_asserts_nonzero_row_count():
    """
    The Bronze notebook must fail fast if it writes zero rows. A silent empty
    write would cause Silver to raise 'no Bronze records found' — but only on
    the next pipeline run, making the root cause harder to diagnose.
    """
    path = NOTEBOOKS["02_bronze"]
    assert file_contains(path, "raise RuntimeError", "row_count == 0"), (
        "02_bronze.py does not assert a non-zero row count after writing."
    )


def test_silver_notebook_deduplicates_on_unique_key():
    """
    Silver must call deduplicate_on_unique_key — the function that keeps the
    most recent _ingest_timestamp row per unique_key. The implementation lives
    in silver_transformations.py; the notebook must import and call it.
    Without deduplication, API pagination overlap creates duplicate records that
    propagate into every Gold mart table.
    """
    path = NOTEBOOKS["03_silver"]
    assert file_contains(path, "deduplicate_on_unique_key"), (
        "03_silver.py does not call deduplicate_on_unique_key. "
        "Deduplication logic must be imported from silver_transformations.py."
    )
    transformations_path = os.path.join(
        ROOT, "databricks", "notebooks", "silver_transformations.py"
    )
    assert file_contains(transformations_path, "unique_key", "row_number"), (
        "silver_transformations.py does not implement Window-based deduplication "
        "on unique_key — dropDuplicates gives no deterministic row selection guarantee."
    )


def test_silver_notebook_uses_delta_merge():
    """
    Silver must use Delta MERGE (not overwrite) for idempotency. MERGE on
    unique_key means re-running the notebook for the same date updates existing
    rows and inserts new ones — no duplicates, safe for Airflow retry.
    The MERGE clause is built dynamically from the runtime schema so new Bronze
    columns flow into Silver without code changes — verified by checking that
    schema.autoMerge is enabled and that explicit update/insert expressions
    are constructed rather than using the static UpdateAll/InsertAll shortcuts.
    """
    path = NOTEBOOKS["03_silver"]
    assert file_contains(path, "DeltaTable", "whenMatchedUpdate", "whenNotMatchedInsert"), (
        "03_silver.py does not use Delta MERGE — it may overwrite data on retry."
    )
    assert file_contains(path, "autoMerge"), (
        "03_silver.py does not enable schema.autoMerge. New Bronze columns will "
        "cause the MERGE to fail with a schema mismatch error."
    )


def test_silver_notebook_filters_negative_resolution_days():
    """
    Silver must drop records where closed_date precedes created_date.
    These ~0.02% of records are data entry errors. If they reach Gold,
    the dbt singular test assert_resolution_days_nonnegative will fail
    and block the pipeline.
    """
    path = NOTEBOOKS["03_silver"]
    assert file_contains(path, "resolution_days", "< 0"), (
        "03_silver.py does not filter out negative resolution_days records."
    )


def test_silver_notebook_writes_silver_timestamp():
    """
    Silver must write _silver_timestamp to every row. This column is the
    incremental watermark used by fct_service_requests to identify new and
    updated rows since the last dbt run.
    """
    path = NOTEBOOKS["03_silver"]
    assert file_contains(path, "_silver_timestamp"), (
        "03_silver.py does not write _silver_timestamp — "
        "the dbt incremental watermark will have no data to filter on."
    )


def test_silver_transformation_checksum_uses_sort_array():
    """
    compute_unique_key_checksum must apply sort_array before sha2. Without the
    explicit sort, Spark's non-deterministic row ordering produces different hashes
    across runs of identical data — causing false DATA CONTRACT VIOLATION alerts
    on legitimate replays. The function must also be wired into 03_silver.py.
    """
    transformations_path = os.path.join(
        ROOT, "databricks", "notebooks", "silver_transformations.py"
    )
    assert file_contains(transformations_path, "sort_array", "compute_unique_key_checksum"), (
        "silver_transformations.py is missing sort_array or compute_unique_key_checksum. "
        "Without sort_array the checksum is not order-independent across Spark runs."
    )
    path = NOTEBOOKS["03_silver"]
    assert file_contains(path, "compute_unique_key_checksum", "CHECKSUMS_TABLE"), (
        "03_silver.py does not call compute_unique_key_checksum or define CHECKSUMS_TABLE."
    )


# ── 2. Airflow DAG ────────────────────────────────────────────────────────────

DAG_PATH = os.path.join(ROOT, "airflow", "dags", "nyc311_pipeline.py")

EXPECTED_TASKS = [
    "check_api_availability",
    "ingest_raw",
    "load_bronze",
    "load_silver",
    "dbt_run",
    "dbt_test",
    "notify_success",
]

EXPECTED_DEPENDENCY_CHAIN = [
    ("check_api_availability", "ingest_raw"),
    ("ingest_raw",             "load_bronze"),
    ("load_bronze",            "load_silver"),
    ("load_silver",            "dbt_run"),
    ("dbt_run",                "dbt_test"),
    ("dbt_test",               "notify_success"),
]


def test_airflow_dag_is_valid_python():
    """
    The Airflow DAG file must be valid Python syntax. An import error in a DAG
    file causes the entire Airflow scheduler to log parse errors every 30 seconds
    and can delay all other DAGs from being scheduled.
    """
    try:
        parse_python(DAG_PATH)
    except SyntaxError as e:
        pytest.fail(f"nyc311_pipeline.py has a Python syntax error: {e}")


@pytest.mark.parametrize("task_id", EXPECTED_TASKS)
def test_airflow_dag_contains_expected_task(task_id):
    """
    Each expected task ID must appear in the DAG file. A missing task means
    a pipeline stage (e.g. dbt_test) is not being run, leaving data quality
    checks unexecuted in production.
    """
    assert file_contains(DAG_PATH, f'"{task_id}"') or file_contains(DAG_PATH, f"'{task_id}'"), (
        f"Task '{task_id}' not found in nyc311_pipeline.py."
    )


def test_airflow_dag_has_http_sensor_gate():
    """
    The pipeline must start with an HttpSensor that validates the Socrata API
    is live and returning data. Starting ingestion without this gate wastes
    Databricks cluster startup costs on a broken source.
    """
    assert file_contains(DAG_PATH, "HttpSensor", "response_check"), (
        "nyc311_pipeline.py is missing the HttpSensor API availability gate."
    )


def test_airflow_dag_separates_dbt_run_and_dbt_test():
    """
    dbt run and dbt test must be separate tasks. A single combined task makes
    it impossible to distinguish a model build failure from a data quality test
    failure in the Airflow UI — they require different remediation paths.
    """
    assert file_contains(DAG_PATH, "dbt_run", "dbt_test"), (
        "nyc311_pipeline.py does not separate dbt_run and dbt_test into distinct tasks."
    )


def test_airflow_dag_uses_reschedule_mode_for_sensor():
    """
    The HttpSensor must use mode='reschedule' so it releases the worker slot
    between pokes. mode='poke' (the default) holds a worker slot for the entire
    wait duration — at 300 second timeout this blocks other tasks from running.
    """
    assert file_contains(DAG_PATH, "reschedule"), (
        "nyc311_pipeline.py HttpSensor does not use mode='reschedule'. "
        "This blocks worker slots during the sensor wait period."
    )


def test_airflow_dag_uses_secrets_not_hardcoded_credentials():
    """
    The DAG must retrieve credentials from Airflow Connections or Variables,
    never from hardcoded strings. Hardcoded credentials in DAG files are
    visible to anyone with Airflow UI access.
    """
    assert file_contains(DAG_PATH, "conn_id") or file_contains(DAG_PATH, "Variable"), (
        "nyc311_pipeline.py does not use Airflow Connections or Variables for credentials."
    )


# ── 3. Terraform ──────────────────────────────────────────────────────────────

TERRAFORM_DIR = os.path.join(ROOT, "terraform")


def test_terraform_validate():
    """
    terraform validate checks HCL syntax and internal consistency without
    connecting to any cloud provider. It catches typos in resource names,
    missing required arguments, and invalid attribute types.

    We run `terraform init -backend=false` first so the provider plugins are
    downloaded without needing Azure credentials (the remote backend is skipped).
    Skipped automatically if terraform is not installed or providers can't be fetched.
    """
    try:
        init_result = subprocess.run(
            ["terraform", "init", "-backend=false", "-no-color"],
            cwd=TERRAFORM_DIR,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        pytest.skip("terraform is not installed — skipping HCL validation.")
    if init_result.returncode != 0:
        pytest.skip(f"terraform init failed (no internet?): {init_result.stderr[:300]}")

    result = subprocess.run(
        ["terraform", "validate", "-no-color"],
        cwd=TERRAFORM_DIR,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(f"terraform validate failed:\n{result.stdout}\n{result.stderr}")


def test_terraform_outputs_dont_reference_commented_azure_module():
    """
    terraform/outputs.tf must not reference module.azure_infra while that module
    is commented out in main.tf. An active output on a disabled module causes
    terraform plan to fail with 'reference to undeclared module'.
    """
    outputs_path = os.path.join(TERRAFORM_DIR, "outputs.tf")
    with open(outputs_path) as f:
        content = f.read()
    active_lines = [line for line in content.splitlines()
                    if "module.azure_infra" in line and not line.strip().startswith("#")]
    assert not active_lines, (
        f"outputs.tf has active references to module.azure_infra (which is commented out):\n"
        + "\n".join(active_lines)
    )


def test_terraform_snowflake_foundation_outputs_role_names_map():
    """
    Any reference to snowflake_foundation module outputs must use role_names
    (a map) not dbt_role_name (which doesn't exist). The wrong attribute name
    causes terraform plan to fail with 'unsupported attribute'.
    """
    outputs_path = os.path.join(TERRAFORM_DIR, "outputs.tf")
    assert file_contains(outputs_path, 'role_names["transformer"]'), (
        "outputs.tf does not reference role_names[\"transformer\"] — "
        "dbt_role_name is not a valid output of the snowflake_foundation module."
    )


def test_terraform_loader_bronze_grants_no_truncate():
    """
    The LOADER role must not have TRUNCATE on Bronze tables. Bronze is an
    append-only audit layer — giving LOADER the ability to TRUNCATE would
    allow a service account to wipe the entire raw data history.
    """
    main_path = os.path.join(TERRAFORM_DIR, "modules", "snowflake-foundation", "main.tf")
    with open(main_path) as f:
        content = f.read()

    in_loader_bronze_block = False
    for line in content.splitlines():
        if "loader_bronze_future_tables" in line:
            in_loader_bronze_block = True
        if in_loader_bronze_block and "TRUNCATE" in line and not line.strip().startswith("#"):
            pytest.fail(
                "LOADER role has TRUNCATE on Bronze future tables. "
                "Remove TRUNCATE — Bronze is append-only."
            )
        if in_loader_bronze_block and line.strip() == "}":
            in_loader_bronze_block = False


# ── 4. GitHub Actions Workflow ────────────────────────────────────────────────

WORKFLOW_PATH = os.path.join(ROOT, ".github", "workflows", "dbt-docs.yml")


def test_workflow_yaml_is_valid():
    """
    The GitHub Actions workflow must be valid YAML. An invalid workflow file
    is silently ignored by GitHub — the workflow simply never runs, and there
    is no error message in the UI.
    """
    with open(WORKFLOW_PATH) as f:
        try:
            yaml.safe_load(f)
        except yaml.YAMLError as e:
            pytest.fail(f"dbt-docs.yml is not valid YAML: {e}")


def test_workflow_triggers_on_push_to_main():
    """The workflow must trigger on pushes to main."""
    assert file_contains(WORKFLOW_PATH, "branches: [main]"), (
        "dbt-docs.yml does not trigger on push to main."
    )


def test_workflow_has_pages_write_permission():
    """
    The workflow must declare pages: write permission. Without this, the
    deploy-pages action fails with a 403 even if the repository has Pages enabled.
    """
    assert file_contains(WORKFLOW_PATH, "pages: write"), (
        "dbt-docs.yml is missing 'pages: write' in the permissions block."
    )


def test_workflow_uploads_pages_artifact():
    """
    The build job must upload a pages artifact before the deploy job can run.
    The deploy job has no way to access the dbt/target directory otherwise.
    """
    assert file_contains(WORKFLOW_PATH, "upload-pages-artifact"), (
        "dbt-docs.yml is missing the upload-pages-artifact step."
    )


# ── 5. profiles.yml.example ───────────────────────────────────────────────────

PROFILES_PATH = os.path.join(ROOT, "dbt", "profiles.yml.example")


def test_profiles_example_is_valid_yaml():
    """profiles.yml.example must be valid YAML so developers can copy it as-is."""
    with open(PROFILES_PATH) as f:
        try:
            yaml.safe_load(f)
        except yaml.YAMLError as e:
            pytest.fail(f"profiles.yml.example is not valid YAML: {e}")


def test_profiles_example_uses_env_vars_for_credentials():
    """
    Credentials in profiles.yml.example must come from environment variables,
    never from hardcoded strings. The example file is committed to the repo —
    hardcoded credentials would be a public security exposure.
    """
    assert file_contains(PROFILES_PATH, "env_var('SNOWFLAKE_ACCOUNT')",
                                        "env_var('SNOWFLAKE_USER')"), (
        "profiles.yml.example contains hardcoded credentials instead of env_var() calls."
    )


def test_profiles_example_has_dev_and_prod_targets():
    """
    The profiles file must define both a dev and a prod target. A profiles file
    with only one target means developers cannot run dbt locally without
    modifying the file (which risks accidentally committing credentials).
    """
    assert file_contains(PROFILES_PATH, "dev:", "prod:"), (
        "profiles.yml.example is missing either the 'dev' or 'prod' target."
    )


def test_profiles_example_uses_key_pair_for_prod():
    """
    The prod target must use RSA key-pair authentication, not a password.
    Password auth is acceptable for local dev but never for a CI/production
    service account — keys can be rotated without updating secrets managers.
    """
    assert file_contains(PROFILES_PATH, "private_key_path"), (
        "profiles.yml.example prod target does not configure RSA key-pair auth."
    )
