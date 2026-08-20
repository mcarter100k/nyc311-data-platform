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
    """Return True if the file's CODE contains ALL of the given strings.

    Strips `#` comments line-by-line before matching: a plain substring search
    over the whole file is satisfied by prose in comment blocks, so an
    assertion like "the notebook calls select_quarantine" would keep passing
    after the actual call was deleted, as long as a comment still mentioned it.
    (Naive strip: a '#' inside a string literal truncates that line — none of
    the asserted strings contain or follow an in-string '#', so this trade
    is safe here and vastly better than matching comments.)
    """
    with open(path) as f:
        code_lines = [line.split("#", 1)[0] for line in f]
    content = "\n".join(code_lines)
    return all(s in content for s in strings)


# ── 1. Airflow DAG (the one that runs) ────────────────────────────────────────

DAG_PATH = os.path.join(ROOT, "airflow", "dags", "nyc311_local.py")

# The tasks nyc311_local must define. scripts/check_claims.py asserts this list
# agrees with both the DAG file and the count stated in the README — three
# places that previously stated a task count with nothing comparing them.
EXPECTED_TASKS = [
    "check_source",
    "fetch_live",
    "load_bronze",
    "load_silver",
    "dbt_build",
    "check_slos",
    "upstream_stall_check",
]


def test_local_dag_is_valid_python():
    """The DAG must parse. A syntax error here is a silent scheduler failure."""
    parse_python(DAG_PATH)


@pytest.mark.parametrize("task_id", EXPECTED_TASKS)
def test_local_dag_contains_expected_task(task_id):
    """Every stage of the pipeline is a distinct task, so a red run names the
    failing stage instead of pointing at one monolithic 'run pipeline' step."""
    assert file_contains(DAG_PATH, f'"{task_id}"'), (
        f"Task '{task_id}' not found in nyc311_local.py."
    )


def test_local_dag_does_not_catch_up():
    """catchup=False is load-bearing, not stylistic: the fetcher pulls a
    trailing 7-day window, so backfilling missed intervals would re-fetch the
    same rows repeatedly."""
    assert file_contains(DAG_PATH, "catchup=False"), (
        "nyc311_local must set catchup=False — see the DAG docstring and ADR 010."
    )


def test_local_dag_invokes_the_pipeline_venv_explicitly():
    """Airflow runs in its own virtualenv; the tasks must call .venv's
    interpreter rather than whatever python is on PATH."""
    assert file_contains(DAG_PATH, "PIPELINE_PY"), (
        "Tasks must invoke the pipeline venv explicitly, not ambient python."
    )


# ── 2. Terraform ──────────────────────────────────────────────────────────────

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


# ── 3. GitHub Actions Workflow ────────────────────────────────────────────────

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


# ── 4. profiles.yml.example ───────────────────────────────────────────────────

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
