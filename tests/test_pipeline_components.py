"""
Pipeline Component Tests — NYC 311 Data Platform

Tests the non-dbt pieces of the pipeline without needing live cloud credentials:

  1. Airflow DAG           — syntax validity, task count, dependency chain
  2. Terraform             — HCL syntax validity via terraform validate
  3. GitHub Actions        — workflow YAML structure
  4. profiles.yml.example  — connection config correctness
  5. Workflow operations   — timeouts, SHA pinning, evidence-on-failure, and
                             the daily-run heartbeat's decision logic

A sixth category — Databricks notebooks — was removed with the Databricks path;
this header listed it for some time after the tests themselves were gone, which
is the failure mode `file_contains` below exists to prevent in the other
direction.
"""

import ast
import importlib
import os
import subprocess
import sys
from datetime import datetime, timezone

import yaml
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# scripts/ is a directory of standalone checkers, not a package. Section 5b
# imports one of them to test its decision function directly rather than
# asserting on its source text.
sys.path.insert(0, os.path.join(ROOT, "scripts"))


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
            check=False,   # returncode drives the skip below
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
        check=False,   # returncode drives the pytest.fail below
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
        "outputs.tf has active references to module.azure_infra (which is commented out):\n"
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


# ── 5. Workflow operational guarantees ────────────────────────────────────────
# The tests above check one workflow's shape (dbt-docs.yml). These check
# properties that must hold across EVERY workflow, plus the two step-level
# guarantees that were silently absent until they were looked for:
# evidence-on-failure in daily-run.yml, and a job timeout anywhere at all.

WORKFLOW_DIR = os.path.join(ROOT, ".github", "workflows")


def load_workflow(name):
    with open(os.path.join(WORKFLOW_DIR, name)) as f:
        return yaml.safe_load(f)


def all_workflow_files():
    return sorted(
        f for f in os.listdir(WORKFLOW_DIR) if f.endswith((".yml", ".yaml"))
    )


def test_every_workflow_is_valid_yaml():
    """
    Extends the dbt-docs.yml-only check above to the whole directory. An
    unparseable workflow is not an error in the GitHub UI — it simply never
    runs, which for daily-run.yml means silent data staleness.
    """
    for name in all_workflow_files():
        try:
            load_workflow(name)
        except yaml.YAMLError as e:
            pytest.fail(f"{name} is not valid YAML: {e}")


def test_every_job_declares_a_timeout():
    """
    Every job must set timeout-minutes. The GitHub default is 6 HOURS.
    daily-run.yml queues rather than cancels on its concurrency group, so one
    hung run could hold the group past the next day's 10:00 UTC trigger and
    take the pipeline offline without ever going red.
    """
    missing = [
        f"{name}:{job_id}"
        for name in all_workflow_files()
        for job_id, job in load_workflow(name)["jobs"].items()
        if "timeout-minutes" not in job
    ]
    assert not missing, (
        f"jobs with no timeout-minutes (they default to 6 hours): {missing}"
    )


def test_every_action_reference_is_sha_pinned():
    """
    Every `uses:` must name a 40-character commit SHA, not a tag. Tags are
    mutable: a compromised or retagged action would execute inside a workflow
    holding issues: write on this repository.
    """
    unpinned = []
    for name in all_workflow_files():
        for job_id, job in load_workflow(name)["jobs"].items():
            for step in job.get("steps", []):
                ref = step.get("uses", "").partition("@")[2]
                if not (len(ref) == 40 and all(c in "0123456789abcdef" for c in ref)):
                    if "uses" in step:
                        unpinned.append(f"{name}:{job_id}:{step['uses']}")
    assert not unpinned, f"action references not pinned to a full commit SHA: {unpinned}"


def test_daily_run_uploads_evidence_even_when_the_pipeline_fails():
    """
    The upload step must carry `if: always()`.

    A step with no `if:` key defaults to success(), so this upload — the one
    the workflow comments describe as existing FOR the postmortem — was skipped
    on precisely the runs that needed it. It went unnoticed because all three
    historical failures happened downstream of the upload step.
    """
    steps = load_workflow("daily-run.yml")["jobs"]["daily-run"]["steps"]
    upload = next(s for s in steps if s.get("name") == "Upload DuckDB artifact")
    assert upload.get("if") == "always()", (
        "Upload DuckDB artifact has no `if: always()` — on a pipeline failure "
        "it defaults to success() and no evidence bundle is produced."
    )


def test_heartbeat_watches_the_daily_run_on_its_own_schedule():
    """
    The heartbeat must be independently scheduled. Its whole purpose is to
    speak when daily-run.yml does not run, so it cannot be triggered by
    daily-run.yml or share its schedule.
    """
    wf = load_workflow("heartbeat.yml")
    # PyYAML 1.1 resolves the bare key `on` to the boolean True; GitHub's own
    # parser keeps it as the string "on". Accept whichever this PyYAML produced.
    triggers = wf.get("on", wf.get(True))
    assert "schedule" in triggers, "heartbeat.yml is not on a schedule"
    assert triggers["schedule"], "heartbeat.yml declares an empty schedule"


def test_heartbeat_job_is_least_privilege():
    """
    The heartbeat reads the Actions API and files an issue. It must hold
    exactly `actions: read` + `issues: write` over the read-only default, and
    nothing more — it never touches code, packages, or Pages.
    """
    wf = load_workflow("heartbeat.yml")
    assert wf["permissions"] == {"contents": "read"}, (
        "heartbeat.yml must default to contents: read at the workflow level"
    )
    assert wf["jobs"]["heartbeat"]["permissions"] == {
        "contents": "read",
        "actions": "read",
        "issues": "write",
    }, "heartbeat job permissions drifted from least privilege"


def test_heartbeat_dedups_issues_instead_of_filing_a_new_one_each_run():
    """
    The heartbeat runs every 4 hours. Without the repo's list-then-comment
    dedup it would file six issues a day for one outage, and the alert would
    be ignored within a day of being right.
    """
    path = os.path.join(WORKFLOW_DIR, "heartbeat.yml")
    assert file_contains(path, "gh issue list --label daily-run-breach --state open"), (
        "heartbeat.yml does not look for an existing open issue before creating one"
    )
    assert file_contains(path, "gh issue comment"), (
        "heartbeat.yml never comments on an existing issue — it can only create"
    )


# ── 5b. Heartbeat decision logic ──────────────────────────────────────────────
# The workflow above cannot be executed in a test, but the decision it makes
# can: check_daily_run_heartbeat.evaluate() takes the two API facts, a clock,
# and a threshold, and returns a verdict with no I/O of its own.

heartbeat = importlib.import_module("check_daily_run_heartbeat")

NOW = datetime(2026, 8, 27, 12, 0, 0, tzinfo=timezone.utc)


def test_heartbeat_passes_on_a_recent_success():
    v = heartbeat.evaluate(
        workflow_state="active",
        last_success_completed_at="2026-08-27T10:00:00Z",
        now=NOW,
        threshold_hours=26,
    )
    assert v.ok is True
    assert v.code == "live"
    assert v.age_hours == pytest.approx(2.0)


def test_heartbeat_fails_when_the_last_success_is_older_than_the_threshold():
    v = heartbeat.evaluate(
        workflow_state="active",
        last_success_completed_at="2026-08-26T09:00:00Z",
        now=NOW,
        threshold_hours=26,
    )
    assert v.ok is False
    assert v.code == "stale"
    assert v.age_hours == pytest.approx(27.0)


def test_heartbeat_threshold_boundary_is_exclusive_at_26h():
    """26.0h exactly is a breach; a second under it is not. Pinning the
    comparison direction so a refactor cannot silently widen the window."""
    just_inside = heartbeat.evaluate(
        workflow_state="active",
        last_success_completed_at="2026-08-26T10:00:01Z",
        now=NOW,
        threshold_hours=26,
    )
    exactly_at = heartbeat.evaluate(
        workflow_state="active",
        last_success_completed_at="2026-08-26T10:00:00Z",
        now=NOW,
        threshold_hours=26,
    )
    assert just_inside.ok is True
    assert exactly_at.ok is False


def test_heartbeat_fails_a_disabled_workflow_even_with_a_fresh_success():
    """
    The failure mode that motivates the whole check. GitHub disables scheduled
    workflows on public repos after 60 days without repository activity, and a
    maintainer can disable one by hand. Either way the last success can still
    be minutes old while no future run will ever fire — so 'disabled' must
    outrank freshness, not be masked by it.
    """
    v = heartbeat.evaluate(
        workflow_state="disabled_inactivity",
        last_success_completed_at="2026-08-27T11:59:00Z",
        now=NOW,
        threshold_hours=26,
    )
    assert v.ok is False
    assert v.code == "workflow-disabled"


def test_heartbeat_fails_when_the_workflow_has_never_succeeded():
    """An empty run list must not read as 'age unknown, therefore fine'."""
    v = heartbeat.evaluate(
        workflow_state="active",
        last_success_completed_at=None,
        now=NOW,
        threshold_hours=26,
    )
    assert v.ok is False
    assert v.code == "never-succeeded"


def test_heartbeat_only_counts_successes_on_the_watched_branch(monkeypatch):
    """
    The runs query must carry a branch filter, and the workflow must pass
    `main`. Actions cache scoping means a run dispatched from a feature branch
    saves into that branch's cache and never advances main's accumulated
    DuckDB — counting it would silence the alert for a day while main's data
    actually aged. Asserted at the API call, not just in the workflow text.
    """
    calls = []

    def fake_gh_api(path):
        calls.append(path)
        if "/runs" in path:
            return {"workflow_runs": [{"updated_at": "2026-08-27T10:00:00Z"}]}
        return {"state": "active"}

    monkeypatch.setattr(heartbeat, "gh_api", fake_gh_api)
    state, last = heartbeat.fetch_facts("o/r", "daily-run.yml", "main")

    assert state == "active"
    assert last == "2026-08-27T10:00:00Z"
    runs_call = next(c for c in calls if "/runs" in c)
    assert "branch=main" in runs_call, f"runs query has no branch filter: {runs_call}"
    assert "status=success" in runs_call, f"runs query does not filter to successes: {runs_call}"
    assert file_contains(os.path.join(WORKFLOW_DIR, "heartbeat.yml"), "--branch main"), (
        "heartbeat.yml does not pass --branch main to the checker"
    )


def test_heartbeat_default_threshold_matches_slo1():
    """
    The default must stay the SLO-1 number. The external watcher and the
    internal freshness SLO measure one commitment from two sides; if the
    default drifted below 26 the heartbeat would file breaches for runs SLO-1
    still passes.
    """
    assert heartbeat.DEFAULT_THRESHOLD_HOURS == 26.0
    assert file_contains(os.path.join(ROOT, "docs", "SLO.md"), "< 26")
