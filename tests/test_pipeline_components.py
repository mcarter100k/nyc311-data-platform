"""
Tests for the non-dbt pieces of the pipeline. No cloud credentials needed.

  1. Airflow DAG           — the dependency graph, read from the file's AST
  2. Terraform             — outputs and the LOADER grants on Bronze
  3. GitHub Actions        — dbt-docs.yml structure
  4. profiles.yml.example  — connection config
  5. Workflow operations   — timeouts, SHA pinning, evidence on failure, the
                             daily run's unattended contract, the history
                             check, and the heartbeat's decision logic
"""

import ast
import importlib
import os
import re
import sys
from datetime import datetime, UTC

import yaml
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# scripts/ is not a package; section 5b imports the heartbeat checker from it.
sys.path.insert(0, os.path.join(ROOT, "scripts"))


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_python(path):
    """Parse a Python file and return the AST. Raises SyntaxError on failure."""
    with open(path) as f:
        source = f.read()
    return ast.parse(source, filename=path)


def file_contains(path, *strings):
    """Return True if the file's code, with `#` comments removed, contains
    every string. Matching comments would let a guard pass on prose after the
    code it describes was deleted. A `#` inside a string literal also cuts the
    line; none of the asserted strings sit after one.
    """
    with open(path) as f:
        code_lines = [line.split("#", 1)[0] for line in f]
    content = "\n".join(code_lines)
    return all(s in content for s in strings)


def hcl_top_level_blocks(text):
    """Yield (header, body) for every TOP-LEVEL block in an HCL document.

    A brace-depth scan, not a full HCL parser (python-hcl2 is not a
    dependency). Tracking depth means a block ends at its own closing brace,
    not at the first nested `}`, so arguments after a nested block are still
    read. Braces inside double-quoted strings and `#` / `//` comments are
    skipped. Heredocs, `/* */` comments and object literals are not handled;
    none appear in this repo's .tf files.

    `header` is the last non-blank line before the opening `{`, e.g.
    `resource "snowflake_grant_privileges_to_account_role" "loader_db_usage" {`.
    `body` is the raw text between the braces.
    """
    blocks = []
    depth = 0
    i = 0
    n = len(text)
    start = 0
    header = None
    body_start = None
    in_string = False
    in_comment = False

    while i < n:
        ch = text[i]

        if in_comment:
            if ch == "\n":
                in_comment = False
            i += 1
            continue

        if in_string:
            if ch == "\\":
                i += 2
                continue
            if ch == '"':
                in_string = False
            i += 1
            continue

        if ch == '"':
            in_string = True
            i += 1
            continue

        if ch == "#" or text[i:i + 2] == "//":
            in_comment = True
            i += 1
            continue

        if ch == "{":
            depth += 1
            if depth == 1:
                preamble = text[start:i].strip()
                header = preamble.splitlines()[-1].strip() if preamble else ""
                body_start = i + 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                blocks.append((header, text[body_start:i]))
                start = i + 1
        i += 1

    return blocks


def hcl_string_list(body, attr):
    """Return the string elements of `attr = ["A", "B"]` inside an HCL body.

    Only the block's OWN attribute is matched (the search is anchored to a line
    start and stops at the first `]`), and only quoted elements are returned —
    a computed value such as `privileges = var.something` yields None so the
    caller can distinguish "no such attribute" from "not a literal list".
    """
    match = re.search(
        r"^[ \t]*" + re.escape(attr) + r"\s*=\s*\[([^\]]*)\]",
        body, re.MULTILINE,
    )
    if match is None:
        return None
    return re.findall(r'"([^"]*)"', match.group(1))


# ── 1. Airflow DAG (the one that runs) ────────────────────────────────────────

DAG_PATH = os.path.join(ROOT, "airflow", "dags", "nyc311_local.py")

# The tasks nyc311_local must define. scripts/check_claims.py checks this list
# against the DAG file and the task names in docs/ARCHITECTURE.md.
EXPECTED_TASKS = [
    "check_source",
    "fetch_live",
    "load_bronze",
    "load_silver",
    "dbt_build",
    "check_slos",
    "upstream_stall_check",
]


def test_local_dag_does_not_catch_up():
    """catchup=False matters: each run fetches a trailing window, so catching
    up missed intervals would re-fetch the same rows repeatedly."""
    assert file_contains(DAG_PATH, "catchup=False"), (
        "nyc311_local must set catchup=False — see the DAG docstring and ADR 010."
    )


def test_local_dag_invokes_the_pipeline_venv_explicitly():
    """Airflow runs in its own virtualenv; the tasks must call .venv's
    interpreter rather than whatever python is on PATH."""
    assert file_contains(DAG_PATH, "PIPELINE_PY"), (
        "Tasks must invoke the pipeline venv explicitly, not ambient python."
    )


# ── 1b. The DAG's dependency graph ───────────────────────────────────────────
#
# These read the real `>>` edges: a check on task names alone passes whatever
# the wiring. The file is parsed as an AST rather than imported because this
# suite's .venv has no Airflow (it lives in .venv-airflow), and
# `pytest.importorskip("airflow")` would not skip: the repo's airflow/
# directory imports as an empty namespace package. dag_dependency_edges raises
# on any construct it cannot model, so a missing edge is a failure, not a
# silent pass.
#
# Ordering is checked as reachability ("b runs somewhere after a"), not exact
# sequence, so inserting or parallelising tasks stays green while reversing
# two dependent tasks goes red.

# Ordering relationships that must hold for the pipeline to be correct.
REQUIRED_ORDERING = [
    # Don't spend a fetch on a source that isn't answering.
    ("check_source", "fetch_live"),
    # Medallion order: raw lands before bronze, bronze before silver.
    ("fetch_live", "load_bronze"),
    ("load_bronze", "load_silver"),
    # dbt reads silver. Building first would transform stale or absent data.
    ("load_silver", "dbt_build"),
    # SLO-1 freshness is measured on what dbt just built, not what was there
    # before the run.
    ("dbt_build", "check_slos"),
    # Same for the stall warning: it reads the DuckDB the build populates.
    # Deliberately NOT pinned to `check_slos` — the two are independent readers
    # and parallelising them is a legitimate change.
    ("dbt_build", "upstream_stall_check"),
]

# The only task that may have no upstream. Any other root fires at DAG start.
DAG_ROOT_TASKS = {"check_source"}

# Dependency helpers that create edges this AST reader cannot see. Their
# presence makes the reconstructed graph a lie, so it refuses to return one.
UNMODELLED_DEPENDENCY_HELPERS = {"chain", "chain_linear", "cross_downstream"}


def _task_ids_by_variable(tree):
    """Map `x` -> "the_task_id" for every `x = SomeOperator(task_id="...")`.

    `>>` chains name Python variables, so edges are resolved through this map.
    """
    mapping = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        task_id = next(
            (
                kw.value.value
                for kw in node.value.keywords
                if kw.arg == "task_id"
                and isinstance(kw.value, ast.Constant)
                and isinstance(kw.value.value, str)
            ),
            None,
        )
        if task_id is None:
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                mapping[target.id] = task_id
    return mapping


def _endpoints(node, task_ids, edges):
    """Reduce a dependency expression to (entry tasks, exit tasks), collecting
    edges into `edges` on the way.

    `a >> b >> c` parses as `BinOp(BinOp(a, >>, b), >>, c)`, so the edge b->c
    needs the EXIT of the left subtree, not its root. Lists fan out: for
    `[a, b] >> c` the entry and exit sets are both {a, b}, producing a->c and
    b->c. Raises on any node shape it does not model, so an unreadable
    expression fails loudly instead of contributing zero edges.
    """
    if isinstance(node, ast.Name):
        if node.id not in task_ids:
            raise AssertionError(
                f"Dependency chain references '{node.id}', which is not a "
                f"variable assigned an operator with a task_id. Known tasks: "
                f"{sorted(task_ids)}"
            )
        return {task_ids[node.id]}, {task_ids[node.id]}
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        members = set()
        for element in node.elts:
            entry, exit_ = _endpoints(element, task_ids, edges)
            members |= entry | exit_
        return members, members
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
        left_entry, left_exit = _endpoints(node.left, task_ids, edges)
        right_entry, right_exit = _endpoints(node.right, task_ids, edges)
        if isinstance(node.op, ast.RShift):
            edges.update((u, d) for u in left_exit for d in right_entry)
            return left_entry, right_exit
        # `a << b` means b is upstream of a.
        edges.update((u, d) for u in right_exit for d in left_entry)
        return right_entry, left_exit
    raise AssertionError(
        f"Unreadable dependency expression at line {getattr(node, 'lineno', '?')}: "
        f"{ast.dump(node)[:200]}. Extend _endpoints() rather than leaving the "
        f"reconstructed graph incomplete."
    )


def dag_dependency_edges(path=DAG_PATH):
    """Return (task_ids_by_variable, {(upstream_task_id, downstream_task_id)}).

    Raises rather than returning an empty or partial graph, so a missing
    file, a deleted dependency block, or an unmodelled helper fails the test.
    """
    assert os.path.exists(path), (
        f"{path} does not exist. If the DAG was renamed, update DAG_PATH — "
        f"do not let this guard find nothing to check."
    )
    tree = parse_python(path)
    task_ids = _task_ids_by_variable(tree)
    assert task_ids, (
        f"No `variable = Operator(task_id=...)` assignments found in {path}. "
        f"Either the DAG defines no tasks or it builds them in a way this "
        f"reader cannot follow; in both cases the graph below would be empty."
    )

    edges = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id in UNMODELLED_DEPENDENCY_HELPERS:
                raise AssertionError(
                    f"{path} line {node.lineno} uses {func.id}(), which creates "
                    f"dependencies this AST reader does not model. The "
                    f"reconstructed graph would be missing edges and the "
                    f"ordering assertions would be vacuous. Extend "
                    f"dag_dependency_edges() to handle it."
                )
            if isinstance(func, ast.Attribute) and func.attr in (
                "set_downstream",
                "set_upstream",
            ):
                base_entry, base_exit = _endpoints(func.value, task_ids, edges)
                for arg in node.args:
                    arg_entry, arg_exit = _endpoints(arg, task_ids, edges)
                    if func.attr == "set_downstream":
                        edges.update((u, d) for u in base_exit for d in arg_entry)
                    else:
                        edges.update((u, d) for u in arg_exit for d in base_entry)
        elif isinstance(node, ast.Expr) and isinstance(node.value, ast.BinOp):
            if isinstance(node.value.op, (ast.RShift, ast.LShift)):
                _endpoints(node.value, task_ids, edges)

    assert edges, (
        f"{path} declares tasks but NO dependencies between them. Every task "
        f"would fire at DAG start. If the `>>` chain moved or was deleted, this "
        f"is the bug; if it moved to a construct this reader cannot see, extend "
        f"dag_dependency_edges()."
    )
    return task_ids, edges


def _reachable_from(edges, start):
    """Task ids reachable downstream of `start`. Cycle-safe (visited set)."""
    downstream = {}
    for upstream, downstream_task in edges:
        downstream.setdefault(upstream, set()).add(downstream_task)
    seen, stack = set(), [start]
    while stack:
        for nxt in downstream.get(stack.pop(), ()):
            if nxt not in seen:
                seen.add(nxt)
                stack.append(nxt)
    return seen


def test_local_dag_dependency_graph_is_readable_and_complete():
    """Every expected task is defined and appears in at least one edge, so the
    ordering tests below run against a real graph."""
    task_ids, edges = dag_dependency_edges()

    missing = [t for t in EXPECTED_TASKS if t not in set(task_ids.values())]
    assert not missing, f"DAG does not define task(s): {missing}"

    wired = {t for edge in edges for t in edge}
    unwired = [t for t in EXPECTED_TASKS if t not in wired]
    assert not unwired, (
        f"Task(s) {unwired} are defined but appear in no dependency edge — "
        f"they would run immediately at DAG start."
    )


@pytest.mark.parametrize("upstream,downstream", REQUIRED_ORDERING)
def test_local_dag_orders_tasks_correctly(upstream, downstream):
    """`downstream` must be reachable from `upstream` in the edge list."""
    _, edges = dag_dependency_edges()
    reachable = _reachable_from(edges, upstream)
    assert downstream in reachable, (
        f"'{downstream}' does not run after '{upstream}' in nyc311_local. "
        f"Reachable from '{upstream}': {sorted(reachable) or 'nothing'}. "
        f"Edges: {sorted(edges)}"
    )


def test_local_dag_has_no_orphaned_tasks():
    """Only `check_source` may have no upstream; any other root fires at DAG
    start (e.g. check_slos judging the previous run's warehouse)."""
    _, edges = dag_dependency_edges()
    has_upstream = {downstream for _, downstream in edges}
    roots = {t for t in EXPECTED_TASKS if t not in has_upstream}
    assert roots == DAG_ROOT_TASKS, (
        f"Tasks with no upstream should be exactly {sorted(DAG_ROOT_TASKS)}, "
        f"but are {sorted(roots)}. Extra roots fire at DAG start."
    )


def test_local_dag_is_acyclic():
    """Airflow rejects a cyclic DAG, but only on the scheduler; nothing here
    loads the DAG with Airflow."""
    _, edges = dag_dependency_edges()
    cyclic = [t for t in EXPECTED_TASKS if t in _reachable_from(edges, t)]
    assert not cyclic, (
        f"Task(s) {cyclic} are reachable from themselves — the DAG has a cycle "
        f"and Airflow will refuse to import it. Edges: {sorted(edges)}"
    )


# ── 2. Terraform ──────────────────────────────────────────────────────────────

TERRAFORM_DIR = os.path.join(ROOT, "terraform")


def test_terraform_snowflake_foundation_outputs_role_names_map():
    """Root outputs read role names from the module's role_names map.

    terraform.yml validates both root modules, but it is not a required check;
    this keeps a cheap version of the check in fast-gate.
    """
    outputs_path = os.path.join(TERRAFORM_DIR, "outputs.tf")
    assert file_contains(outputs_path, 'role_names["transformer"]'), (
        "outputs.tf does not reference role_names[\"transformer\"] — "
        "dbt_role_name is not a valid output of the snowflake_foundation module."
    )


# Privileges that let the holder empty a Bronze table, either directly or by
# containing the one that does.
#
#   TRUNCATE        — the privilege itself.
#   ALL PRIVILEGES  — Snowflake's docs: "Grants all privileges, except
#     OWNERSHIP, on a table." TRUNCATE is an ordinary table privilege in that
#     list, so ALL CONTAINS IT. `ALL` is the documented synonym.
#     https://docs.snowflake.com/en/user-guide/security-access-control-privileges
#   OWNERSHIP       — the same page says OWNERSHIP "Grants full control over
#     the table". It does NOT enumerate TRUNCATE under OWNERSHIP, so this is
#     the one entry below that is an inference rather than a quoted guarantee:
#     an owner holds the object with grant option and can therefore grant
#     itself TRUNCATE at will. Treated as equivalent, and flagged here as an
#     inference so the next reader does not mistake it for a citation.
#
# Terraform's snowflake_grant_privileges_to_account_role also exposes a boolean
# `all_privileges = true` that grants the same set without naming a privilege;
# it is checked separately below because it is not a list element.
BRONZE_DESTRUCTIVE_PRIVILEGES = {"TRUNCATE", "ALL PRIVILEGES", "ALL", "OWNERSHIP"}


def loader_bronze_grant_blocks():
    """Every top-level grant resource that gives the LOADER role something on
    BRONZE. Selected by content, not resource name, so renaming or adding a
    resource cannot drop it from the guard."""
    main_path = os.path.join(TERRAFORM_DIR, "modules", "snowflake-foundation", "main.tf")
    with open(main_path) as f:
        content = f.read()

    found = []
    for header, body in hcl_top_level_blocks(content):
        if not header.startswith('resource "snowflake_grant'):
            continue
        # Comments mention TRUNCATE and Bronze; only code counts.
        code = "\n".join(line.split("#", 1)[0] for line in body.splitlines())
        # Both provider spellings of the role resource (snowflake_role was
        # renamed snowflake_account_role), so a grant written either way is seen.
        if not re.search(r"\bsnowflake_(?:account_)?role\.loader\b", code):
            continue
        if "fq_bronze" not in code and "BRONZE" not in code:
            continue
        found.append((header, code))
    return found


def test_terraform_loader_bronze_grants_no_truncate():
    """
    The LOADER role must not be able to TRUNCATE Bronze. Bronze is an
    append-only audit layer — a service account that can empty it can erase the
    entire raw data history, and nothing downstream would report a gap.

    Checks every LOADER grant touching Bronze, selected by content, and
    fails if none is found: "nothing to check" must not look like "clean".

    Known limit: LOADER has CREATE TABLE on BRONZE, so it owns the tables it
    creates and could grant itself TRUNCATE. Closing that means moving table
    creation to another role.
    """
    blocks = loader_bronze_grant_blocks()

    assert blocks, (
        "No LOADER grant on BRONZE found in modules/snowflake-foundation/main.tf. "
        "Either the grants moved to another file (point this guard at it) or "
        "they were deleted. Failing rather than passing: a guard that finds "
        "nothing to inspect has verified nothing."
    )

    assert any("TABLES" in code for _, code in blocks), (
        "LOADER has grants on BRONZE but none on TABLES — the object type this "
        "guard exists to constrain. If the table grant genuinely moved, update "
        "this guard; do not let it pass by having nothing to check."
    )

    violations = []
    for header, code in blocks:
        privileges = hcl_string_list(code, "privileges") or []
        for privilege in privileges:
            if privilege.strip().upper() in BRONZE_DESTRUCTIVE_PRIVILEGES:
                violations.append(f"{header.strip()}\n      privileges include {privilege!r}")
        if re.search(r"^\s*all_privileges\s*=\s*true", code, re.MULTILINE):
            violations.append(f"{header.strip()}\n      sets all_privileges = true")

    assert not violations, (
        "LOADER can empty BRONZE. Bronze is append-only — the privilege layer "
        "is what enforces that, not convention:\n    "
        + "\n    ".join(violations)
        + "\n  TRUNCATE, ALL PRIVILEGES/ALL, OWNERSHIP and all_privileges = true "
        "all confer the ability to empty a table."
    )


# ── 3. GitHub Actions Workflow ────────────────────────────────────────────────

WORKFLOW_PATH = os.path.join(ROOT, ".github", "workflows", "dbt-docs.yml")


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
# Properties every workflow must have, plus daily-run.yml's evidence upload.

WORKFLOW_DIR = os.path.join(ROOT, ".github", "workflows")


def load_workflow(name):
    with open(os.path.join(WORKFLOW_DIR, name)) as f:
        return yaml.safe_load(f)


def all_workflow_files():
    return sorted(
        f for f in os.listdir(WORKFLOW_DIR) if f.endswith((".yml", ".yaml"))
    )


def test_every_workflow_is_valid_yaml():
    """An unparseable workflow never runs; for daily-run.yml that means
    silent staleness."""
    for name in all_workflow_files():
        try:
            load_workflow(name)
        except yaml.YAMLError as e:
            pytest.fail(f"{name} is not valid YAML: {e}")


def test_every_job_declares_a_timeout():
    """Every job sets timeout-minutes. The default is 6 hours, and
    daily-run.yml queues runs, so one hung run could swallow the next day's
    trigger without ever going red."""
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
    """Every `uses:` names a 40-character commit SHA. Tags can be moved, and
    a retagged action would run with issues: write on this repository."""
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
    """The upload step needs `if: always()`. Without an `if:` a step runs
    only on success, so the evidence would be missing from failed runs."""
    steps = load_workflow("daily-run.yml")["jobs"]["daily-run"]["steps"]
    upload = next(s for s in steps if s.get("name") == "Upload DuckDB artifact")
    assert upload.get("if") == "always()", (
        "Upload DuckDB artifact has no `if: always()` — on a pipeline failure "
        "it defaults to success() and no evidence bundle is produced."
    )


def test_daily_run_scheduled_and_manual_windows_agree():
    """Scheduled runs have no inputs and use the WINDOW_DAYS fallback; manual
    runs use the input default. Both must equal local_runner.LIVE_DAYS (read as
    text: fast-gate has no pandas to import local_runner)."""
    with open(os.path.join(ROOT, "local", "local_runner.py")) as f:
        runner = f.read()
    live_days = re.search(r"^LIVE_DAYS\s*=\s*(\d+)", runner, re.MULTILINE).group(1)
    wf = load_workflow("daily-run.yml")
    triggers = wf.get("on", wf.get(True))
    manual_default = triggers["workflow_dispatch"]["inputs"]["window_days"]["default"]
    run_step = next(
        s for s in wf["jobs"]["daily-run"]["steps"] if "--days" in s.get("run", "")
    )
    match = re.fullmatch(
        r"\$\{\{ github\.event\.inputs\.window_days \|\| '(\d+)' \}\}",
        run_step["env"]["WINDOW_DAYS"],
    )
    assert match, f"unexpected WINDOW_DAYS expression: {run_step['env']['WINDOW_DAYS']}"
    assert match.group(1) == manual_default, (
        f"scheduled runs fetch {match.group(1)} days but manual runs default to "
        f"{manual_default}"
    )
    assert '--days "$WINDOW_DAYS"' in run_step["run"]
    assert manual_default == live_days, (
        f"daily-run.yml fetches {manual_default} days but LIVE_DAYS is {live_days}"
    )


def test_heartbeat_watches_the_daily_run_on_its_own_schedule():
    """The heartbeat has its own schedule; it must speak when daily-run.yml
    does not run."""
    wf = load_workflow("heartbeat.yml")
    # PyYAML 1.1 resolves the bare key `on` to the boolean True; GitHub's own
    # parser keeps it as the string "on". Accept whichever this PyYAML produced.
    triggers = wf.get("on", wf.get(True))
    assert "schedule" in triggers, "heartbeat.yml is not on a schedule"
    assert triggers["schedule"], "heartbeat.yml declares an empty schedule"


def test_heartbeat_job_is_least_privilege():
    """The heartbeat holds exactly actions: read and issues: write on top of
    the read-only default."""
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
    """The heartbeat runs several times a day. Without list-then-comment
    dedup it would file a new issue each run for one outage."""
    path = os.path.join(WORKFLOW_DIR, "heartbeat.yml")
    assert file_contains(path, "gh issue list --label daily-run-breach --state open"), (
        "heartbeat.yml does not look for an existing open issue before creating one"
    )
    assert file_contains(path, "gh issue comment"), (
        "heartbeat.yml never comments on an existing issue — it can only create"
    )


# ── 5a. The daily run's unattended contract ───────────────────────────────────
# What makes the pipeline run and raise alarms with nobody watching: a cron,
# an SLO gate, a breach issue on failure, and the history check around the
# pipeline step (ADR 017). Deleting any of these must fail CI.

def _daily_steps():
    return load_workflow("daily-run.yml")["jobs"]["daily-run"]["steps"]


def _step_index(steps, needle):
    """Index of the one step whose `run` or `uses` contains needle."""
    hits = [i for i, s in enumerate(steps) if needle in (s.get("run", "") + s.get("uses", ""))]
    assert len(hits) == 1, f"expected one daily-run step containing {needle!r}, found {len(hits)}"
    return hits[0]


def test_daily_run_is_scheduled():
    wf = load_workflow("daily-run.yml")
    triggers = wf.get("on", wf.get(True))  # PyYAML reads a bare `on` as True
    crons = [e.get("cron") for e in (triggers.get("schedule") or [])]
    assert any(crons), "daily-run.yml has no cron schedule, so it never runs unattended"


def test_daily_run_gates_on_both_slos():
    steps = _daily_steps()
    slo = steps[_step_index(steps, "scripts/check_slos.py")]
    assert "if" not in slo and not slo.get("continue-on-error"), (
        "the SLO step must run on every successful build and fail the run on a breach"
    )
    assert _step_index(steps, "scripts/check_slos.py") > _step_index(steps, "local/local_runner.py --live")


def test_daily_run_files_a_breach_issue_when_it_fails():
    steps = _daily_steps()
    breach = next((s for s in steps if "--label daily-run-breach" in s.get("run", "")), None)
    assert breach is not None, "no daily-run step files a daily-run-breach issue"
    assert breach.get("if") == "failure()", f"breach step runs on {breach.get('if')!r}, not failure()"
    assert "gh issue list --label daily-run-breach --state open" in breach["run"], "breach step does not dedup"


def test_history_check_brackets_the_pipeline_step():
    """The snapshot must see the restored database before the pipeline writes
    to it, and the compare must see the result (ADR 017)."""
    steps = _daily_steps()
    restore = _step_index(steps, "actions/cache/restore@")
    snap = _step_index(steps, "check_history.py snapshot")
    pipeline = _step_index(steps, "local/local_runner.py --live")
    compare = _step_index(steps, "check_history.py compare")
    assert restore < snap < pipeline < compare, (restore, snap, pipeline, compare)
    assert steps[compare].get("id") == "history"
    notice = next((s for s in steps if "--label history-loss" in s.get("run", "")), None)
    assert notice is not None and notice.get("if") == "steps.history.outputs.lost == 'true'"


def test_every_issue_label_a_workflow_uses_is_declared_in_terraform():
    """gh issue create fails on an unknown label, which would turn a warning
    into a red run. Labels live in terraform/github (ADR 012)."""
    with open(os.path.join(ROOT, "terraform", "github", "main.tf")) as f:
        declared = set(re.findall(r'resource "github_issue_label"[^{]*\{[^}]*name\s*=\s*"([^"]+)"', f.read()))
    used = set()
    for name in os.listdir(WORKFLOW_DIR):
        with open(os.path.join(WORKFLOW_DIR, name)) as f:
            used |= set(re.findall(r"--label\s+([\w-]+)", f.read()))
    assert used, "found no --label uses at all; the pattern is broken"
    assert used <= declared, f"labels used but not declared in terraform/github: {sorted(used - declared)}"


# ── 5a-ii. History check decision logic ───────────────────────────────────────

history = importlib.import_module("check_history")
BEFORE = {"exists": True, "rows": 522_464, "first_day": "2026-08-11", "last_day": "2026-09-29"}


def test_history_lost_when_nothing_was_restored():
    lost, reasons = history.verdict({"exists": False}, BEFORE)
    assert lost and "no previous database" in reasons[0]


def test_history_lost_when_gold_shrinks_past_the_tolerance():
    after = {**BEFORE, "rows": 384_992}
    lost, reasons = history.verdict(BEFORE, after)
    assert lost and "down from 522,464" in reasons[0]


def test_history_lost_when_the_earliest_day_moves_later():
    lost, reasons = history.verdict(BEFORE, {**BEFORE, "first_day": "2026-08-24"})
    assert lost and "2026-08-11 to 2026-08-24" in reasons[0]


def test_history_intact_on_normal_growth_and_small_quarantine_drops():
    assert history.verdict(BEFORE, {**BEFORE, "rows": 522_572}) == (False, [])
    small_drop = int(BEFORE["rows"] * (1 - history.ROW_DROP_TOLERANCE)) + 1
    assert history.verdict(BEFORE, {**BEFORE, "rows": small_drop}) == (False, [])


# ── 5b. Heartbeat decision logic ──────────────────────────────────────────────
# check_daily_run_heartbeat.evaluate() takes the API facts, a clock and a
# threshold, and returns a verdict with no I/O.

heartbeat = importlib.import_module("check_daily_run_heartbeat")

NOW = datetime(2026, 8, 27, 12, 0, 0, tzinfo=UTC)
THRESHOLD = 30


def test_heartbeat_passes_on_a_recent_success():
    v = heartbeat.evaluate(
        workflow_state="active",
        last_success_completed_at="2026-08-27T10:00:00Z",
        now=NOW,
        threshold_hours=THRESHOLD,
    )
    assert v.ok is True
    assert v.code == "live"
    assert v.age_hours == pytest.approx(2.0)


def test_heartbeat_fails_when_the_last_success_is_older_than_the_threshold():
    v = heartbeat.evaluate(
        workflow_state="active",
        last_success_completed_at="2026-08-26T05:00:00Z",
        now=NOW,
        threshold_hours=THRESHOLD,
    )
    assert v.ok is False
    assert v.code == "stale"
    assert v.age_hours == pytest.approx(31.0)


def test_heartbeat_threshold_boundary_is_exclusive():
    """Exactly the threshold is a breach; a second under it is not."""
    just_inside = heartbeat.evaluate(
        workflow_state="active",
        last_success_completed_at="2026-08-26T06:00:01Z",
        now=NOW,
        threshold_hours=THRESHOLD,
    )
    exactly_at = heartbeat.evaluate(
        workflow_state="active",
        last_success_completed_at="2026-08-26T06:00:00Z",
        now=NOW,
        threshold_hours=THRESHOLD,
    )
    assert just_inside.ok is True
    assert exactly_at.ok is False


def test_heartbeat_fails_a_disabled_workflow_even_with_a_fresh_success():
    """A disabled workflow will never run again, so it is a breach however
    recent its last success. The headline names the watched workflow."""
    v = heartbeat.evaluate(
        workflow_state="disabled_inactivity",
        last_success_completed_at="2026-08-27T11:59:00Z",
        now=NOW,
        threshold_hours=THRESHOLD,
        workflow="other-run.yml",
    )
    assert v.ok is False
    assert v.code == "workflow-disabled"
    assert "`other-run.yml`" in v.headline


def test_heartbeat_fails_when_the_workflow_has_never_succeeded():
    """An empty run list must not read as 'age unknown, therefore fine'."""
    v = heartbeat.evaluate(
        workflow_state="active",
        last_success_completed_at=None,
        now=NOW,
        threshold_hours=THRESHOLD,
    )
    assert v.ok is False
    assert v.code == "never-succeeded"


def test_heartbeat_only_counts_successes_on_the_watched_branch(monkeypatch):
    """Only successes on main count: a branch run saves to its own cache and
    never refreshes main's database. Checked at the API call and in the
    workflow."""
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


def test_heartbeat_workflow_uses_one_threshold():
    """heartbeat.yml defines the threshold once (THRESHOLD_HOURS) and uses it
    for both the check and the issue title, and it matches the script default.

    It must stay above the longest gap between healthy daily runs (about 27h,
    because GitHub starts the cron hours late), or healthy days raise alarms.
    """
    wf = load_workflow("heartbeat.yml")
    job = wf["jobs"]["heartbeat"]
    threshold = float(job["env"]["THRESHOLD_HOURS"])
    assert threshold == heartbeat.DEFAULT_THRESHOLD_HOURS, (
        f"heartbeat.yml THRESHOLD_HOURS={threshold} but the script default is "
        f"{heartbeat.DEFAULT_THRESHOLD_HOURS}"
    )
    assert threshold > 27, f"threshold {threshold}h is inside normal cron delay"
    scripts = "\n".join(step.get("run", "") for step in job["steps"])
    assert '--threshold-hours "$THRESHOLD_HOURS"' in scripts, (
        "the check step does not read THRESHOLD_HOURS"
    )
    assert "no successful run in ${THRESHOLD_HOURS}h" in scripts, (
        "the issue title does not read THRESHOLD_HOURS"
    )
