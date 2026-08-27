"""
Pipeline Component Tests — NYC 311 Data Platform

Tests the non-dbt pieces of the pipeline without needing live cloud credentials:

  1. Airflow DAG           — syntax validity, task count, and the dependency
                             GRAPH reconstructed from the file's AST (§1b)
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
import re
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


def hcl_top_level_blocks(text):
    """Yield (header, body) for every TOP-LEVEL block in an HCL document.

    This is a brace-DEPTH-AWARE SCAN, NOT A FULL HCL PARSER. It tracks nesting
    depth so a block ends at its OWN closing brace, and steps over double-quoted
    strings (with backslash escapes) and `#` / `//` line comments so braces
    inside them do not move the depth. It does not understand heredocs
    (`<<EOT`), `/* */` block comments, or object-literal values — none of which
    appear in this repo's .tf files. If one is introduced, this scanner must be
    revisited; python-hcl2 is not a declared dependency of this repo, so a real
    parser was not available.

    Why depth matters: the naive line scan this replaced ended a resource at
    the first line equal to `}`, which is the closing brace of the first NESTED
    block (`on_schema_object { future { ... } }`). Everything after it — in HCL,
    argument order is free — escaped the scan entirely.

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


# ── 1b. The DAG's dependency GRAPH, not just its task names ───────────────────
#
# The tests above assert that seven task_id STRINGS appear in the file. That is
# satisfied by a DAG whose tasks are wired in any order, or in no order at all.
# Three mutations were run against the suite as it stood on 2026-08-26 and all
# three passed green:
#
#   1. `dbt_build` moved ahead of `load_silver` — transform before the data it
#      transforms has been loaded.
#   2. the `dbt_build >> check_slos` edge deleted — `check_slos` and
#      `upstream_stall_check` become roots and fire at DAG start, evaluating
#      SLOs against yesterday's warehouse.
#   3. the entire dependency block deleted — seven tasks, no edges, everything
#      fires at once.
#
# Each of those is a catastrophic orchestration bug that ships silently. The
# checks below read the actual edge list.
#
# WHY THE SOURCE AND NOT THE BUILT DAG
# ------------------------------------
# The honest test would import the DAG and read `task.upstream_task_ids`,
# because that is what Airflow itself will do. It is not available here: this
# suite runs in `.venv`, which has no Airflow (Airflow lives in `.venv-airflow`,
# kept separate on purpose — see the DAG docstring). And the usual guard,
# `pytest.importorskip("airflow")`, is actively DANGEROUS in this repo: `import
# airflow` SUCCEEDS from the repo root because `airflow/` is a directory and
# Python treats it as a namespace package, so importorskip would neither skip
# nor import anything real:
#
#     >>> import airflow; airflow.__file__ is None      # True — namespace pkg
#     >>> from airflow.sdk import DAG                   # ModuleNotFoundError
#
# So these tests parse the file's AST and reconstruct the `>>` / `<<` chains
# into an edge list. That is weaker than the built DAG — it cannot see edges
# created by anything the extractor does not model, which is why
# `dag_dependency_edges` RAISES on constructs it does not understand rather
# than returning a quietly incomplete graph.
#
# REACHABILITY, NOT EXACT SEQUENCE
# --------------------------------
# `REQUIRED_ORDERING` is asserted as "b is reachable from a", not as adjacency
# and not as an exact task sequence. The distinction is the whole point: the
# constraints below are the SEMANTIC ones (you cannot transform data you have
# not loaded), so inserting a task between two of them, or running `check_slos`
# and `upstream_stall_check` in parallel off `dbt_build`, is a harmless change
# and must stay green. Reversing two of them is a data-corruption bug and must
# go red. An exact-sequence equality would conflate the two and get itself
# deleted the first time someone parallelised anything.

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

# The only task that may have no upstream. Anything else without an upstream is
# an orphan that fires at DAG start — mutation 2 above.
DAG_ROOT_TASKS = {"check_source"}

# Dependency helpers that create edges this AST reader cannot see. Their
# presence makes the reconstructed graph a lie, so it refuses to return one.
UNMODELLED_DEPENDENCY_HELPERS = {"chain", "chain_linear", "cross_downstream"}


def _task_ids_by_variable(tree):
    """Map `x` -> "the_task_id" for every `x = SomeOperator(task_id="...")`.

    The `>>` chains reference python variables, not task_ids. Resolving through
    this map means a variable renamed without its task_id (or vice versa) is
    caught rather than silently producing edges between names that don't exist.
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

    Reads the SOURCE, not a DAG object built by Airflow — see the section
    comment above for why. Raises rather than returning an empty or partial
    graph, so a missing file, a deleted dependency block, or a dependency
    helper this reader cannot model is a test FAILURE and never a silent pass.
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
    """Anti-vacuity guard for every ordering assertion below.

    Those assertions are all of the form "b is reachable from a". On an empty
    graph that form fails, but only because reachability is empty — which is
    the right answer for the wrong reason and would not survive someone
    "fixing" a test by relaxing it. This states the precondition directly: the
    file exists, all seven tasks resolve, and every one of them appears in the
    edge list.
    """
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
    """`downstream` must be reachable from `upstream` in the real edge list.

    Reachability, not adjacency: inserting a task between the two is fine,
    reversing them is not. See the section comment for the distinction.
    """
    _, edges = dag_dependency_edges()
    reachable = _reachable_from(edges, upstream)
    assert downstream in reachable, (
        f"'{downstream}' does not run after '{upstream}' in nyc311_local. "
        f"Reachable from '{upstream}': {sorted(reachable) or 'nothing'}. "
        f"Edges: {sorted(edges)}"
    )


def test_local_dag_has_no_orphaned_tasks():
    """Only `check_source` may start with no upstream.

    A task with no upstream is not "unordered", it is scheduled at DAG start.
    Deleting one `>>` is enough to make `check_slos` evaluate SLOs against the
    previous run's warehouse, on a green DAG.
    """
    _, edges = dag_dependency_edges()
    has_upstream = {downstream for _, downstream in edges}
    roots = {t for t in EXPECTED_TASKS if t not in has_upstream}
    assert roots == DAG_ROOT_TASKS, (
        f"Tasks with no upstream should be exactly {sorted(DAG_ROOT_TASKS)}, "
        f"but are {sorted(roots)}. Extra roots fire at DAG start."
    )


def test_local_dag_is_acyclic():
    """Airflow rejects a cyclic DAG at parse time — but nothing here parses it
    with Airflow, so the cycle would only surface on the scheduler."""
    _, edges = dag_dependency_edges()
    cyclic = [t for t in EXPECTED_TASKS if t in _reachable_from(edges, t)]
    assert not cyclic, (
        f"Task(s) {cyclic} are reachable from themselves — the DAG has a cycle "
        f"and Airflow will refuse to import it. Edges: {sorted(edges)}"
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
    BRONZE. Selected by CONTENT (role reference + Bronze reference), never by
    resource name, so renaming or adding a resource cannot drop it from the
    guard's coverage."""
    main_path = os.path.join(TERRAFORM_DIR, "modules", "snowflake-foundation", "main.tf")
    with open(main_path) as f:
        content = f.read()

    found = []
    for header, body in hcl_top_level_blocks(content):
        if not header.startswith('resource "snowflake_grant'):
            continue
        # Strip comments before matching: the prose above these resources talks
        # about TRUNCATE and Bronze at length, and must not be evidence.
        code = "\n".join(line.split("#", 1)[0] for line in body.splitlines())
        if "snowflake_role.loader" not in code:
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

    Scope: EVERY LOADER grant touching Bronze, current tables and future,
    matched on the role and schema the block references rather than on one
    hand-written resource name. The predecessor scanned a single named resource
    line by line and had three holes, each proven inert before this replaced it:

      1. It ended the block at the first line equal to `}` — the closing brace
         of the NESTED `future { ... }` block — so a `privileges` list written
         after `on_schema_object` (legal HCL; argument order is free) was never
         read. Verified: a LOADER TRUNCATE grant written that way PASSED.
      2. Renaming the resource made the scan match nothing and pass having
         examined no lines at all. Verified: renamed + TRUNCATE granted PASSED.
      3. It matched the literal word TRUNCATE, so `ALL PRIVILEGES` on Bronze's
         CURRENT tables — a grant the old guard did not look at in any case —
         went straight through. Verified: PASSED.

    Vacuity is now fatal: if no LOADER-on-Bronze table grant is found at all,
    this fails rather than passes, because "found nothing to check" and "checked
    and found it clean" must never produce the same colour.

    KNOWN LIMIT, recorded rather than quietly fixed. loader_bronze_schema grants
    LOADER `CREATE TABLE` on BRONZE, so LOADER OWNS every Bronze table it
    creates — and an owner holds the object with grant option and can grant
    itself TRUNCATE. The append-only property is therefore weaker than "no
    TRUNCATE grant exists" makes it sound. Closing that means moving table
    creation to another role, which is an infrastructure decision, not a test
    change; this guard states the gap instead of implying it is covered.
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
