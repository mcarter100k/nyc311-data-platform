#!/usr/bin/env python3
"""
check_claims.py — CI guard that keeps README.md true.

The README states each count exactly once, wrapped in a machine-readable marker:

    <!--claim:NAME-->VALUE<!--/claim-->

This script recomputes each NAME from the repo and fails (exit 1) on mismatch,
so a drifted count fails the build instead of shipping. It also verifies that
every relative markdown link in README.md resolves to a file on disk.

Checked claims:
    test_count   pytest collection count of tests/ (structural, via pytest
                 --collect-only) + AST count of test functions in tests/unit
                 and tests/local (that tier skips wholesale when dbt-duckdb is
                 absent, so collection alone would undercount and vary by
                 environment)
    adr_count    markdown files in docs/adr/
    adr table    every ADR on disk has a row in the README table (the count
                 marker alone let the table fall 2 ADRs behind)
    dag tasks    README task count == DAG operators == EXPECTED_TASKS in tests
    tf counts    README schema/role counts == the Terraform module
    fct_models   dbt/models/marts/fct_*.sql files

Run:  python scripts/check_claims.py
"""

import ast
import glob
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
README = os.path.join(ROOT, "README.md")

MARKER_RE = re.compile(r"<!--claim:([a-z_]+)-->(.*?)<!--/claim-->", re.S)
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")


def structural_test_count() -> int:
    out = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "--ignore=tests/unit",
         "--ignore=tests/local", "--collect-only", "-q"],
        cwd=ROOT, capture_output=True, text=True,
    )
    m = re.search(r"(\d+) tests? collected", out.stdout)
    if not m:
        print(f"ERROR: could not parse pytest collection output:\n{out.stdout[-500:]}")
        sys.exit(2)
    return int(m.group(1))


def skippable_tier_test_count() -> int:
    # Counted via AST, not pytest collection: without dbt-duckdb
    # installed these modules are reported as a single skip and their tests
    # never appear in the collection count — which would also make the total
    # depend on what happens to be installed.
    total = 0
    for path in (glob.glob(os.path.join(ROOT, "tests", "unit", "test_*.py"))
                 + glob.glob(os.path.join(ROOT, "tests", "local", "test_*.py"))):
        tree = ast.parse(open(path).read(), filename=path)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
                n = 1
                for deco in node.decorator_list:
                    # Expand pytest.mark.parametrize first-arg lists so a future
                    # parametrized unit test is counted the way pytest counts it.
                    if (isinstance(deco, ast.Call)
                            and getattr(deco.func, "attr", "") == "parametrize"
                            and len(deco.args) >= 2
                            and isinstance(deco.args[1], (ast.List, ast.Tuple))):
                        n *= max(1, len(deco.args[1].elts))
                total += n
    return total


def expected_values() -> dict:
    return {
        "test_count": structural_test_count() + skippable_tier_test_count(),
        "adr_count": len(glob.glob(os.path.join(ROOT, "docs", "adr", "*.md"))),
        "fct_models": len(glob.glob(os.path.join(ROOT, "dbt", "models", "marts", "fct_*.sql"))),
    }


def check_markers(readme_text: str, expected: dict) -> list:
    errors = []
    found = {}
    for name, value in MARKER_RE.findall(readme_text):
        found.setdefault(name, []).append(value.strip())

    for name, values in found.items():
        if name not in expected:
            errors.append(f"README marker 'claim:{name}' has no computed source in this script")
            continue
        if len(values) > 1:
            errors.append(f"claim:{name} appears {len(values)} times — a number is stated once")
        if values[0] != str(expected[name]):
            errors.append(
                f"claim:{name} — README says {values[0]!r}, repo says {expected[name]}"
            )

    for name in expected:
        if name not in found:
            errors.append(f"claim:{name} marker missing from README")
    return errors


SLO_DOC = os.path.join(ROOT, "docs", "SLO.md")
SLO_BLOCK_RE = re.compile(r"<!--slo-sql:([^\s>]+)-->\s*```sql\n(.*?)```", re.S)


def check_slo_doc_sync() -> list:
    # docs/SLO.md reproduces the queries whose executable form lives in
    # scripts/slo/. Two copies is the numbers-in-six-places bug for SQL —
    # tolerated only because this check makes them provably identical.
    if not os.path.exists(SLO_DOC):
        return []
    errors = []
    doc = open(SLO_DOC).read()
    blocks = SLO_BLOCK_RE.findall(doc)
    if not blocks:
        errors.append("docs/SLO.md has no <!--slo-sql:...--> guarded blocks")
    for rel_path, doc_sql in blocks:
        full = os.path.join(ROOT, rel_path)
        if not os.path.exists(full):
            errors.append(f"SLO doc references missing file: {rel_path}")
            continue
        if doc_sql.strip() != open(full).read().strip():
            errors.append(
                f"SLO drift: the query shown in docs/SLO.md differs from {rel_path} "
                f"— edit the .sql file and re-paste it into the doc"
            )
    return errors


def check_links(readme_text: str) -> list:
    errors = []
    for target in LINK_RE.findall(readme_text):
        if target.startswith(("http://", "https://", "mailto:", "#")):
            continue
        path = target.split("#")[0]
        if not os.path.exists(os.path.join(ROOT, path)):
            errors.append(f"broken relative link: {target}")
    return errors


def check_adr_table(readme_text: str) -> list:
    """Every ADR on disk must have a row in the README's ADR TABLE.

    The adr_count marker only checks the DIRECTORY count. That let the table
    fall two ADRs behind while the count claim stayed green: 11 files on disk,
    9 rows in the table. Counting a directory is not the same as documenting it.

    Scoped to the table block on purpose: an earlier version searched the whole
    README and was vacuous, because ADRs are also linked from prose elsewhere —
    deleting a table row left those references and the check still passed.
    """
    errors = []
    start = readme_text.find("## Architecture Decision Records")
    if start == -1:
        return ["README has no '## Architecture Decision Records' section"]
    end = readme_text.find("\n## ", start + 1)
    table = readme_text[start: end if end != -1 else len(readme_text)]

    for path in sorted(glob.glob(os.path.join(ROOT, "docs", "adr", "*.md"))):
        fname = os.path.basename(path)
        if f"docs/adr/{fname}" not in table:
            errors.append(
                f"ADR {fname.split('-')[0]} ({fname}) has no row in the README ADR "
                f"table — the table is behind the docs/adr/ directory"
            )
    return errors


def check_airflow_task_count(readme_text: str) -> list:
    """DAG operators == EXPECTED_TASKS == the count stated for the cloud DAG.

    The code-to-code half (DAG vs test) is the load-bearing one: it cannot be
    satisfied by prose. The README half is matched against the specific
    'N-task cloud DAG' phrase rather than a bare 'N-task' substring, which an
    earlier version used and which was vacuous — the repo tree names two DAGs
    and the other line kept the check green.
    """
    errors = []
    dag = open(os.path.join(ROOT, "airflow", "dags", "nyc311_local.py")).read()
    n_dag = len(re.findall(r"task_id\s*=\s*[\"']", dag))

    tests = open(os.path.join(ROOT, "tests", "test_pipeline_components.py")).read()
    block = re.search(r"EXPECTED_TASKS\s*=\s*\[(.*?)\]", tests, re.DOTALL)
    n_test = len(re.findall(r"[\"'][a-z_]+[\"']", block.group(1))) if block else -1

    if n_dag != n_test:
        errors.append(
            f"Airflow task count mismatch: nyc311_local.py defines {n_dag} tasks, "
            f"EXPECTED_TASKS lists {n_test}"
        )
    if f"{n_dag}-task DAG" not in readme_text:
        errors.append(
            f"README does not state '{n_dag}-task DAG' — the DAG defines {n_dag} tasks"
        )
    return errors


def check_terraform_counts(readme_text: str) -> list:
    """Schema / role counts claimed in the README vs the Terraform module."""
    errors = []
    tf = open(os.path.join(
        ROOT, "terraform", "modules", "snowflake-foundation", "main.tf")).read()
    for label, pattern, phrase in (
        ("schemas", r'^resource "snowflake_schema"', "{n} schemas"),
        ("roles", r'^resource "snowflake_role"', "{n} roles"),
    ):
        n = len(re.findall(pattern, tf, re.MULTILINE))
        if phrase.format(n=n) not in readme_text:
            errors.append(
                f"README does not state '{phrase.format(n=n)}' — Terraform defines {n} {label}"
            )
    return errors


def main() -> int:
    readme_text = open(README).read()
    expected = expected_values()
    errors = (check_markers(readme_text, expected) + check_links(readme_text)
              + check_slo_doc_sync() + check_adr_table(readme_text)
              + check_airflow_task_count(readme_text)
              + check_terraform_counts(readme_text))

    if errors:
        print("README claim check FAILED:")
        for e in errors:
            print(f"  ✗ {e}")
        return 1

    for name, value in expected.items():
        print(f"  ✓ claim:{name} = {value}")
    print(f"  ✓ all relative README links resolve")
    print("README claim check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
