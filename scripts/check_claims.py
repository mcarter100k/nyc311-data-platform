#!/usr/bin/env python3
"""
check_claims.py — CI guard that keeps README.md AND docs/ true.

Scope
-----
This script used to open exactly two files: README.md and docs/SLO.md. Everything
else under docs/ drifted unchecked, and that is precisely where the August 2026
audit found the worst defects — an ARCHITECTURE DAG diagram naming four tasks
that do not exist, a model inventory missing three models, and a claims register
whose line-number citations pointed at unrelated code.

The guarded set is now README.md plus every markdown file under docs/, including
docs/adr/ and docs/postmortems/.

The ADR carve-out, and its limits
---------------------------------
An ADR is a historical record of a decision AT A TIME. Checks that ask "does this
prose describe the code as it is TODAY" must exempt docs/adr/, or a truthful
record of a removed subsystem becomes a build failure:

    exempt   check_superseded_claims   (a superseded claim inside an ADR is history)
    exempt   check_path_spans          (ADR 010 cites databricks/, deleted 2026-08-20)
    exempt   check_citations           (same reason: the cited code may be gone)

Checks that ask "is this document mechanically intact" apply everywhere,
ADRs included — a link a reader cannot follow is broken regardless of when it
was written:

    universal  check_links             (relative targets resolve, fragments exist)
    universal  check_markers           (a marker is a live claim by definition)
    universal  check_orphan_anchors    (an anchor nothing links to is dead weight)

Marker claims
-------------
A count is stated in the docs exactly once per site, wrapped in a marker:

    <!--claim:NAME-->VALUE<!--/claim-->

Recomputed here; a mismatch fails the build. Repetition across sites is allowed
(and across files) because every copy is provably equal to the computed value —
the old "a number is stated once" rule was what kept ARCHITECTURE.md's mermaid
diagram saying "3 facts" while the README's guarded marker said 4. Provable
agreement beats enforced scarcity.

    test_count        pytest structural collection + AST count of the tiers that
                      skip wholesale without dbt-duckdb
    structural_test_count / unit_test_count / behavioral_test_count
                      the three tiers, which must sum to test_count
    adr_count         markdown files in docs/adr/
    fct_models        dbt/models/marts/fct_*.sql
    dim_models        dbt/models/marts/dim_*.sql
    dbt_test_count / dbt_generic_tests / dbt_singular_tests
                      read from the dbt manifest (see "Manifest dependency")

Citations
---------
    `path/to/file.ext#"a unique string from that file"`

The string must appear EXACTLY ONCE in the named file. Zero matches or several
are both failures. This replaced `path:118-128`, where six of seven citations in
docs/CLAIMS.md had rotted onto unrelated code and nothing could see it: the old
check_links() did `target.split("#")[0]` and only asserted the FILE existed.
Line numbers rot on every edit above them; a unique string moves with the code it
names, so the register heals itself instead of decaying.

A line-number fragment in a markdown link (`main.tf#L471-L481`) is rejected for
the same reason, with a pointer to this form.

Other checks
------------
    slo doc sync      docs/SLO.md reproduces scripts/slo/*.sql byte-identically
    adr table         every ADR on disk has a row in the README's ADR table
    dag tasks         DAG operators == EXPECTED_TASKS == the task NAMES documented
                      in docs/ARCHITECTURE.md == the count stated in the README
    model inventory   every model in the manifest is listed in ARCHITECTURE.md's
                      marked inventory block, and nothing else is
    star counts       ARCHITECTURE.md's mermaid label matches the marts on disk
    tf counts         README schema/role counts == the Terraform module
    path spans        every `dir/file.ext` code span resolves; `file.py::symbol`
                      spans name a function or class that exists
    superseded        registered dead claims have not reappeared

Manifest dependency
-------------------
Three claims and the model inventory read dbt/target/manifest.json. When it is
absent this script FAILS with exit code 2 and the exact command to produce it,
rather than skipping with a warning.

Why fail rather than skip: the number these claims guard ("N dbt data tests")
rotted twice through merges as a bare literal while the marker-guarded counts
caught every drift. A warning printed to stdout inside a green CI job reproduces
exactly that failure mode — nobody reads it. Exit code 2 keeps "I could not
check" distinguishable from exit 1, "the docs are wrong", so a red build still
names its own cause. CI's fast-gate runs `dbt parse` before this script, and
run_tests.sh rebuilds the manifest in step 1, so the only caller who can hit
this is a developer on a cold checkout — who gets a one-line fix instruction.

Run:  python scripts/check_claims.py
"""

import ast
import glob
import json
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
README = os.path.join(ROOT, "README.md")
ARCHITECTURE = os.path.join(ROOT, "docs", "ARCHITECTURE.md")
SLO_DOC = os.path.join(ROOT, "docs", "SLO.md")
MANIFEST = os.path.join(ROOT, "dbt", "target", "manifest.json")
DAG_PATH = os.path.join(ROOT, "airflow", "dags", "nyc311_local.py")

MANIFEST_HINT = (
    "cd dbt && dbt deps --profiles-dir ci-profile --project-dir . --no-version-check "
    "&& dbt parse --profiles-dir ci-profile --project-dir . --target ci --no-version-check"
)

MARKER_RE = re.compile(r"<!--claim:([a-z_]+)-->(.*?)<!--/claim-->", re.DOTALL)
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
CODE_SPAN_RE = re.compile(r"`([^`\n]+)`")
# `path/to/file.ext#"unique string"` — the path may not contain whitespace, a
# '#' or a quote; the anchor is everything between the first quote and the last.
CITATION_RE = re.compile(r'^([^\s#"]+)#"(.+)"$')
# `dir/file.ext` or `dir/file.py::symbol` — at least one slash and an extension,
# so prose like `_loaded_at` or `fct_service_requests` is not mistaken for a path.
PATH_SPAN_RE = re.compile(
    r"^[A-Za-z0-9_.][A-Za-z0-9_./-]*/[A-Za-z0-9_.-]+\.[A-Za-z0-9]{1,5}(?:::([A-Za-z0-9_]+))?$"
)
ANCHOR_TAG_RE = re.compile(r'<a\s+name="([^"]+)"')
HEADING_RE = re.compile(r"^#{1,6}\s+(.*?)\s*$", re.MULTILINE)


# ── Document set ──────────────────────────────────────────────────────────────

def doc_files() -> list:
    """README.md plus every markdown file under docs/, as repo-relative paths.

    Deliberately not local/README_LOCAL.md or terraform/github/README.md: those
    are directory-local operating notes, not the claim surface the README points
    a reader at. Widening the set is a one-line change here if that stops being
    true.
    """
    docs = sorted(
        os.path.relpath(p, ROOT)
        for p in glob.glob(os.path.join(ROOT, "docs", "**", "*.md"), recursive=True)
    )
    return ["README.md"] + docs


def is_adr(rel_path: str) -> bool:
    return rel_path.replace(os.sep, "/").startswith("docs/adr/")


def read(rel_path: str) -> str:
    with open(os.path.join(ROOT, rel_path)) as fh:
        return fh.read()


# ── Manifest ──────────────────────────────────────────────────────────────────

def load_manifest():
    """The parsed dbt manifest, or None when it has not been built."""
    if not os.path.exists(MANIFEST):
        return None
    with open(MANIFEST) as fh:
        return json.load(fh)


def manifest_model_names(manifest) -> list:
    return sorted(
        node["name"]
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model"
    )


def manifest_test_counts(manifest) -> tuple:
    """(total, generic, singular). Generic tests carry test_metadata; singular
    tests are the hand-written .sql files under dbt/tests/, which do not."""
    tests = [n for n in manifest["nodes"].values() if n["resource_type"] == "test"]
    generic = [t for t in tests if t.get("test_metadata")]
    return len(tests), len(generic), len(tests) - len(generic)


# ── Test counts ───────────────────────────────────────────────────────────────

def structural_test_count() -> int:
    out = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "--ignore=tests/unit",
         "--ignore=tests/local", "--collect-only", "-q"],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    m = re.search(r"(\d+) tests? collected", out.stdout)
    if not m:
        print(f"ERROR: could not parse pytest collection output:\n{out.stdout[-500:]}")
        sys.exit(2)
    return int(m.group(1))


def ast_test_count(directory: str) -> int:
    # Counted via AST, not pytest collection: without dbt-duckdb installed these
    # modules are reported as a single skip and their tests never appear in the
    # collection count — which would also make the total depend on what happens
    # to be installed.
    total = 0
    for path in sorted(glob.glob(os.path.join(ROOT, directory, "test_*.py"))):
        tree = ast.parse(open(path).read(), filename=path)

        # Module-level list/tuple constants, so a parametrize over a NAMED list
        # expands too. Only expanding inline literals silently undercounted:
        # `@parametrize("m", LOCAL_MODULES)` scored 1 instead of len(LOCAL_MODULES),
        # and the failure mode is a claim-checker that is quietly wrong about the
        # number it exists to police.
        # Sets count too: `@parametrize("s", sorted(HTTP_RETRYABLE_STATUS))` where
        # the constant is a set literal. Ignoring Set undercounted the behavioral
        # tier by 4 and the marker was set to the WRONG value to match — a
        # claim-checker confidently wrong about the number it exists to police,
        # which is the third time this counter has had that bug.
        constants = {}
        for node in tree.body:
            if isinstance(node, ast.Assign) and isinstance(node.value, (ast.List, ast.Tuple, ast.Set)):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        constants[target.id] = len(node.value.elts)

        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
                n = 1
                for deco in node.decorator_list:
                    # Expand pytest.mark.parametrize argument lists so a
                    # parametrized test is counted the way pytest counts it.
                    if not (isinstance(deco, ast.Call)
                            and getattr(deco.func, "attr", "") == "parametrize"
                            and len(deco.args) >= 2):
                        continue
                    arg = deco.args[1]
                    # Unwrap a builtin that merely reorders/retypes its argument.
                    # `sorted(NAME)` is a Call, not a Name, so the plain lookup
                    # below missed it entirely and scored 1.
                    if (isinstance(arg, ast.Call)
                            and isinstance(arg.func, ast.Name)
                            and arg.func.id in {"sorted", "list", "tuple", "set", "reversed"}
                            and len(arg.args) == 1):
                        arg = arg.args[0]
                    if isinstance(arg, (ast.List, ast.Tuple, ast.Set)):
                        n *= max(1, len(arg.elts))
                    elif isinstance(arg, ast.Name) and arg.id in constants:
                        n *= max(1, constants[arg.id])
                    else:
                        # UNRESOLVABLE — refuse to guess.
                        #
                        # This counter has been silently wrong three times: over
                        # a named list, over a set literal, and over a constant
                        # IMPORTED from another module, which this file's AST
                        # cannot see. Each time it scored 1, and each time the
                        # README marker was set to match the wrong value — a
                        # checker confidently wrong about the one number it
                        # exists to police.
                        #
                        # Silently undercounting is the worst behaviour
                        # available. Refuse, and name the remedy — the same
                        # principle as exiting 2 on a missing manifest rather
                        # than warning inside a green job.
                        raise SystemExit(
                            f"claim check CANNOT COUNT: "
                            f"{os.path.relpath(path, ROOT)}:{deco.lineno}\n"
                            f"  parametrize argument is neither a literal nor a "
                            f"module-level constant in this file, so its cases "
                            f"cannot be counted from the AST.\n"
                            f"  Bind it to a module-level name in this file, or "
                            f"inline the list."
                        )
                total += n
    return total


def expected_values(manifest) -> dict:
    structural = structural_test_count()
    unit = ast_test_count(os.path.join("tests", "unit"))
    behavioral = ast_test_count(os.path.join("tests", "local"))
    total_dbt, generic_dbt, singular_dbt = manifest_test_counts(manifest)
    return {
        "test_count": structural + unit + behavioral,
        "structural_test_count": structural,
        "unit_test_count": unit,
        "behavioral_test_count": behavioral,
        "adr_count": len(glob.glob(os.path.join(ROOT, "docs", "adr", "*.md"))),
        "fct_models": len(glob.glob(os.path.join(ROOT, "dbt", "models", "marts", "fct_*.sql"))),
        "dim_models": len(glob.glob(os.path.join(ROOT, "dbt", "models", "marts", "dim_*.sql"))),
        "dbt_test_count": total_dbt,
        "dbt_generic_tests": generic_dbt,
        "dbt_singular_tests": singular_dbt,
    }


# ── Markers ───────────────────────────────────────────────────────────────────

def check_markers(docs: list, expected: dict) -> list:
    """Every marker in every guarded document agrees with the computed value.

    Repetition is legal — see the module docstring. What is not legal is a
    marker whose name has no computed source here, because that reads as
    guarded and is not.
    """
    errors = []
    seen = set()
    for rel in docs:
        for name, value in MARKER_RE.findall(read(rel)):
            seen.add(name)
            if name not in expected:
                errors.append(
                    f"{rel}: marker 'claim:{name}' has no computed source in "
                    f"check_claims.py — it looks guarded and is not"
                )
                continue
            if value.strip() != str(expected[name]):
                errors.append(
                    f"{rel}: claim:{name} — doc says {value.strip()!r}, "
                    f"repo says {expected[name]}"
                )

    for name in expected:
        if name not in seen:
            errors.append(
                f"claim:{name} is computed but stated nowhere — add a "
                f"<!--claim:{name}-->{expected[name]}<!--/claim--> marker or drop the claim"
            )

    # Code-to-code: the tier table and the headline total cannot disagree.
    tier_keys = ("structural_test_count", "unit_test_count", "behavioral_test_count")
    if all(k in expected for k in tier_keys) and "test_count" in expected:
        tiers = sum(expected[k] for k in tier_keys)
        if tiers != expected["test_count"]:
            errors.append(
                f"internal: tier counts sum to {tiers}, test_count is "
                f"{expected['test_count']}"
            )
    return errors


# ── Links and fragments ───────────────────────────────────────────────────────

def slugify(heading: str) -> str:
    """GitHub's heading-anchor slug, close enough for the anchors this repo uses."""
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", heading)   # link text only
    text = text.replace("`", "")
    # Emphasis markers only. Underscore is NOT stripped: GitHub keeps it, and
    # stripping it turned `fct_service_requests` into `fctservicerequests`.
    text = re.sub(r"[*~]", "", text)
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    # Per character, not per run: GitHub maps each whitespace character to one
    # hyphen, so a heading with a doubled space has a doubled hyphen in its slug.
    return re.sub(r"\s", "-", text)


def anchors_in(text: str) -> set:
    names = {slugify(h) for h in HEADING_RE.findall(text)}
    names |= set(ANCHOR_TAG_RE.findall(text))
    return names


def check_links(docs: list) -> list:
    """Relative targets resolve, and markdown fragments name something real.

    Resolution is relative to the LINKING FILE, not the repo root. The old
    version rooted every path at ROOT, which happened to work because it only
    ever read README.md; docs/ARCHITECTURE.md's `adr/008-...md` would have been
    reported broken the moment the set widened.
    """
    errors = []
    anchor_cache = {}
    for rel in docs:
        base = os.path.dirname(rel)
        text = read(rel)
        for target in LINK_RE.findall(text):
            if target.startswith(("http://", "https://", "mailto:")):
                continue

            path, _, frag = target.partition("#")
            if path:
                resolved = os.path.normpath(os.path.join(base, path))
                if not os.path.exists(os.path.join(ROOT, resolved)):
                    errors.append(f"{rel}: broken relative link: {target}")
                    continue
            else:
                resolved = rel  # bare "#fragment" — same document

            if not frag:
                continue

            if resolved.endswith(".md"):
                if resolved not in anchor_cache:
                    anchor_cache[resolved] = anchors_in(read(resolved))
                if frag not in anchor_cache[resolved]:
                    errors.append(
                        f"{rel}: link {target} points at a fragment that does not "
                        f"exist in {resolved}"
                    )
            elif re.fullmatch(r"L\d+(-L\d+)?", frag):
                errors.append(
                    f"{rel}: link {target} cites LINE NUMBERS, which rot on every "
                    f'edit above them — cite `{path}#"a unique string"` instead'
                )
    return errors


def check_orphan_anchors(docs: list) -> list:
    """An <a name="x"></a> nothing links to is dead weight, and usually the
    residue of a deleted section.

    docs/ARCHITECTURE.md carried `<a name="test-suite"></a>` with no section
    under it and no link to it. check_links could never see that: it is not a
    broken link, it is a broken destination.
    """
    referenced = set()
    for rel in docs:
        base = os.path.dirname(rel)
        for target in LINK_RE.findall(read(rel)):
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            path, _, frag = target.partition("#")
            if not frag:
                continue
            resolved = os.path.normpath(os.path.join(base, path)) if path else rel
            referenced.add((resolved, frag))

    errors = []
    for rel in docs:
        for name in ANCHOR_TAG_RE.findall(read(rel)):
            if (rel, name) not in referenced:
                errors.append(
                    f'{rel}: <a name="{name}"> is linked from nowhere — delete the '
                    f"anchor or link to it"
                )
    return errors


# ── Citations ─────────────────────────────────────────────────────────────────

def check_citations(docs: list) -> list:
    """`path#"unique string"` — the string must appear exactly once in the file."""
    errors = []
    checked = 0
    for rel in docs:
        if is_adr(rel):
            continue
        for span in CODE_SPAN_RE.findall(read(rel)):
            m = CITATION_RE.match(span)
            if not m:
                continue
            path, anchor = m.group(1), m.group(2)
            checked += 1
            full = os.path.join(ROOT, path)
            if not os.path.exists(full):
                errors.append(f"{rel}: citation names a missing file: {path}")
                continue
            n = open(full).read().count(anchor)
            if n != 1:
                errors.append(
                    f"{rel}: citation `{path}#\"{anchor}\"` matches {n} times "
                    f"(must be exactly 1) — the code moved or the anchor is not unique"
                )
    if not checked:
        errors.append(
            "no `path#\"anchor\"` citations found in the guarded docs — the "
            "citation checker is vacuous"
        )
    return errors


def check_path_spans(docs: list) -> list:
    """Every `dir/file.ext` code span resolves; `file.py::symbol` names a real
    function or class.

    This is the other half of docs/CLAIMS.md: the register's second column
    names a verifying test, and nothing checked that the test existed. It
    cited `tests/test_pipeline_components.py::test_airflow_dag_uses_write_audit_publish`,
    which does not.
    """
    errors = []
    for rel in docs:
        if is_adr(rel):
            continue
        for span in CODE_SPAN_RE.findall(read(rel)):
            m = PATH_SPAN_RE.match(span)
            if not m:
                continue
            path, _, symbol = span.partition("::")
            if not os.path.exists(os.path.join(ROOT, path)):
                errors.append(f"{rel}: `{span}` names a path that does not exist")
                continue
            if not symbol:
                continue
            if not path.endswith(".py"):
                errors.append(f"{rel}: `{span}` uses ::symbol on a non-Python file")
                continue
            source = open(os.path.join(ROOT, path)).read()
            if not re.search(rf"^\s*(?:async\s+)?(?:def|class)\s+{re.escape(symbol)}\b",
                             source, re.MULTILINE):
                errors.append(
                    f"{rel}: `{span}` — {path} defines no {symbol}"
                )
    return errors


# ── SLO doc ───────────────────────────────────────────────────────────────────

SLO_BLOCK_RE = re.compile(r"<!--slo-sql:([^\s>]+)-->\s*```sql\n(.*?)```", re.DOTALL)


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


# ── README structure ──────────────────────────────────────────────────────────

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


DAG_DOC_RE = re.compile(
    r"<!--dag-tasks:([^\s>]+)-->\s*```\n(.*?)```", re.DOTALL
)


def check_dag_tasks(readme_text: str, arch_text: str) -> list:
    """DAG operators == EXPECTED_TASKS == the NAMES documented in ARCHITECTURE.md
    == the count stated in the README.

    The count half of this check was the whole check, and it passed while
    docs/ARCHITECTURE.md named `check_api_availability → ingest_raw → ... →
    dbt_publish → notify_success`: four names wrong, two of them tasks that have
    never existed, and the total right. Counting seven of anything is not
    evidence that the seven are these seven.
    """
    errors = []
    dag = open(DAG_PATH).read()
    dag_tasks = re.findall(r"task_id\s*=\s*[\"']([a-z_]+)[\"']", dag)

    tests = open(os.path.join(ROOT, "tests", "test_pipeline_components.py")).read()
    block = re.search(r"EXPECTED_TASKS\s*=\s*\[(.*?)\]", tests, re.DOTALL)
    test_tasks = re.findall(r"[\"']([a-z_]+)[\"']", block.group(1)) if block else []

    if dag_tasks != test_tasks:
        errors.append(
            f"Airflow task mismatch: nyc311_local.py defines {dag_tasks}, "
            f"EXPECTED_TASKS lists {test_tasks}"
        )

    doc = DAG_DOC_RE.search(arch_text)
    if not doc:
        errors.append(
            "docs/ARCHITECTURE.md has no <!--dag-tasks:airflow/dags/nyc311_local.py--> "
            "guarded block — the documented task chain is unverifiable"
        )
    else:
        if doc.group(1) != "airflow/dags/nyc311_local.py":
            errors.append(
                f"docs/ARCHITECTURE.md dag-tasks marker names {doc.group(1)}, "
                f"but this check reads airflow/dags/nyc311_local.py"
            )
        documented = [t.strip() for t in re.split(r"→|->", doc.group(2)) if t.strip()]
        if documented != dag_tasks:
            errors.append(
                f"docs/ARCHITECTURE.md documents the task chain as {documented}, "
                f"the DAG defines {dag_tasks}"
            )

    n_dag = len(dag_tasks)
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


# ── ARCHITECTURE.md ───────────────────────────────────────────────────────────

INVENTORY_RE = re.compile(
    r"<!--model-inventory-->(.*?)<!--/model-inventory-->", re.DOTALL
)
INVENTORY_ITEM_RE = re.compile(r"^-\s+`([a-z0-9_]+)`", re.MULTILINE)


def check_model_inventory(arch_text: str, manifest) -> list:
    """Every model in the manifest is listed in ARCHITECTURE.md, and nothing else.

    Scoped to a marked block for the same reason check_adr_table is: searching
    the whole document would pass on a model merely mentioned in passing prose,
    which is how `fct_data_quality`, `fct_complaint_recurrence` and
    `int_load_completeness` went unlisted while the page still read as complete.
    """
    block = INVENTORY_RE.search(arch_text)
    if not block:
        return ["docs/ARCHITECTURE.md has no <!--model-inventory--> block"]

    documented = set(INVENTORY_ITEM_RE.findall(block.group(1)))
    actual = set(manifest_model_names(manifest))

    errors = []
    for name in sorted(actual - documented):
        errors.append(
            f"docs/ARCHITECTURE.md model inventory omits `{name}` — it is in the "
            f"dbt manifest"
        )
    for name in sorted(documented - actual):
        errors.append(
            f"docs/ARCHITECTURE.md model inventory lists `{name}`, which is not a "
            f"model in the dbt manifest"
        )
    return errors


def check_star_counts(arch_text: str, expected: dict) -> list:
    """The mermaid marts node states the fact/dimension counts on disk.

    A <!--claim:--> marker cannot be used inside a mermaid label: the diagram
    renderer would print the HTML comment. So the phrase is matched instead,
    which is the same technique check_terraform_counts uses on the README.
    """
    phrase = f"{expected['fct_models']} facts · {expected['dim_models']} dims"
    if phrase not in arch_text:
        return [
            f"docs/ARCHITECTURE.md's mermaid diagram does not say '{phrase}' — "
            f"dbt/models/marts holds {expected['fct_models']} fct_*.sql and "
            f"{expected['dim_models']} dim_*.sql"
        ]
    return []


# ── Superseded claims ─────────────────────────────────────────────────────────

# Claims that were true once, were superseded by a later decision, and must not
# come back. Every entry here was found in MERGED documentation by a review that
# read the prose against the code — seven of them in one pass, none of which any
# automated check could see.
#
# This is a tripwire, not a contradiction detector. It cannot tell that two
# pages disagree; it only knows that these specific sentences describe a system
# that no longer exists. That is a narrow guarantee, and it is the one that
# would have caught every finding in that review.
#
# Adding an entry is part of superseding a claim: when a redesign makes a
# sentence false, register the sentence so it cannot quietly return — the same
# principle as model_drift_baseline.json, applied to prose.
SUPERSEDED_CLAIMS = [
    (
        "Source-side staleness is SLO-2",
        "SLO-2 became a source reconciliation (#24); it answers whether WE loaded "
        "what the city published, and passes 372/372 on a stalled source. "
        "Source staleness is the non-gating warning — see ADR 013.",
    ),
    (
        "proven by this incident, twice",
        "slo2_completeness.sql detected the Aug 2026 incident in its OLD form. "
        "The current file passes on that same incident class; the detector is "
        "now check_upstream_stall.py.",
    ),
    (
        "publish once daily at roughly 02:20 UTC carrying data up to that moment",
        "Falsified by direct probes on 2026-08-20 and 2026-08-22: each publish "
        "lands ~01:40 carrying data only to ~02:05 the PREVIOUS day, a ~23.5h lag.",
    ),
    (
        "The watermark now keys on `:updated_at`",
        "Never adopted. :updated_at is mass re-stamped nightly (~540k rows/day vs "
        "~53k created per week, ADR 010); the only caller passes created_window. "
        "The daily run re-pulls a trailing 7-day created_date window instead.",
    ),
    # Registered as the ASSERTION, not the word. A document explaining that no
    # HttpSensor exists must stay legal — the first version of this entry banned
    # the bare token and immediately flagged the correction that removed the
    # claim. A tripwire that blocks the fix is worse than no tripwire.
    (
        "HttpSensor is a cost gate",
        "No HttpSensor exists. check_source is a BashOperator running curl with "
        "-o /dev/null: status only, no body inspection, no poke interval, no waiting.",
    ),
    (
        "`HttpSensor` at the front validates",
        "Same claim in docs/ARCHITECTURE.md. The gate cannot see an empty body, "
        "which is exactly the published August 2026 stall.",
    ),
    (
        "core portfolio objective",
        "Databricks was removed on 2026-08-20. Any ADR rationale resting on "
        "showcasing it needs a superseding note, not a silent survival.",
    ),
    # ── The three README findings withdrawn in the Phase 7 rewrite ──────────
    # Each was measured, published, and then failed to reproduce. They are
    # registered as the ASSERTION rather than the topic, so a document that
    # EXPLAINS the withdrawal stays legal — the failure mode the HttpSensor
    # entry above records.
    (
        "the problem had resolved itself before anyone arrived",
        "Withdrawn. The claim was that No Condition Found recurs LEAST, below "
        "Work Performed, arguing 'nothing there' closures were mostly correct "
        "rather than premature. It reversed when the load grew from 7 days to "
        "12: Work Performed 5.7%, No Condition Found 6.7%. Only the Access "
        "Failed ranking reproduces (first in all 8 window x chronic specs).",
    ),
    (
        "Composition flips; volume doesn't",
        "Withdrawn. Reported a weekend RISE (8,918 -> 9,403/day) produced by "
        "dividing weekday and weekend totals by the same day count over a window "
        "holding 8 weekdays and 4 weekend days. Volume FALLS at weekends: "
        "10,955 -> 9,903/day. The noise composition finding was never affected.",
    ),
    (
        "| Marked resolved |",
        "Withdrawn as a published measure. Registered as the TABLE COLUMN, not "
        "the phrase: the prose explaining the removal must stay legal. The column "
        "reported how much of the observation window had elapsed at snapshot "
        "time, not city performance. Rates over unfinished cohorts are "
        "right-censored; fct_daily_volume now returns NULL unless "
        "is_denominator_closed.",
    ),
    (
        "this closes the exposure",
        "Written in docs/SLO.md about the five-probe max source count (#50). "
        "False: max-of-N helps only when SOME replica holds the day. When the "
        "source has not published the day at all, every probe correctly returns "
        "0. The zero denominator is closed by SLO-2's population and by zero "
        "no longer being a pass — see ADR 015.",
    ),
    (
        "published for yesterday",
        "SLO-2 no longer measures T-1, or any fixed offset from the clock. The "
        "publish lag is not a constant (23.3h, 23.5h, then 49.0h measured in "
        "one week), so every offset is a ~2-hour stub on some days. The "
        "population is every day int_load_completeness marks complete — ADR 015.",
    ),
    (
        "a zero source count passes",
        "Deleted on 2026-08-27. Within SLO-2's population a zero source count "
        "is a CONTRADICTION — the load says the source published that day "
        "through to midnight — and fails. It was the branch that let the gate "
        "certify a comparison against nothing. See ADR 015.",
    ),
    # Registered as the specific retired NUMBER, not as the word "noise". The
    # 17%-spread-is-noise framing this corrects lives in int_load_completeness's
    # comments and in ADR 015 — neither of which this check scans (models are
    # not docs; ADRs are history). What IS scanned is docs/SLO.md, which
    # reproduces scripts/slo/slo2_completeness.sql verbatim, so reverting that
    # comment reintroduces the false budget into a guarded doc. That is the
    # reachable regression, and this is the phrase that carries it.
    (
        "0.9976 to 0.9998",
        "The 0.98 floor's justification named only quarantine and dedup (up to "
        "0.24%) and read the headroom as ~1.76 points. Settling skew adds up to "
        "0.96% — four times larger — because the load takes whichever Socrata "
        "replica answered while the capture takes the maximum over probes. Real "
        "budget 1.20%, real margin 0.80 points; worst day actually measured is "
        "11,513 / 11,627 = 0.9902 on 2026-08-27. See ADR 016.",
    ),
]
# Deliberately NOT registered here: the phantom DAG task names
# (check_api_availability, ingest_raw, dbt_publish, notify_success) that
# docs/ARCHITECTURE.md carried until 2026-08-26. They are bare tokens, not
# assertions, so banning them would also ban a sentence explaining that they
# never existed — the failure the HttpSensor entry above records. check_dag_tasks
# compares the documented names against the DAG directly, which is the stronger
# guarantee anyway: it catches a WRONG name, not just a known-bad one.


def check_superseded_claims(docs: list) -> list:
    """Fail if a claim a later decision invalidated has reappeared in the docs."""
    errors = []
    for rel in docs:
        # ADRs are immutable records of a decision AT A TIME. A superseded claim
        # inside one is history, not drift, and is corrected by a superseding
        # ADR rather than by editing the original.
        if is_adr(rel):
            continue
        text = read(rel)
        for phrase, why in SUPERSEDED_CLAIMS:
            if phrase in text:
                errors.append(f"{rel} repeats a superseded claim — \"{phrase}\"\n      {why}")
    return errors


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> int:
    manifest = load_manifest()
    if manifest is None:
        print("claim check CANNOT RUN: dbt/target/manifest.json does not exist.")
        print("  Three claims (dbt_test_count, dbt_generic_tests, dbt_singular_tests)")
        print("  and the ARCHITECTURE model inventory are derived from it.")
        print("  Build it with:")
        print(f"      {MANIFEST_HINT}")
        print("  (CI's fast-gate and run_tests.sh both do this before calling this script.)")
        return 2

    docs = doc_files()
    readme_text = read("README.md")
    arch_text = read(os.path.relpath(ARCHITECTURE, ROOT))
    expected = expected_values(manifest)

    errors = (
        check_markers(docs, expected)
        + check_links(docs)
        + check_orphan_anchors(docs)
        + check_citations(docs)
        + check_path_spans(docs)
        + check_slo_doc_sync()
        + check_adr_table(readme_text)
        + check_dag_tasks(readme_text, arch_text)
        + check_terraform_counts(readme_text)
        + check_model_inventory(arch_text, manifest)
        + check_star_counts(arch_text, expected)
        + check_superseded_claims(docs)
    )

    if errors:
        print("Documentation claim check FAILED:")
        for e in errors:
            print(f"  ✗ {e}")
        return 1

    for name, value in sorted(expected.items()):
        print(f"  ✓ claim:{name} = {value}")
    print(f"  ✓ {len(docs)} guarded documents: links resolve, fragments exist, "
          f"no orphan anchors")
    print("  ✓ every citation anchor matches exactly once in the file it names")
    print("  ✓ every documented path and ::symbol exists")
    print("  ✓ ARCHITECTURE model inventory == dbt manifest "
          f"({len(manifest_model_names(manifest))} models)")
    print("  ✓ documented DAG task names == the DAG == EXPECTED_TASKS")
    print(f"  ✓ no superseded claims resurfaced ({len(SUPERSEDED_CLAIMS)} registered)")
    print("Documentation claim check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
