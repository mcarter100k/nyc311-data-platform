"""Guards on scripts/backfill_is_overdue.py.

The backfill duplicates the is_overdue CASE from
models/marts/fct_service_requests.sql, because it has to recompute the column
without rebuilding the model. Duplicated business logic drifts, and drift here
is silent: the backfill would happily write values the model would never
produce, and the daily run would go green on wrong data.

So the duplication is pinned. If either copy is edited without the other, the
first test fails and names both files.
"""

import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKFILL = os.path.join(ROOT, "scripts", "backfill_is_overdue.py")
LOCAL_MODEL = os.path.join(ROOT, "local", "models", "marts", "fct_service_requests.sql")
DBT_MODEL = os.path.join(ROOT, "dbt", "models", "marts", "fct_service_requests.sql")


def _branches(sql: str) -> list[str]:
    """The decision branches of an is_overdue CASE, normalised.

    Strips comments, the `r.` table alias the models use and the backfill does
    not, and all runs of whitespace — so the comparison is about the RULE, not
    about formatting or aliasing.
    """
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = sql.replace("r.", "")
    out = []
    # `$` in the lookahead matters: _model_case strips the closing `end`, so the
    # final `else` branch has no keyword after it and would be dropped — making
    # two rules that differ only in their ELSE compare as equal.
    for m in re.finditer(r"\b(when|else)\b(.*?)(?=\bwhen\b|\belse\b|\bend\b|$)", sql, re.DOTALL | re.IGNORECASE):
        out.append(re.sub(r"\s+", " ", (m.group(1) + m.group(2))).strip().lower())
    return out


def _model_case(path: str) -> str:
    """The CASE expression aliased `as is_overdue` in a model file.

    The body must not be allowed to span an `end`: these models declare
    is_resolved and is_actioned as CASEs immediately above is_overdue, and a
    greedy match swallows all three, silently comparing the wrong rule.
    """
    src = open(path).read()
    m = re.search(r"case((?:(?!\bend\b).)*?)end\s+as is_overdue", src, re.DOTALL | re.IGNORECASE)
    assert m, f"no `case ... end as is_overdue` found in {path}"
    return m.group(1)


def test_backfill_rule_matches_local_model():
    """The backfill must encode exactly the rule the DuckDB model computes."""
    src = open(BACKFILL).read()
    m = re.search(r"IS_OVERDUE_RULE\s*=\s*\"\"\"(.*?)\"\"\"", src, re.DOTALL)
    assert m, "IS_OVERDUE_RULE not found in the backfill script."
    backfill = _branches(m.group(1))
    model = _branches(_model_case(LOCAL_MODEL))

    assert backfill, "backfill rule parsed to zero branches — the parser is broken, not the rule."
    assert backfill == model, (
        "scripts/backfill_is_overdue.py and local/models/marts/fct_service_requests.sql "
        "no longer encode the same is_overdue rule.\n"
        f"  backfill: {backfill}\n"
        f"  model:    {model}\n"
        "Edit both, or the backfill will write values the model would never produce."
    )


def test_both_model_trees_agree_on_the_rule():
    """local/ runs daily; dbt/ is the Snowflake mirror. A fix applied to one and
    not the other is how this defect class starts — the status guard existing in
    one tree and not the other would leave the deployed pipeline wrong while the
    reviewed one looked right."""
    assert _branches(_model_case(LOCAL_MODEL)) == _branches(_model_case(DBT_MODEL)), (
        "local/ and dbt/ disagree on the is_overdue rule."
    )


def test_status_guard_is_the_first_branch():
    """Order is load-bearing. `resolution_days is null` first would return NULL
    for open rows that HAVE a resolution_days — the exact rows the source emits
    with a closed_date while still Open — and they would fall through to FALSE.
    That was the original defect, and it is invisible in aggregate."""
    branches = _branches(_model_case(LOCAL_MODEL))
    assert branches[0].startswith("when status <> 'closed'"), (
        f"the status guard must be the FIRST branch; branches are {branches}"
    )


@pytest.mark.parametrize(
    "status,resolution_days,expected",
    [
        ("Open", 3.0, None),           # the regression: closed_date present, still open
        ("In Progress", 45.0, None),
        ("Assigned", 0.5, None),
        ("Closed", None, None),        # closed with no resolution_days: unknowable
        ("Closed", 31.0, True),
        ("Closed", 30.0, False),
        ("Closed", 0.0, False),
    ],
)
def test_rule_semantics(status, resolution_days, expected):
    """The rule's truth table, asserted directly against DuckDB so the SQL — not
    a Python paraphrase of it — is what gets checked."""
    duckdb = pytest.importorskip("duckdb")
    import importlib.util

    spec = importlib.util.spec_from_file_location("backfill", BACKFILL)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    con = duckdb.connect(":memory:")
    con.execute("create table t (status varchar, resolution_days double)")
    con.execute("insert into t values (?, ?)", [status, resolution_days])
    got = con.sql(f"select ({mod.IS_OVERDUE_RULE}) from t").fetchone()[0]
    assert got is expected, (
        f"status={status!r} resolution_days={resolution_days!r}: "
        f"rule returned {got!r}, expected {expected!r}"
    )
