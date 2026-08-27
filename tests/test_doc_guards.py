"""
Tests for scripts/check_claims.py — the documentation guard itself.

Why this file exists. Every check in check_claims.py is an assertion about the
repository, and an assertion that cannot fail is worse than no assertion: it
reports green and is read as evidence. This repo has shipped three such checks by
accident — an ADR-table search that passed because ADRs are also linked from
prose, an `N-task` substring match that a second DAG name kept satisfying, and a
line-number citation format that `check_links` silently discarded before
comparing. Each was found by hand, months later.

So each guard is exercised here against a synthetic tree with the thing it
guards deliberately broken. A refactor that makes a check unfireable fails these
tests instead of quietly passing CI.

The functions under test read files under `check_claims.ROOT`; the tests point
ROOT at a tmp_path so nothing here touches the real repo.
"""

import importlib.util
import json
import os
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_spec = importlib.util.spec_from_file_location(
    "check_claims", os.path.join(REPO_ROOT, "scripts", "check_claims.py")
)
cc = importlib.util.module_from_spec(_spec)
sys.modules["check_claims"] = cc
_spec.loader.exec_module(cc)


@pytest.fixture
def tree(tmp_path, monkeypatch):
    """A minimal fake repo rooted at tmp_path, with ROOT pointed at it."""
    monkeypatch.setattr(cc, "ROOT", str(tmp_path))

    def write(rel, text):
        path = tmp_path / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        return rel

    return write


# ── Citations: exactly-once is the whole point ────────────────────────────────

@pytest.mark.parametrize("body, occurrences", [
    ("alpha\nunique_marker_line\nomega\n", 1),
    ("alpha\nomega\n", 0),
    ("unique_marker_line\nomega\nunique_marker_line\n", 2),
])
def test_citation_requires_exactly_one_match(tree, body, occurrences):
    tree("src/thing.sql", body)
    tree("docs/CLAIMS.md", 'x `src/thing.sql#"unique_marker_line"` y\n')

    errors = cc.check_citations(["docs/CLAIMS.md"])

    if occurrences == 1:
        assert errors == []
    else:
        assert len(errors) == 1
        assert f"matches {occurrences} times" in errors[0]


def test_citation_to_a_missing_file_is_an_error(tree):
    tree("docs/CLAIMS.md", '`src/gone.sql#"anything"`\n')
    assert "missing file" in cc.check_citations(["docs/CLAIMS.md"])[0]


def test_citation_checker_reports_itself_vacuous_when_nothing_uses_the_form(tree):
    """The register could be rewritten without citations and every citation
    check would pass by having nothing to check. That is the failure mode this
    file exists for, so it is an error rather than a silent zero."""
    tree("docs/CLAIMS.md", "no citations at all\n")
    assert "vacuous" in cc.check_citations(["docs/CLAIMS.md"])[0]


def test_citations_are_not_checked_inside_adrs(tree):
    """An ADR records a decision AT A TIME; the code it cites may be gone.
    docs/adr/010 still cites databricks/, deleted 2026-08-20, and truthfully so."""
    tree("src/live.sql", "still_here\n")
    tree("docs/adr/001-x.md", '`src/deleted.sql#"long gone"`\n')
    tree("docs/CLAIMS.md", '`src/live.sql#"still_here"`\n')

    assert cc.check_citations(["docs/adr/001-x.md", "docs/CLAIMS.md"]) == []
    # …and the same citation outside an ADR does fail, so the carve-out is a
    # carve-out and not a hole that swallows everything.
    tree("docs/OTHER.md", '`src/deleted.sql#"long gone"`\n')
    assert cc.check_citations(["docs/OTHER.md"])


# ── Path spans: the "verifying test" column ───────────────────────────────────

def test_path_span_flags_a_test_function_that_does_not_exist(tree):
    tree("tests/test_x.py", "def test_present():\n    pass\n")
    tree("docs/CLAIMS.md", "`tests/test_x.py::test_absent`\n")

    errors = cc.check_path_spans(["docs/CLAIMS.md"])
    assert len(errors) == 1
    assert "defines no test_absent" in errors[0]


def test_path_span_accepts_a_test_function_that_does_exist(tree):
    tree("tests/test_x.py", "def test_present():\n    pass\n")
    tree("docs/CLAIMS.md", "`tests/test_x.py::test_present`\n")
    assert cc.check_path_spans(["docs/CLAIMS.md"]) == []


def test_path_span_flags_a_missing_file(tree):
    tree("docs/CLAIMS.md", "`scripts/gone.py`\n")
    assert "does not exist" in cc.check_path_spans(["docs/CLAIMS.md"])[0]


@pytest.mark.parametrize("span", ["_loaded_at", "fct_service_requests", "22M rows"])
def test_path_span_ignores_prose_in_backticks(tree, span):
    """A code span is not a path unless it has a directory and an extension —
    otherwise every identifier in the docs becomes a false failure."""
    tree("docs/CLAIMS.md", f"`{span}`\n")
    assert cc.check_path_spans(["docs/CLAIMS.md"]) == []


# ── Links and fragments ───────────────────────────────────────────────────────

def test_link_is_resolved_relative_to_the_linking_file_not_the_repo_root(tree):
    """docs/ARCHITECTURE.md's `adr/008-...md` is valid; rooting it at the repo
    root — which the pre-2026-08-26 checker did — would call it broken."""
    tree("docs/adr/008-x.md", "# X\n")
    tree("docs/ARCHITECTURE.md", "[ADR 008](adr/008-x.md)\n")
    assert cc.check_links(["docs/ARCHITECTURE.md"]) == []


def test_broken_relative_link_is_flagged(tree):
    tree("docs/ARCHITECTURE.md", "[ADR 008](adr/008-missing.md)\n")
    assert "broken relative link" in cc.check_links(["docs/ARCHITECTURE.md"])[0]


def test_fragment_must_exist_in_the_target_document(tree):
    tree("docs/SLO.md", "# Service Level Objectives\n\n## Upstream stall warning\n")
    tree("docs/A.md", "[gone](SLO.md#no-such-heading)\n")
    assert "fragment that does not exist" in cc.check_links(["docs/A.md"])[0]


def test_fragment_that_matches_a_heading_slug_passes(tree):
    tree("docs/SLO.md", "## Upstream stall warning (not an SLO)\n")
    tree("docs/A.md", "[ok](SLO.md#upstream-stall-warning-not-an-slo)\n")
    assert cc.check_links(["docs/A.md"]) == []


def test_line_number_fragments_are_rejected_on_sight(tree):
    """The citation format this replaced. A link whose target exists but whose
    line range has drifted is exactly the defect nothing could see."""
    tree("terraform/main.tf", "resource\n")
    tree("docs/CLAIMS.md", "[main.tf](../terraform/main.tf#L471-L481)\n")
    errors = cc.check_links(["docs/CLAIMS.md"])
    assert len(errors) == 1
    assert "cites LINE NUMBERS" in errors[0]


def test_orphan_anchor_is_flagged_and_a_linked_one_is_not(tree):
    tree("docs/A.md", '<a name="orphan"></a>\n<a name="used"></a>\n[here](#used)\n')
    errors = cc.check_orphan_anchors(["docs/A.md"])
    assert len(errors) == 1
    assert "orphan" in errors[0]


# ── Markers ───────────────────────────────────────────────────────────────────

def test_marker_mismatch_is_flagged_in_any_guarded_document(tree):
    tree("docs/ARCHITECTURE.md", "<!--claim:fct_models-->3<!--/claim-->\n")
    errors = cc.check_markers(["docs/ARCHITECTURE.md"], {"fct_models": 4})
    assert len(errors) == 1
    assert "doc says '3', repo says 4" in errors[0]


def test_the_same_marker_may_be_repeated_when_every_copy_agrees(tree):
    """Repetition is safe precisely because it is checked. The old
    'a number is stated once' rule is what kept ARCHITECTURE.md's diagram
    unguarded while the README's copy was correct."""
    tree("README.md", "<!--claim:fct_models-->4<!--/claim-->\n")
    tree("docs/ARCHITECTURE.md", "<!--claim:fct_models-->4<!--/claim-->\n")
    assert cc.check_markers(["README.md", "docs/ARCHITECTURE.md"], {"fct_models": 4}) == []


def test_a_marker_with_no_computed_source_is_an_error(tree):
    """It renders as guarded and is not."""
    tree("README.md", "<!--claim:invented-->9<!--/claim-->\n")
    errors = cc.check_markers(["README.md"], {})
    assert any("no computed source" in e for e in errors)


def test_a_computed_claim_stated_nowhere_is_an_error(tree):
    tree("README.md", "nothing here\n")
    errors = cc.check_markers(["README.md"], {"fct_models": 4})
    assert any("stated nowhere" in e for e in errors)


def test_tier_counts_must_sum_to_the_headline_total(tree):
    tree("README.md", "".join(
        f"<!--claim:{k}-->{v}<!--/claim-->\n" for k, v in (
            ("test_count", 146), ("structural_test_count", 97),
            ("unit_test_count", 8), ("behavioral_test_count", 40))))
    expected = {"test_count": 146, "structural_test_count": 97,
                "unit_test_count": 8, "behavioral_test_count": 40}
    assert any("tier counts sum to" in e for e in cc.check_markers(["README.md"], expected))


# ── DAG task names ────────────────────────────────────────────────────────────

DAG_SRC = (
    'a = Op(task_id="check_source")\n'
    'b = Op(task_id="fetch_live")\n'
)
TESTS_SRC = 'EXPECTED_TASKS = [\n    "check_source",\n    "fetch_live",\n]\n'


@pytest.fixture
def dag_tree(tree, monkeypatch, tmp_path):
    tree("airflow/dags/nyc311_local.py", DAG_SRC)
    tree("tests/test_pipeline_components.py", TESTS_SRC)
    monkeypatch.setattr(cc, "DAG_PATH", str(tmp_path / "airflow/dags/nyc311_local.py"))
    return tree


def _arch(chain):
    return (f"<!--dag-tasks:airflow/dags/nyc311_local.py-->\n```\n{chain}\n```\n")


def test_documented_task_names_must_match_the_dag(dag_tree):
    errors = cc.check_dag_tasks("2-task DAG", _arch("check_api_availability → fetch_live"))
    assert any("documents the task chain as" in e for e in errors)


def test_correct_task_names_pass(dag_tree):
    assert cc.check_dag_tasks("2-task DAG", _arch("check_source → fetch_live")) == []


def test_a_right_count_with_wrong_names_still_fails(dag_tree):
    """The check this replaced compared counts only, and passed on a diagram
    naming four tasks that did not exist."""
    errors = cc.check_dag_tasks("2-task DAG", _arch("ingest_raw → dbt_publish"))
    assert errors, "a task chain of the right LENGTH and the wrong NAMES must fail"


def test_a_missing_guarded_block_is_an_error_not_a_skip(dag_tree):
    errors = cc.check_dag_tasks("2-task DAG", "check_source → fetch_live\n")
    assert any("no <!--dag-tasks:" in e for e in errors)


# ── Model inventory ───────────────────────────────────────────────────────────

def _manifest(*names):
    return {"nodes": {n: {"resource_type": "model", "name": n} for n in names}}


def test_model_inventory_flags_an_undocumented_model():
    arch = "<!--model-inventory-->\n- `dim_date` — spine\n<!--/model-inventory-->"
    errors = cc.check_model_inventory(arch, _manifest("dim_date", "fct_data_quality"))
    assert any("omits `fct_data_quality`" in e for e in errors)


def test_model_inventory_flags_a_model_that_no_longer_exists():
    arch = "<!--model-inventory-->\n- `dim_date` — spine\n- `fct_ghost` — gone\n<!--/model-inventory-->"
    errors = cc.check_model_inventory(arch, _manifest("dim_date"))
    assert any("lists `fct_ghost`" in e for e in errors)


def test_model_inventory_is_scoped_to_the_marked_block():
    """Mentioning a model in prose elsewhere must not satisfy the inventory —
    that is how check_adr_table was vacuous before it was scoped."""
    arch = ("`fct_data_quality` is discussed at length here.\n"
            "<!--model-inventory-->\n- `dim_date` — spine\n<!--/model-inventory-->")
    errors = cc.check_model_inventory(arch, _manifest("dim_date", "fct_data_quality"))
    assert any("omits `fct_data_quality`" in e for e in errors)


def test_star_counts_compare_against_the_marts_directory():
    expected = {"fct_models": 4, "dim_models": 3}
    assert cc.check_star_counts("marts: 4 facts · 3 dims", expected) == []
    assert cc.check_star_counts("marts: 3 facts · 3 dims", expected)


# ── Superseded claims, and the ADR carve-out ──────────────────────────────────

def test_a_superseded_claim_outside_adr_fails(tree):
    phrase = cc.SUPERSEDED_CLAIMS[0][0]
    tree("docs/ARCHITECTURE.md", f"prose. {phrase}. more prose.\n")
    assert cc.check_superseded_claims(["docs/ARCHITECTURE.md"])


def test_the_same_claim_inside_an_adr_is_history_not_drift(tree):
    phrase = cc.SUPERSEDED_CLAIMS[0][0]
    tree("docs/adr/013-x.md", f"At the time: {phrase}.\n")
    assert cc.check_superseded_claims(["docs/adr/013-x.md"]) == []


# ── Manifest dependency ───────────────────────────────────────────────────────

def test_missing_manifest_is_reported_as_a_precondition_not_a_drift(tmp_path, monkeypatch, capsys):
    """Exit 2, not 1 and not 0: 'I could not check' must stay distinguishable
    from 'the docs are wrong', and must never be silently green."""
    monkeypatch.setattr(cc, "MANIFEST", str(tmp_path / "nope.json"))
    assert cc.main() == 2
    assert "CANNOT RUN" in capsys.readouterr().out


def test_manifest_test_counts_split_generic_from_singular(tmp_path):
    manifest = {"nodes": {
        "t1": {"resource_type": "test", "test_metadata": {"name": "unique"}},
        "t2": {"resource_type": "test", "test_metadata": {"name": "not_null"}},
        "t3": {"resource_type": "test"},
        "m1": {"resource_type": "model", "name": "dim_date"},
    }}
    (tmp_path / "m.json").write_text(json.dumps(manifest))
    assert cc.manifest_test_counts(manifest) == (3, 2, 1)
    assert cc.manifest_model_names(manifest) == ["dim_date"]


@pytest.mark.parametrize("heading, slug", [
    ("## Upstream stall warning (not an SLO)", "upstream-stall-warning-not-an-slo"),
    ("### `fct_service_requests` — the core fact", "fct_service_requests--the-core-fact"),
    ("# NYC 311 Data Platform", "nyc-311-data-platform"),
])
def test_slugify_matches_github_heading_anchors(heading, slug):
    assert cc.slugify(cc.HEADING_RE.findall(heading)[0]) == slug
