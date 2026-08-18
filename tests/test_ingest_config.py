"""
Unit tests for ingest_config.build_page_params — the ingestion contract.

Pure Python (no Spark, no Databricks), so these run in the structural tier.

The load-bearing assertion is the watermark column: the incremental predicate
must key on Socrata's `:updated_at` system field, never on created_date. A
created_date predicate fetches each request exactly once — on its creation
day — and never re-fetches it when its status later changes, which turns the
Silver MERGE's whenMatchedUpdate and the fct incremental update path into
dead code. That was the original bug; the regression assertion below fails
against any reintroduction of it.
"""

import os
import sys

_NOTEBOOKS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "databricks", "notebooks",
)
if _NOTEBOOKS_DIR not in sys.path:
    sys.path.insert(0, _NOTEBOOKS_DIR)

from ingest_config import PAGE_SIZE, build_page_params


def test_incremental_watermark_is_updated_at_not_created_date():
    """Regression guard for the dead-update-path bug: the incremental filter
    must select rows created OR updated since run_date."""
    params = build_page_params("incremental", "2024-06-01", page=0)
    where = params["$where"]
    assert ":updated_at" in where, (
        f"Incremental $where is {where!r} — it must watermark on the "
        f":updated_at system field so post-creation updates are re-fetched."
    )
    assert "created_date" not in where, (
        f"Incremental $where {where!r} filters on created_date — each request "
        f"would be fetched once and its later status changes never seen."
    )
    assert where == ":updated_at >= '2024-06-01T00:00:00'"


def test_full_load_has_no_where_filter():
    """A full load fetches the entire dataset — no watermark predicate."""
    params = build_page_params("full", "2024-06-01", page=0)
    assert "$where" not in params


def test_offset_steps_by_page_size():
    """$offset must advance by exactly PAGE_SIZE per page — an off-by-one in
    either direction silently skips or duplicates rows at page boundaries
    (duplicates are absorbed by Silver dedup; skips are unrecoverable)."""
    for page in (0, 1, 7):
        params = build_page_params("incremental", "2024-06-01", page)
        assert params["$offset"] == page * PAGE_SIZE
        assert params["$limit"] == PAGE_SIZE


def test_ordering_is_stable_across_pages():
    """Pagination is only coherent under a stable sort; Socrata's :id is the
    documented stable ordering key."""
    params = build_page_params("incremental", "2024-06-01", page=3)
    assert params["$order"] == ":id"

