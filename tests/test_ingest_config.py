"""Unit tests for ingest_config.build_page_params, the --live query contract."""

import os
import sys

_LOCAL_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "local")
if _LOCAL_DIR not in sys.path:
    sys.path.insert(0, _LOCAL_DIR)

from ingest_config import PAGE_SIZE, build_page_params


def test_a_page_covers_exactly_one_creation_day():
    """One day per query, on creation; :updated_at is mass re-stamped nightly (ADR 010)."""
    params = build_page_params("2026-08-31", page=0)
    assert params["$where"] == (
        "created_date >= '2026-08-31T00:00:00' and created_date < '2026-09-01T00:00:00'"
    )


def test_the_open_ended_slice_has_no_upper_bound():
    """The window's last slice must also take rows stamped later than today."""
    params = build_page_params("2026-08-11", page=0, open_ended=True)
    assert params["$where"] == "created_date >= '2026-08-11T00:00:00'"


def test_offset_steps_by_page_size():
    """An off-by-one here skips or repeats rows at every page boundary."""
    for page in (0, 1, 7):
        params = build_page_params("2026-08-11", page)
        assert params["$offset"] == page * PAGE_SIZE
        assert params["$limit"] == PAGE_SIZE


def test_ordering_is_stable_across_pages():
    """Offset paging is only coherent under a stable sort; :id is Socrata's."""
    assert build_page_params("2026-08-11", page=3)["$order"] == ":id"
