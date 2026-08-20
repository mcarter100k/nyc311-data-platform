"""
Unit tests for local_runner.fetch_live_records — the daily-run fetch path.

The API is mocked; nothing here touches the network. The contract under test:

  - the query window and pagination come from the ONE existing param builder
    (local/ingest_config.build_page_params) — a second param
    builder growing here is the failure these tests exist to block;
  - the hard row cap is enforced, and hitting it FAILS the run (a normal week
    is nowhere near it, so cap-hit means upstream anomaly, not success);
  - network failure fails loudly after exactly one retry — no partial data;
  - zero rows for a 7-day window is a failure, not an empty success;
  - SOCRATA_APP_TOKEN is used when present and no auth is assumed otherwise.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pandas", reason="pandas not installed — skipping live-fetch tests")

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
for p in (os.path.join(ROOT, "local"), os.path.join(ROOT, "local")):
    if p not in sys.path:
        sys.path.insert(0, p)

from ingest_config import PAGE_SIZE, build_page_params
from local_runner import LIVE_DAYS, LIVE_ROW_CAP, fetch_live_records


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class FakeGet:
    """Records every call; serves configured pages then empty pages."""

    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def __call__(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        i = len(self.calls) - 1
        return FakeResponse(self.pages[i] if i < len(self.pages) else [])


def test_window_and_params_come_from_the_shared_builder():
    get = FakeGet([[{"unique_key": "1"}]])
    fetch_live_records(get=get)

    expected_date = (datetime.now(timezone.utc) - timedelta(days=LIVE_DAYS)).date().isoformat()
    assert get.calls[0]["params"] == build_page_params("created_window", expected_date, 0), (
        "Live fetch must build its query through ingest_config.build_page_params "
        "for the trailing-{}-day window — not through a private param dict.".format(LIVE_DAYS)
    )
    # created_date, NOT :updated_at: the source mass re-stamps :updated_at
    # nightly (~540k rows/day vs ~53k/week created — ADR 010), so a capped
    # daily fetch windows on creation and re-pulls the window in full, which
    # still carries status updates for every row inside it.
    assert get.calls[0]["params"]["$where"].startswith(f"created_date >= '{expected_date}")


def test_pagination_advances_offset_until_empty_page():
    page = [{"unique_key": str(i)} for i in range(3)]
    get = FakeGet([page, page])  # two pages, then the built-in empty page
    records = fetch_live_records(get=get)

    assert len(records) == 6
    offsets = [c["params"]["$offset"] for c in get.calls]
    assert offsets == [0, PAGE_SIZE, 2 * PAGE_SIZE]


def test_row_cap_is_a_hard_failure():
    endless_page = [{"unique_key": str(i)} for i in range(PAGE_SIZE)]
    get = FakeGet([endless_page] * 10)

    with pytest.raises(RuntimeError, match="cap"):
        fetch_live_records(get=get)
    # It must stop paging once the cap is breached, not fetch all ten pages.
    assert len(get.calls) <= (LIVE_ROW_CAP // PAGE_SIZE) + 1


def test_network_failure_fails_loudly_after_one_retry():
    attempts = []

    def dying_get(url, params=None, headers=None, timeout=None):
        attempts.append(1)
        raise ConnectionError("socket closed")

    with pytest.raises(RuntimeError, match="after one retry"):
        fetch_live_records(get=dying_get)
    assert len(attempts) == 2, "exactly one retry — no retry storms, no partial success"


def test_zero_rows_is_a_failure_not_an_empty_success():
    get = FakeGet([])  # immediate empty page
    with pytest.raises(RuntimeError, match="[Zz]ero rows"):
        fetch_live_records(get=get)


def test_app_token_used_when_present_absent_otherwise(monkeypatch):
    monkeypatch.delenv("SOCRATA_APP_TOKEN", raising=False)
    get = FakeGet([[{"unique_key": "1"}]])
    fetch_live_records(get=get)
    assert "X-App-Token" not in get.calls[0]["headers"]

    monkeypatch.setenv("SOCRATA_APP_TOKEN", "tok-123")
    get = FakeGet([[{"unique_key": "1"}]])
    fetch_live_records(get=get)
    assert get.calls[0]["headers"]["X-App-Token"] == "tok-123"


# ── fetch_source_count_yesterday — the SLO-2 reconciliation capture ──────────

from local_runner import fetch_source_count_yesterday  # noqa: E402


def test_source_count_queries_yesterday_utc_window():
    get = FakeGet([[{"n": "319"}]])
    result = fetch_source_count_yesterday(get=get)

    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    where = get.calls[0]["params"]["$where"]
    assert result["target_date"] == yesterday
    assert result["source_count"] == 319
    assert f"'{yesterday}T00:00:00.000'" in where and f"'{yesterday}T23:59:59.999'" in where, (
        "Source count must cover exactly yesterday's UTC calendar day — the same "
        "day SLO-2 reconciles against."
    )
    assert get.calls[0]["params"]["$select"] == "count(*) as n"


def test_source_count_failure_fails_loudly_after_one_retry():
    calls = []

    def failing_get(url, params=None, headers=None, timeout=None):
        calls.append(1)
        raise ConnectionError("boom")

    with pytest.raises(RuntimeError, match="after one retry"):
        fetch_source_count_yesterday(get=failing_get)
    assert len(calls) == 2, "exactly one retry — the run must be red, never gate-blind"


def test_source_count_empty_payload_is_a_failure():
    get = FakeGet([[]])
    with pytest.raises(RuntimeError, match="no count"):
        fetch_source_count_yesterday(get=get)
