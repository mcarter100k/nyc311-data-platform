"""
Unit tests for local_runner.fetch_live_records — the daily-run fetch path.

The API is mocked; nothing here touches the network. The contract under test:

  - the query window and pagination come from the ONE existing param builder
    (local/ingest_config.build_page_params) — a second param
    builder growing here is the failure these tests exist to block;
  - the hard row cap is enforced, and hitting it FAILS the run (a normal week
    is nowhere near it, so cap-hit means upstream anomaly, not success);
  - transient faults — connection errors AND the HTTP statuses `requests`
    hands back as ordinary responses (429, 5xx) — are retried with backoff,
    then fail loudly; a non-retryable status fails on the first response;
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
from local_runner import (HTTP_ATTEMPTS, HTTP_RETRYABLE_STATUS, LIVE_DAYS,
                          LIVE_ROW_CAP, SOURCE_COUNT_PROBES, fetch_live_records)


@pytest.fixture(autouse=True)
def no_backoff_sleep(monkeypatch):
    """Neutralise the retry backoff and the inter-probe pause.

    Without this the suite would spend real seconds asleep proving retry
    behaviour, which is the fastest way to get retry tests deleted.
    """
    monkeypatch.setattr("local_runner.time.sleep", lambda _seconds: None)


class FakeResponse:
    """`status` defaults to 200. It exists because the defect these tests were
    extended for is precisely that a non-200 response is a normal return value
    from `requests`, not an exception — a fake without a status_code could not
    express the failure at all."""

    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class FakeGet:
    """Records every call; serves configured pages then empty pages.

    A page may be a FakeResponse (to give it a status) or a bare payload.
    """

    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def __call__(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        i = len(self.calls) - 1
        page = self.pages[i] if i < len(self.pages) else []
        return page if isinstance(page, FakeResponse) else FakeResponse(page)


def test_window_and_params_come_from_the_shared_builder():
    get = FakeGet([[{"unique_key": "1"}]])
    fetch_live_records(get=get)

    expected_date = (datetime.now(timezone.utc) - timedelta(days=LIVE_DAYS)).date().isoformat()
    assert get.calls[0]["params"] == build_page_params("created_window", expected_date, 0), (
        "Live fetch must build its query through ingest_config.build_page_params "
        f"for the trailing-{LIVE_DAYS}-day window — not through a private param dict."
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


def test_network_failure_fails_loudly_after_the_bounded_retries():
    attempts = []

    def dying_get(url, params=None, headers=None, timeout=None):
        attempts.append(1)
        raise ConnectionError("socket closed")

    with pytest.raises(RuntimeError, match="failed after"):
        fetch_live_records(get=dying_get)
    assert len(attempts) == HTTP_ATTEMPTS, (
        "bounded retries — no retry storms, no partial success"
    )


# Bound to a module-level name so the claim-checker's AST counter can size
# this parametrisation. It reads only THIS file, so a constant imported from
# local_runner is invisible to it — and it used to score such a case as 1,
# silently undercounting the tier by 4.
RETRYABLE_STATUSES = [429, 500, 502, 503, 504]


def test_retryable_status_list_matches_the_pipeline():
    """The local copy above must not drift from local_runner's definition.

    Binding a local constant is what lets the claim-checker size the
    parametrisation, but it puts the same list in two places — the exact defect
    this repo built check_model_drift.py and the claim markers to prevent. This
    asserts the copy is faithful, so the convenience cannot become a lie.
    """
    assert sorted(RETRYABLE_STATUSES) == sorted(HTTP_RETRYABLE_STATUS), (
        f"tests/local/test_live_fetch.py lists {sorted(RETRYABLE_STATUSES)} but "
        f"local_runner.HTTP_RETRYABLE_STATUS is {sorted(HTTP_RETRYABLE_STATUS)} — "
        f"the parametrised retry tests are no longer covering the real set."
    )


@pytest.mark.parametrize("status", RETRYABLE_STATUSES)
def test_transient_http_status_is_retried_then_succeeds(status):
    """THE DEFECT THIS FILE MISSED FOR MONTHS.

    `requests` raises on a dropped socket but returns 429 and 5xx as ordinary
    Response objects. `raise_for_status()` sat OUTSIDE the retry loop, so the
    single most likely fault against a public rate-limited API — a 429 — got
    zero retries while a socket error got one. One bad response aborted the
    whole daily run.

    Parametrized over every status in the retryable set so adding a code
    without wiring it up cannot pass silently.
    """
    get = FakeGet([FakeResponse([], status=status), [{"unique_key": "1"}]])
    records = fetch_live_records(get=get)

    assert records == [{"unique_key": "1"}], records
    assert len(get.calls) >= 2, (
        f"HTTP {status} was not retried — it is a transient fault returned as a "
        f"normal response, which is exactly what the old code could not see."
    )


def test_non_retryable_http_status_fails_on_the_first_response():
    """A 404 or a malformed-query 400 is not transient. Repeating it wastes the
    backoff and buries the real status, so it must raise immediately — the
    retry policy is narrow on purpose."""
    calls = []

    def not_found(url, params=None, headers=None, timeout=None):
        calls.append(1)
        return FakeResponse([], status=404)

    with pytest.raises(RuntimeError, match="404"):
        fetch_live_records(get=not_found)
    assert len(calls) == 1, f"a 404 must not be retried, got {len(calls)} attempts"


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


# ── fetch_source_counts_window — the SLO-2 reconciliation capture ────────────

from local_runner import fetch_source_counts_window  # noqa: E402


def _day(offset):
    return (datetime.now(timezone.utc) - timedelta(days=offset)).date().isoformat()


def _grouped(**by_day):
    """One grouped Socrata response: {'2026-08-20': 10500} -> the JSON shape."""
    return [{"day": f"{d.replace('_', '-')}T00:00:00.000", "n": str(n)}
            for d, n in by_day.items()]


def test_source_counts_cover_the_whole_fetch_window_not_one_day():
    """The capture asks about EVERY day the fetch window covers.

    It used to ask about UTC-yesterday alone, which the publish lag guarantees
    is a stub or empty — so the number SLO-2 reconciled against described a
    ~2-hour sliver. And because the lag is not a constant (23.3h, 23.5h, then
    49.0h measured within one week), no fixed offset replaces it. The gate
    picks its day from int_load_completeness at evaluation time, so the capture
    has to supply the whole window.
    """
    get = FakeGet([[]] * SOURCE_COUNT_PROBES)
    result = fetch_source_counts_window(days=7, get=get)

    assert [r["target_date"] for r in result] == [_day(d) for d in range(7, -1, -1)], (
        "One record per day from the window start through today, inclusive."
    )
    params = get.calls[0]["params"]
    assert params["$where"] == f"created_date >= '{_day(7)}T00:00:00'"
    assert params["$group"] == "date_trunc_ymd(created_date)", (
        "One grouped request must cover the window — widening the population "
        "must not multiply the number of round trips."
    )
    assert len(get.calls) == SOURCE_COUNT_PROBES


def test_a_day_the_source_has_no_rows_for_is_recorded_as_an_explicit_zero():
    """'The source says none' and 'we never asked' are different facts.

    Days absent from the grouped response are written as 0 so
    slo2_completeness.sql can tell them apart: a missing count fails closed
    because the gate is blind, while a captured zero on a day the load says is
    COMPLETE is a contradiction and also fails. Neither is the old
    `WHEN source = 0 THEN true` pass.
    """
    payload = _grouped(**{_day(3).replace("-", "_"): 10500})
    get = FakeGet([payload] * SOURCE_COUNT_PROBES)
    result = fetch_source_counts_window(days=4, get=get)

    assert {r["target_date"]: r["source_count"] for r in result} == {
        _day(4): 0, _day(3): 10500, _day(2): 0, _day(1): 0, _day(0): 0,
    }


def test_source_counts_take_the_per_day_maximum_across_disagreeing_replicas():
    """Socrata answers identical queries from replicas at different indexing states.

    Measured 2026-08-26: six identical count calls for the same day returned
    0, 0, 358, 358, 358, 0. A row visible on ANY replica exists, so the highest
    count is the most complete view available, and a day missing from a
    response counts as that probe's zero — otherwise a day one replica has not
    indexed would report its single sighting as unanimous.

    What this does NOT buy, since the previous version of this code overclaimed
    it: when the source has not published a day at all, every probe correctly
    returns 0 and sampling cannot conjure rows. The defence against a zero
    denominator is the gate's population and its refusal to pass on zero, not
    this.
    """
    key = _day(1).replace("-", "_")
    get = FakeGet([
        [], [], _grouped(**{key: 358}), [], _grouped(**{key: 12}),
    ])
    result = fetch_source_counts_window(days=2, get=get)

    assert len(get.calls) == SOURCE_COUNT_PROBES, (
        f"Expected {SOURCE_COUNT_PROBES} probes, got {len(get.calls)} — one sample "
        f"is a coin flip against a non-read-consistent source."
    )
    assert {r["target_date"]: r["source_count"] for r in result}[_day(1)] == 358, (
        "Expected the per-day maximum (358). Taking the last, the modal, or the "
        "mean value would have captured 0 or 12 here."
    )


def test_source_count_failure_fails_loudly_after_the_bounded_retries():
    calls = []

    def failing_get(url, params=None, headers=None, timeout=None):
        calls.append(1)
        raise ConnectionError("boom")

    with pytest.raises(RuntimeError, match="failed after"):
        fetch_source_counts_window(get=failing_get)
    assert len(calls) == HTTP_ATTEMPTS, (
        "bounded retries — the run must be red, never gate-blind"
    )


def test_source_count_malformed_payload_is_a_failure():
    get = FakeGet([[{"n": "10"}]])          # grouped response with no `day`
    with pytest.raises(RuntimeError, match="missing columns"):
        fetch_source_counts_window(get=get)
