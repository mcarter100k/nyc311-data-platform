"""
Unit tests for the daily-run fetch path: fetch_live_records and
fetch_source_counts_window. The API is mocked; nothing touches the network.

  - query parameters come from ingest_config.build_page_params;
  - hitting the row cap FAILS the run;
  - transient faults (connection errors, 429, 5xx) are retried, then fail
    loudly; any other status fails on the first response;
  - zero rows is a failure, not an empty success;
  - SOCRATA_APP_TOKEN is sent when set.
"""

import os
import sys
from datetime import datetime, timedelta, UTC

import pytest

pytest.importorskip("pandas", reason="pandas not installed — skipping live-fetch tests")

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if os.path.join(ROOT, "local") not in sys.path:
    sys.path.insert(0, os.path.join(ROOT, "local"))

from ingest_config import PAGE_SIZE, build_page_params
from local_runner import (HTTP_ATTEMPTS, HTTP_BACKOFF_SECONDS, HTTP_RETRYABLE_STATUS, LIVE_DAYS,
                          LIVE_ROW_CAP, SOURCE_COUNT_PROBES, fetch_live_records)


@pytest.fixture(autouse=True)
def no_backoff_sleep(monkeypatch):
    """Skip the retry backoff and the inter-probe pause."""
    monkeypatch.setattr("local_runner.time.sleep", lambda _seconds: None)


class FakeResponse:
    """A response with a status, since `requests` returns 429/5xx rather than raising."""

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

    expected_date = (datetime.now(UTC) - timedelta(days=LIVE_DAYS)).date().isoformat()
    assert get.calls[0]["params"] == build_page_params(expected_date, 0), (
        "Live fetch must build its query through ingest_config.build_page_params "
        f"for the trailing-{LIVE_DAYS}-day window — not through a private param dict."
    )


def test_day_slices_tile_the_window_with_no_gap_or_overlap():
    """Each day is one query; together they must cover [start, open end) exactly."""
    days = 5
    get = FakeGet([])  # every query is empty; the fetch then fails on zero rows
    with pytest.raises(RuntimeError, match="[Zz]ero rows"):
        fetch_live_records(days=days, get=get)

    today = datetime.now(UTC).date()
    expected = [build_page_params((today - timedelta(days=days - i)).isoformat(), 0,
                                  open_ended=(i == days))
                for i in range(days + 1)]
    assert [c["params"] for c in get.calls] == expected
    bounded = [c["params"]["$where"] for c in get.calls[:-1]]
    assert all(" and created_date < " in w for w in bounded), bounded
    assert " and " not in get.calls[-1]["params"]["$where"], "the last slice must be open-ended"


def test_pagination_advances_offset_until_empty_page():
    full = [{"unique_key": str(i)} for i in range(PAGE_SIZE)]
    get = FakeGet([full, full])  # two full pages, then the built-in empty page
    records = fetch_live_records(days=0, get=get)  # one slice

    assert len(records) == 2 * PAGE_SIZE
    offsets = [c["params"]["$offset"] for c in get.calls]
    assert offsets == [0, PAGE_SIZE, 2 * PAGE_SIZE]


def test_a_short_page_ends_the_day_without_another_request():
    """A day with fewer rows than a page needs one request, not two. Every extra
    request is another chance to hit a 503 burst (run 36689681838 failed on one)."""
    short = [{"unique_key": str(i)} for i in range(3)]
    get = FakeGet([short, short])
    records = fetch_live_records(days=1, get=get)  # two slices

    assert len(records) == 6
    assert [c["params"]["$offset"] for c in get.calls] == [0, 0], get.calls


def test_the_retry_window_outlasts_the_measured_503_burst():
    """On 2026-09-30, 11 of 60 requests failed and the longest burst was 6
    consecutive 503s over ~5 s. The total backoff must cover that many times over."""
    total_wait = sum(HTTP_BACKOFF_SECONDS * 2 ** k for k in range(HTTP_ATTEMPTS - 1))
    assert total_wait >= 60, f"retries give up after {total_wait:g}s of waiting"


def test_row_cap_is_a_hard_failure():
    endless_page = [{"unique_key": str(i)} for i in range(PAGE_SIZE)]
    n_pages = LIVE_ROW_CAP // PAGE_SIZE + 3        # more than the cap allows
    get = FakeGet([endless_page] * n_pages)

    with pytest.raises(RuntimeError, match="cap"):
        fetch_live_records(get=get)
    # It must stop paging once the cap is breached, not fetch every page.
    assert len(get.calls) <= (LIVE_ROW_CAP // PAGE_SIZE) + 1 < n_pages


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


# A literal in this file so check_claims.py's AST counter can size the
# parametrisation below; the next test keeps it equal to local_runner's.
RETRYABLE_STATUSES = [429, 500, 502, 503, 504]


def test_retryable_status_list_matches_the_pipeline():
    """The local copy above must equal local_runner.HTTP_RETRYABLE_STATUS."""
    assert sorted(RETRYABLE_STATUSES) == sorted(HTTP_RETRYABLE_STATUS), (
        f"tests/local/test_live_fetch.py lists {sorted(RETRYABLE_STATUSES)} but "
        f"local_runner.HTTP_RETRYABLE_STATUS is {sorted(HTTP_RETRYABLE_STATUS)} — "
        f"the parametrised retry tests are no longer covering the real set."
    )


@pytest.mark.parametrize("status", RETRYABLE_STATUSES)
def test_transient_http_status_is_retried_then_succeeds(status):
    """`requests` returns 429 and 5xx as ordinary responses, not exceptions,
    so the retry must inspect the status."""
    get = FakeGet([FakeResponse([], status=status), [{"unique_key": "1"}]])
    records = fetch_live_records(get=get)

    assert records == [{"unique_key": "1"}], records
    assert len(get.calls) >= 2, f"HTTP {status} was not retried."


def test_non_retryable_http_status_fails_on_the_first_response():
    """A 404 or a malformed-query 400 is not transient: raise immediately."""
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
    return (datetime.now(UTC) - timedelta(days=offset)).date().isoformat()


def _grouped(**by_day):
    """One grouped Socrata response: {'2026-08-20': 10500} -> the JSON shape."""
    return [{"day": f"{d.replace('_', '-')}T00:00:00.000", "n": str(n)}
            for d, n in by_day.items()]


def test_source_counts_cover_the_whole_fetch_window_not_one_day():
    """The SLO gate picks its days later (ADR 015), so every day is captured."""
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
    """'The source says none' (0) and 'we never asked' (no row) are different
    facts, and slo2_completeness.sql treats them differently."""
    payload = _grouped(**{_day(3).replace("-", "_"): 10500})
    get = FakeGet([payload] * SOURCE_COUNT_PROBES)
    result = fetch_source_counts_window(days=4, get=get)

    assert {r["target_date"]: r["source_count"] for r in result} == {
        _day(4): 0, _day(3): 10500, _day(2): 0, _day(1): 0, _day(0): 0,
    }


def test_source_counts_take_the_per_day_maximum_across_disagreeing_replicas():
    """Replicas lag and only under-count, so the highest count is kept."""
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


def test_max_of_n_beats_every_other_estimator_on_the_measured_replica_shape():
    """On the measured shape (stale replica answering most probes), the mean,
    median, mode and last probe all return a stale value; only the max is
    right (ADR 016)."""
    key = _day(3).replace("-", "_")
    stale, fresh = 11_515, 11_627
    # 8 stale, 3 fresh, fresh in the middle: mean 11,545.5 (a count the source
    # never reported), median 11,515, mode 11,515, last 11,515.
    s, f = _grouped(**{key: stale}), _grouped(**{key: fresh})
    pages = [s] * 4 + [f] * 3 + [s] * 4
    assert len(pages) == SOURCE_COUNT_PROBES
    get = FakeGet(pages)

    result = {r["target_date"]: r for r in fetch_source_counts_window(days=4, get=get)}
    assert result[_day(3)]["source_count"] == fresh, (
        "The maximum is the estimator. On this distribution the mean (11,545.5) "
        "is a count the source never reported, and the median, the mode and the "
        "last probe all return the STALE 11,515."
    )


def test_probe_evidence_is_recorded_so_the_denominator_can_be_audited():
    """Each day records the probe count, the lowest count seen, and whether the
    probes disagreed, so `source_count - source_count_min` is the settling spread."""
    contested, settled = _day(3).replace("-", "_"), _day(1).replace("-", "_")
    pages = ([_grouped(**{contested: 11_515, settled: 10_857})] * 8
             + [_grouped(**{contested: 11_627, settled: 10_857})] * 3)
    result = {r["target_date"]: r for r in fetch_source_counts_window(days=4, get=FakeGet(pages))}

    still_settling = result[_day(3)]
    assert still_settling["source_count"] == 11_627
    assert still_settling["source_count_min"] == 11_515
    assert still_settling["probes_disagreed"] is True
    assert still_settling["source_count"] - still_settling["source_count_min"] == 112, (
        "The recorded spread is the auditable quantity — 112 rows at 3 days old."
    )

    assert result[_day(1)]["probes_disagreed"] is False, (
        "A day every probe agreed on must not be flagged as contested."
    )
    assert all(r["probe_count"] == SOURCE_COUNT_PROBES for r in result.values())


def test_a_day_only_one_replica_has_indexed_is_not_reported_as_unanimous():
    """A day missing from a probe's response is that probe's ZERO, so one
    sighting of 416 among ten empty responses reads as contested."""
    key = _day(1).replace("-", "_")
    result = {r["target_date"]: r
              for r in fetch_source_counts_window(
                  days=2, get=FakeGet([[]] * 10 + [_grouped(**{key: 416})]))}

    assert result[_day(1)]["source_count"] == 416
    assert result[_day(1)]["source_count_min"] == 0
    assert result[_day(1)]["probes_disagreed"] is True
    assert result[_day(1)]["probe_count"] == SOURCE_COUNT_PROBES


def test_probe_count_is_justified_by_the_measured_replica_split():
    """N is the smallest probe count with P(every probe stale) = 0.65^N under 1%.

    0.65 is the worst stale share measured in a single run (ADR 016).
    """
    worst_observed_stale_share = 0.65
    assert worst_observed_stale_share ** SOURCE_COUNT_PROBES < 0.01, (
        f"N={SOURCE_COUNT_PROBES} must hold P(every probe stale) under 1% at the "
        f"worst measured split — see ADR 016."
    )
    assert worst_observed_stale_share ** (SOURCE_COUNT_PROBES - 1) > 0.01, (
        f"N={SOURCE_COUNT_PROBES} must be the SMALLEST value that does, or the "
        f"pipeline is paying wall-clock and API calls for nothing."
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
