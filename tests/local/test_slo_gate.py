"""
Behavioural tests for the SLO gate and the upstream-stall warning.

These run the REAL artifacts — `scripts/check_slos.py` as a subprocess over the
real `scripts/slo/*.sql`, and the real `QUERY` constant out of
`scripts/check_upstream_stall.py` — against a hand-seeded DuckDB. Nothing here
mocks the queries; a change to either file that breaks a verdict breaks a test.

WHY THIS FILE EXISTS. Until 2026-08-27 neither the SLO queries nor the stall
checker had a single test. Both were wrong for months in ways a test would have
caught in one line:

  * SLO-2 reconciled `current_date - 1`, which the source's publish lag
    guarantees is a ~2-hour stub or empty, and `WHEN source = 0 THEN true`
    turned the empty case into a PASS. The gate certified 3.5% of a day at
    best and nothing at all at worst.
  * The stall warning compared our own row count for that same stub day
    against a 7-day median of our own counts, so it fired on 100% of healthy
    runs.

Every test below is written so it can fail: each seeds a shape, asserts the
verdict, then mutates ONE thing and asserts the verdict flips.
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import pytest

duckdb = pytest.importorskip("duckdb", reason="duckdb not installed — skipping SLO gate tests")

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CHECK_SLOS = os.path.join(ROOT, "scripts", "check_slos.py")
sys.path.insert(0, os.path.join(ROOT, "scripts"))
from check_upstream_stall import (MAX_COMPLETE_DAY_LAG_DAYS,  # noqa: E402
                                  QUERY as STALL_QUERY, verdict)

# UTC, not the session's date: both the capture and the source work in UTC, and
# a laptop an hour west of Greenwich would otherwise seed a different shape than
# the CI runner. This is the same bug the queries themselves carried.
TODAY = datetime.now(timezone.utc).date()

# A day the source publishes normally. The exact figure does not matter to any
# assertion; it is realistic (measured median ~10,500) so failures read clearly.
NORMAL = 10_500


def seed(path, days, loaded_at=None):
    """Build a database with one row per (day_offset, complete, ours, source).

    `days` is a list of tuples: (offset_back_from_today, is_complete_day,
    rows_in_gold, source_count_or_None). A None source count means "no capture
    for this day", which is a distinct state from a captured zero.
    """
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE TABLE gold.int_load_completeness ("
                "load_day DATE, is_complete_day BOOLEAN)")
    con.execute("CREATE TABLE gold.fct_service_requests ("
                "created_date TIMESTAMP, _loaded_at TIMESTAMP)")
    con.execute("CREATE TABLE silver.source_counts ("
                "target_date DATE, source_count BIGINT, captured_at TIMESTAMP)")

    # SLO-1 reads max(_loaded_at); stamp it fresh so SLO-1 never confounds an
    # SLO-2 assertion below.
    stamp = loaded_at or datetime.now(timezone.utc).replace(tzinfo=None)

    for offset, complete, ours, source in days:
        day = TODAY - timedelta(days=offset)
        con.execute("INSERT INTO gold.int_load_completeness VALUES (?, ?)", [day, complete])
        if ours:
            con.execute(
                "INSERT INTO gold.fct_service_requests "
                "SELECT ?::TIMESTAMP + INTERVAL (i) SECOND, ?::TIMESTAMP "
                "FROM range(?) t(i)",
                [datetime.combine(day, datetime.min.time()), stamp, ours],
            )
        if source is not None:
            con.execute("INSERT INTO silver.source_counts VALUES (?, ?, ?)",
                        [day, source, stamp])
    con.close()


def run_gate(path):
    """The real gate binary. Returns (exit_code, stdout)."""
    result = subprocess.run([sys.executable, CHECK_SLOS, str(path)],
                            capture_output=True, text=True, cwd=ROOT, check=False)
    return result.returncode, result.stdout + result.stderr


def stall_row(path):
    con = duckdb.connect(str(path), read_only=True)
    rel = con.sql(STALL_QUERY)
    row = dict(zip(rel.columns, rel.fetchone(), strict=True))
    con.close()
    return row


# A healthy shape at 10:00 UTC: yesterday is the publish-lag stub, the day
# before is the newest complete day, and the days behind it are complete and
# fully loaded. `1` carries a deliberately bad ratio (358 loaded against 10,500
# published) so that any design which assessed the trailing partial day would
# fail these tests loudly instead of quietly.
HEALTHY = [
    (2 + i, True, NORMAL, NORMAL) for i in range(6)
] + [
    (1, False, 358, 358),
]


# ── SLO-2 ────────────────────────────────────────────────────────────────────

def test_gate_passes_on_a_healthy_load_and_assesses_real_days(tmp_path):
    """Green — and the evidence must show it certified WHOLE DAYS, not a stub.

    `complete_days_assessed` is asserted because a gate that passes while
    measuring nothing is the exact defect this redesign replaces. Six complete
    days at 10,500 rows is ~63,000 rows reconciled; the old query certified 358.
    """
    db = tmp_path / "healthy.duckdb"
    seed(db, HEALTHY)
    code, out = run_gate(db)
    assert code == 0, out
    assert "complete_days_assessed=6" in out, out
    assert f"newest_complete_day={TODAY - timedelta(days=2)}" in out, out


def test_gate_ignores_the_trailing_partial_day(tmp_path):
    """The stub day is excluded by population, not by luck.

    HEALTHY's partial day is loaded at 358 against a published 358, so it would
    pass anyway. Publish 10,500 for it instead — a day we loaded 3.4% of — and
    the gate must STILL be green, because that day is not complete and is
    therefore not this gate's business. If it reddens, the population is wrong.
    """
    db = tmp_path / "partial.duckdb"
    seed(db, HEALTHY[:-1] + [(1, False, 358, NORMAL)])
    code, out = run_gate(db)
    assert code == 0, out
    assert "complete_days_assessed=6" in out, out


def test_gate_fails_when_a_complete_day_is_short_loaded(tmp_path):
    """Break it, watch it fail, revert, watch it pass — on one database.

    A complete day loaded at 90% of what the source published is real loss and
    must redden. 0.90 is chosen to sit clearly under the 0.98 floor without
    being so extreme that the test would pass under any threshold.
    """
    db = tmp_path / "short.duckdb"
    broken = [(2, True, int(NORMAL * 0.90), NORMAL)] + HEALTHY[1:]
    seed(db, broken)
    code, out = run_gate(db)
    assert code == 1, out
    assert "SLO BREACH: slo2_completeness.sql" in out, out
    assert f"worst_day={TODAY - timedelta(days=2)}" in out, out
    assert f"worst_day_rows_loaded={int(NORMAL * 0.90)}" in out, out

    # Revert the one thing that was broken.
    con = duckdb.connect(str(db))
    con.execute(
        "INSERT INTO gold.fct_service_requests "
        "SELECT ?::TIMESTAMP + INTERVAL (i) SECOND, ?::TIMESTAMP FROM range(?) t(i)",
        [datetime.combine(TODAY - timedelta(days=2), datetime.min.time()),
         datetime.now(timezone.utc).replace(tzinfo=None), NORMAL - int(NORMAL * 0.90)],
    )
    con.close()
    code, out = run_gate(db)
    assert code == 0, out


def test_a_zero_source_count_on_a_complete_day_fails_instead_of_passing(tmp_path):
    """The branch that was `WHEN (SELECT n FROM source) = 0 THEN true`.

    A day the load shows as published through to midnight cannot also have zero
    rows at the source. Either the capture is wrong or the source retracted the
    day; both mean the gate cannot vouch for that day, and the old query said
    PASS. This is the single line that made the gate certify nothing whenever a
    capture landed on a lagging replica.
    """
    db = tmp_path / "zero.duckdb"
    seed(db, [(2, True, NORMAL, 0)] + HEALTHY[1:])
    code, out = run_gate(db)
    assert code == 1, out
    assert "worst_day_rows_published=0" in out, out


def test_a_missing_source_count_on_a_complete_day_fails_closed(tmp_path):
    """A gate that cannot see its reference must not pass. Unchanged rule,
    now applied per complete day rather than to one clock-chosen day."""
    db = tmp_path / "missing.duckdb"
    seed(db, [(2, True, NORMAL, None)] + HEALTHY[1:])
    code, out = run_gate(db)
    assert code == 1, out
    assert "worst_day_rows_published=None" in out, out


def test_no_complete_day_in_the_window_fails_rather_than_passing_vacuously(tmp_path):
    """A multi-day publish stall wide enough to swallow the whole window.

    Nothing in the load is a whole day, so there is nothing to reconcile. That
    is a breach of the gate itself — the same rule check_slos.py applies when
    the SLO directory is empty. The remedy is ours (widen the fetch window),
    which is why this gates rather than merely warns; see ADR 015.
    """
    db = tmp_path / "nocomplete.duckdb"
    seed(db, [(i, False, 358, 358) for i in range(1, 8)])
    code, out = run_gate(db)
    assert code == 1, out
    assert "complete_days_assessed=0" in out, out


def test_a_day_that_fills_in_later_is_re_reconciled(tmp_path):
    """The deeper bug: a day assessed while incomplete and never revisited.

    Day T-2 arrives as a 358-row stub and is correctly not assessed. The source
    then fills it in and the next run re-fetches and re-counts it — so it
    becomes a complete day with a real source count, and the gate now has an
    opinion about it. Here the refill exposes that we hold only 358 of 10,500,
    and the gate reddens; under the old design that day was reconciled once, on
    the morning it was guaranteed to be a stub, and never looked at again.
    """
    db = tmp_path / "refill.duckdb"
    seed(db, HEALTHY[:-1] + [(1, False, 358, 358), (0, False, 10, 10)])
    code, out = run_gate(db)
    assert code == 0, out

    con = duckdb.connect(str(db))
    con.execute("UPDATE gold.int_load_completeness SET is_complete_day = true "
                "WHERE load_day = ?", [TODAY - timedelta(days=1)])
    con.execute("UPDATE silver.source_counts SET source_count = ? WHERE target_date = ?",
                [NORMAL, TODAY - timedelta(days=1)])
    con.close()
    code, out = run_gate(db)
    assert code == 1, out
    assert f"worst_day={TODAY - timedelta(days=1)}" in out, out


# ── Upstream stall warning ───────────────────────────────────────────────────

def test_stall_warning_is_quiet_on_a_healthy_build(tmp_path):
    """The whole point. This check commented on issue #40 every single day from
    2026-08-20 because it measured the publish-lag stub against a 7-day median
    of full days. On the shape a healthy 10:00 UTC run produces it must say
    nothing at all."""
    db = tmp_path / "healthy.duckdb"
    seed(db, HEALTHY)
    row = stall_row(db)
    stall, reasons = verdict(row)
    assert row["days_behind"] == MAX_COMPLETE_DAY_LAG_DAYS, row
    assert row["volume_ok"] is True, row
    assert not stall, f"fired on a healthy build: {reasons} / {row}"


def test_stall_warning_fires_when_the_source_stops_advancing(tmp_path):
    """One missed publish cycle: the newest complete day slips to T-3. Measured
    on the live source 2026-08-27, which is exactly this shape."""
    db = tmp_path / "behind.duckdb"
    seed(db, [(3 + i, True, NORMAL, NORMAL) for i in range(6)]
             + [(2, False, 358, 358), (1, False, 0, 0)])
    row = stall_row(db)
    stall, reasons = verdict(row)
    assert stall, row
    assert "days behind" in " ".join(reasons), reasons


def test_stall_warning_fires_on_a_source_side_volume_cliff(tmp_path):
    """The partial stall ADR 013 recorded as a known limit: the city publishes a
    day right through to midnight but only part-fills it. The horizon advances
    normally, so only the volume half can catch this — and it is measured
    against the SOURCE's counts, which the old check never looked at."""
    db = tmp_path / "cliff.duckdb"
    thin = int(NORMAL * 0.20)
    seed(db, [(2, True, thin, thin)] + HEALTHY[1:])
    row = stall_row(db)
    stall, reasons = verdict(row)
    assert row["days_behind"] == MAX_COMPLETE_DAY_LAG_DAYS, row
    assert stall, row
    assert "median" in " ".join(reasons), reasons


def test_stall_warning_fires_when_no_day_is_complete(tmp_path):
    """No complete day anywhere in the window is the strongest stall signal
    there is. SLO-2 fails closed on the same shape; this names it."""
    db = tmp_path / "nocomplete.duckdb"
    seed(db, [(i, False, 358, 358) for i in range(1, 8)])
    row = stall_row(db)
    stall, reasons = verdict(row)
    assert stall, row
    assert "no complete day" in " ".join(reasons), reasons


def test_stall_warning_does_not_fire_merely_for_lacking_a_comparison(tmp_path):
    """volume_ok is NULL when there is no prior complete day to take a median
    of. "We cannot compare" is not evidence of a cliff, and the no-data case is
    already covered above — so NULL must not be read as a stall the way the old
    check read it."""
    db = tmp_path / "onlyone.duckdb"
    seed(db, [(2, True, NORMAL, NORMAL), (1, False, 358, 358)])
    row = stall_row(db)
    stall, reasons = verdict(row)
    assert row["volume_ok"] is None, row
    assert not stall, reasons


# ── The doc/query contract ───────────────────────────────────────────────────

def test_slo_doc_reproduces_the_queries_byte_for_byte():
    """check_claims.py enforces this in CI; asserting it here too means a local
    `pytest tests/local` catches the drift before the push does."""
    result = subprocess.run([sys.executable, os.path.join(ROOT, "scripts", "check_claims.py")],
                            capture_output=True, text=True, cwd=ROOT, check=False)
    assert "SLO drift" not in result.stdout, result.stdout


def test_source_count_file_shape_is_a_list_of_days(monkeypatch):
    """The capture writes one record per day of the window. A regression to the
    single-day object would silently reduce SLO-2's population to one day, and
    stage 3 would still load it — so the shape is asserted rather than assumed."""
    sys.path.insert(0, os.path.join(ROOT, "local"))
    from local_runner import fetch_source_counts_window
    monkeypatch.setattr("local_runner.time.sleep", lambda _seconds: None)

    class Resp:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return [{"day": (TODAY - timedelta(days=3)).isoformat() + "T00:00:00.000",
                     "n": "10500"}]

    payload = fetch_source_counts_window(days=4, get=lambda *a, **k: Resp())
    assert isinstance(payload, list)
    assert json.loads(json.dumps(payload))  # must be JSON-serialisable as written
    assert [p["target_date"] for p in payload] == [
        (TODAY - timedelta(days=d)).isoformat() for d in (4, 3, 2, 1, 0)
    ]
    assert [p["source_count"] for p in payload] == [0, 10500, 0, 0, 0], (
        "Days the source has no rows for must be recorded as an explicit 0 — "
        "'the source says none' and 'we never asked' are different facts and "
        "slo2_completeness.sql treats them differently."
    )
