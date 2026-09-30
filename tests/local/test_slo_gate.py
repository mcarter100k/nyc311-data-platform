"""
Behavioural tests for the SLO gate and the upstream-stall warning.

These run the REAL artifacts — `scripts/check_slos.py` as a subprocess over the
real `scripts/slo/*.sql`, and the real `QUERY` constant out of
`scripts/check_upstream_stall.py` — against a hand-seeded DuckDB. Nothing here
mocks the queries; a change to either file that breaks a verdict breaks a test.
"""

import os
import subprocess
import sys
from datetime import datetime, timedelta, UTC

import pytest

duckdb = pytest.importorskip("duckdb", reason="duckdb not installed — skipping SLO gate tests")

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CHECK_SLOS = os.path.join(ROOT, "scripts", "check_slos.py")
sys.path.insert(0, os.path.join(ROOT, "scripts"))
from check_upstream_stall import (MAX_COMPLETE_DAY_LAG_DAYS,  # noqa: E402
                                  QUERY as STALL_QUERY, verdict)

# UTC, not the session's date: the capture and the source both work in UTC.
TODAY = datetime.now(UTC).date()

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
    stamp = loaded_at or datetime.now(UTC).replace(tzinfo=None)

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


def run_gate(path, *extra):
    """The real gate binary. Returns (exit_code, stdout)."""
    result = subprocess.run([sys.executable, CHECK_SLOS, *extra, str(path)],
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


# ── SLO-1 ────────────────────────────────────────────────────────────────────

def test_slo1_measures_elapsed_hours_not_hour_boundaries(tmp_path):
    """25h58m old passes the 26-hour threshold; 26h30m fails.

    Counting hour boundaries crossed would read 25h58m as 26 on most runs.
    """
    now = datetime.now(UTC).replace(tzinfo=None)
    fresh = tmp_path / "fresh.duckdb"
    seed(fresh, HEALTHY, loaded_at=now - timedelta(hours=25, minutes=58))
    code, out = run_gate(fresh)
    assert code == 0, out

    stale = tmp_path / "stale.duckdb"
    seed(stale, HEALTHY, loaded_at=now - timedelta(hours=26, minutes=30))
    code, out = run_gate(stale)
    assert code == 1, out
    assert "SLO BREACH: slo1_freshness.sql" in out, out


# ── SLO-2 ────────────────────────────────────────────────────────────────────

def test_gate_passes_on_a_healthy_load_and_assesses_real_days(tmp_path):
    """Green, and the evidence shows it assessed the six whole days, not the stub."""
    db = tmp_path / "healthy.duckdb"
    seed(db, HEALTHY)
    code, out = run_gate(db)
    assert code == 0, out
    assert "complete_days_assessed=6" in out, out
    assert f"newest_complete_day={TODAY - timedelta(days=2)}" in out, out


def test_report_flag_may_come_before_the_db_path(tmp_path):
    """`--report r.md` must not be mistaken for the database path."""
    db = tmp_path / "healthy.duckdb"
    report = tmp_path / "r.md"
    seed(db, HEALTHY)
    code, out = run_gate(db, "--report", str(report))
    assert code == 0, out
    assert "complete_days_assessed=6" in report.read_text()


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
         datetime.now(UTC).replace(tzinfo=None), NORMAL - int(NORMAL * 0.90)],
    )
    con.close()
    code, out = run_gate(db)
    assert code == 0, out


def test_a_zero_source_count_on_a_complete_day_fails_instead_of_passing(tmp_path):
    """A day the load shows as published to midnight cannot have zero rows at
    the source: either the capture is wrong or the day was retracted."""
    db = tmp_path / "zero.duckdb"
    seed(db, [(2, True, NORMAL, 0)] + HEALTHY[1:])
    code, out = run_gate(db)
    assert code == 1, out
    assert "worst_day_rows_published=0" in out, out


def test_a_missing_source_count_on_a_complete_day_fails_closed(tmp_path):
    """A gate that cannot see its reference must not pass."""
    db = tmp_path / "missing.duckdb"
    seed(db, [(2, True, NORMAL, None)] + HEALTHY[1:])
    code, out = run_gate(db)
    assert code == 1, out
    assert "worst_day_rows_published=None" in out, out


def test_no_complete_day_in_the_window_fails_rather_than_passing_vacuously(tmp_path):
    """A multi-day publish stall wide enough to swallow the whole window.

    Nothing in the load is a whole day, so there is nothing to reconcile. That
    is a breach of the gate itself — the same rule check_slos.py applies when
    the SLO directory is empty. A gate that measured nothing must not pass,
    which is why this gates rather than merely warns; see ADR 015.
    """
    db = tmp_path / "nocomplete.duckdb"
    seed(db, [(i, False, 358, 358) for i in range(1, 8)])
    code, out = run_gate(db)
    assert code == 1, out
    assert "complete_days_assessed=0" in out, out


def test_a_day_that_fills_in_later_is_re_reconciled(tmp_path):
    """A stub day is skipped, then assessed once the source fills it in.

    Here the refill shows we hold only 358 of 10,500, so the gate reddens.
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
    """On the shape a healthy 10:00 UTC run produces, the warning stays quiet."""
    db = tmp_path / "healthy.duckdb"
    seed(db, HEALTHY)
    row = stall_row(db)
    stall, reasons = verdict(row)
    assert row["days_behind"] == MAX_COMPLETE_DAY_LAG_DAYS, row
    assert row["volume_ok"] is True, row
    assert not stall, f"fired on a healthy build: {reasons} / {row}"


def test_stall_warning_fires_when_the_source_stops_advancing(tmp_path):
    """One missed publish cycle: the newest complete day slips to T-3."""
    db = tmp_path / "behind.duckdb"
    seed(db, [(3 + i, True, NORMAL, NORMAL) for i in range(6)]
             + [(2, False, 358, 358), (1, False, 0, 0)])
    row = stall_row(db)
    stall, reasons = verdict(row)
    assert stall, row
    assert "days behind" in " ".join(reasons), reasons


def test_stall_warning_fires_on_a_source_side_volume_cliff(tmp_path):
    """A partial stall: the city publishes a day to midnight but only part-fills
    it. The horizon advances normally, so only the volume check catches it."""
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
    """volume_ok is NULL with no prior complete day to compare against, and
    "we cannot compare" is not evidence of a cliff."""
    db = tmp_path / "onlyone.duckdb"
    seed(db, [(2, True, NORMAL, NORMAL), (1, False, 358, 358)])
    row = stall_row(db)
    stall, reasons = verdict(row)
    assert row["volume_ok"] is None, row
    assert not stall, reasons
