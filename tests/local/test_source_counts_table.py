"""
What `silver.source_counts` contains after a real stage3_silver run.

The probe evidence (`probe_count`, `source_count_min`, `probes_disagreed`,
ADR 016) must survive the trip through DuckDB, and a day captured again must
replace its earlier row.
"""

import json
import os
import sys

import pytest

duckdb = pytest.importorskip("duckdb", reason="duckdb not installed")
pytest.importorskip("pandas", reason="pandas not installed")

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOCAL_DIR = os.path.join(ROOT, "local")
if LOCAL_DIR not in sys.path:
    sys.path.insert(0, LOCAL_DIR)

RAW_RECORDS = [
    {"unique_key": "k1", "created_date": "2026-08-24T09:00:00",
     "closed_date": "2026-08-25T09:00:00", "status": "Closed",
     "borough": "BROOKLYN", "complaint_type": "Noise - Residential", "agency": "NYPD"},
]

# A real capture: a settled day, a still-settling day, and a 3-day-old day
# with a 112-row spread.
CAPTURE = [
    {"target_date": "2026-08-21", "source_count": 11521, "captured_at": "2026-08-27 05:30:00",
     "source_count_min": 11519, "probe_count": 11, "probes_disagreed": True},
    {"target_date": "2026-08-24", "source_count": 11627, "captured_at": "2026-08-27 05:30:00",
     "source_count_min": 11515, "probe_count": 11, "probes_disagreed": True},
    {"target_date": "2026-08-20", "source_count": 11061, "captured_at": "2026-08-27 05:30:00",
     "source_count_min": 11061, "probe_count": 11, "probes_disagreed": False},
]


def _run_stage3(workdir, capture):
    """Run the real stage3_silver with its paths pointed at `workdir`.

    local_runner reads its paths from module constants, so they are rebound
    around the call; the developer's own local/data/ is never touched.
    """
    import local_runner

    raw_file = workdir / "nyc311_raw.json"
    raw_file.write_text(json.dumps(RAW_RECORDS))
    count_file = workdir / "source_count.json"
    count_file.write_text(json.dumps(capture))
    db_path = workdir / "nyc311_local.duckdb"

    names = ("RAW_FILE", "DUCKDB_PATH", "DATA_DIR", "RAW_DIR", "SOURCE_COUNT_FILE")
    saved = {n: getattr(local_runner, n) for n in names}
    local_runner.RAW_FILE = raw_file
    local_runner.DUCKDB_PATH = db_path
    local_runner.DATA_DIR = workdir
    local_runner.RAW_DIR = workdir
    local_runner.SOURCE_COUNT_FILE = count_file
    try:
        local_runner.stage3_silver()
    finally:
        for n, v in saved.items():
            setattr(local_runner, n, v)
    return db_path


def _rows(db_path):
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        cols = [r[0] for r in con.execute("DESCRIBE silver.source_counts").fetchall()]
        rows = {
            str(r[0]): dict(zip(cols, r, strict=True))
            for r in con.execute(
                "SELECT * FROM silver.source_counts ORDER BY target_date").fetchall()
        }
    finally:
        con.close()
    return rows


def test_the_recorded_spread_is_what_makes_the_denominator_auditable(tmp_path):
    """`source_count - source_count_min` is the settling spread for that day."""
    rows = _rows(_run_stage3(tmp_path, CAPTURE))
    day = rows["2026-08-24"]
    assert day["source_count"] == 11627
    assert day["source_count_min"] == 11515
    assert day["source_count"] - day["source_count_min"] == 112
    assert day["probes_disagreed"] is True
    assert day["probe_count"] == 11

    settled = rows["2026-08-20"]
    assert settled["probes_disagreed"] is False, (
        "A day every probe agreed on must be distinguishable from a contested one."
    )
    assert settled["source_count"] == settled["source_count_min"]


def test_a_capture_without_probe_evidence_stores_null(tmp_path):
    """NULL means "not recorded", which is not `probe_count = 1`."""
    legacy_payload = [{"target_date": "2026-08-22", "source_count": 10047,
                       "captured_at": "2026-08-27 05:30:00"}]
    row = _rows(_run_stage3(tmp_path, legacy_payload))["2026-08-22"]

    assert row["source_count"] == 10047
    assert row["probe_count"] is None, (
        "A capture file with no probe metadata must record NULL, not a "
        "made-up probe count."
    )
    assert row["source_count_min"] is None
    assert row["probes_disagreed"] is None


def test_a_day_is_refreshed_in_place_rather_than_duplicated(tmp_path):
    """A later capture of the same day replaces the earlier row and its evidence."""
    _run_stage3(tmp_path, CAPTURE)
    # A second run: 2026-08-24 has aged a day and settled.
    settled = [{"target_date": "2026-08-24", "source_count": 11627,
                "captured_at": "2026-08-28 05:30:00",
                "source_count_min": 11627, "probe_count": 11,
                "probes_disagreed": False}]
    db_path = _run_stage3(tmp_path, settled)
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        n = con.execute("SELECT count(*) FROM silver.source_counts").fetchone()[0]
    finally:
        con.close()
    rows = _rows(db_path)

    assert n == len(CAPTURE), "One row per day — the capture refreshes, it does not append."
    assert rows["2026-08-24"]["probes_disagreed"] is False, (
        "The refreshed row must carry the later capture's evidence."
    )
    assert rows["2026-08-21"]["probes_disagreed"] is True, (
        "Days not in the later capture keep their earlier row."
    )
