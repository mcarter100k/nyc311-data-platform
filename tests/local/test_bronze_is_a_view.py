"""
The Bronze contract from ADR 014: raw is a view over the file, never a copy.

  1. `bronze.service_requests` is a VIEW. A materialised table gives identical
     row counts downstream, so nothing else in the suite would notice.
  2. Stage 3 reads the raw FILE, not the Bronze relation. Both paths give
     identical Silver rows, so this one is asserted on the source text.

Runs stage2_bronze() on a fixture file, so nothing here skips.
"""

import json
import os
import sys

import duckdb
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RUNNER = os.path.join(ROOT, "local", "local_runner.py")

# Plain import, not importorskip: a broken import must be a red test.
if os.path.join(ROOT, "local") not in sys.path:
    sys.path.insert(0, os.path.join(ROOT, "local"))

import local_runner  # noqa: E402

# Fields Gold drops. The Bronze view is the only way back to them short of
# re-fetching, which is the whole practical argument for the view existing.
FIELDS_GOLD_DROPS = ("council_district", "bbl", "police_precinct")


@pytest.fixture(scope="module")
def bronze_db(tmp_path_factory):
    """Run the real stage2_bronze() with its path constants pointed at a tmp dir."""
    workdir = tmp_path_factory.mktemp("bronzeview")
    raw_dir = workdir / "raw"
    raw_dir.mkdir()
    raw_file = raw_dir / "nyc311_raw.json"

    # Two Socrata-shaped records carrying every field Gold drops. If stage 2
    # projects an explicit column list, the second test below goes red.
    raw_file.write_text(json.dumps([
        {
            "unique_key": "fixture-1", "created_date": "2026-01-02T10:00:00.000",
            "agency": "HPD", "complaint_type": "HEAT/HOT WATER",
            "borough": "BROOKLYN", "council_district": "33",
            "bbl": "3000010001", "police_precinct": "94",
        },
        {
            "unique_key": "fixture-2", "created_date": "2026-01-02T11:00:00.000",
            "agency": "NYPD", "complaint_type": "Noise - Residential",
            "borough": "QUEENS", "council_district": "26",
            "bbl": "4000020002", "police_precinct": "108",
        },
    ]))

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(local_runner, "DATA_DIR", workdir)
    monkeypatch.setattr(local_runner, "RAW_DIR", raw_dir)
    monkeypatch.setattr(local_runner, "RAW_FILE", raw_file)
    monkeypatch.setattr(local_runner, "DUCKDB_PATH", workdir / "nyc311_local.duckdb")

    local_runner.stage2_bronze()

    db_path = str(workdir / "nyc311_local.duckdb")
    assert os.path.exists(db_path), (
        "stage2_bronze() did not create a database at the patched DUCKDB_PATH. "
        "If local_runner stopped reading these module constants, repoint this "
        "fixture — do not let the tests below run against nothing."
    )
    yield db_path
    monkeypatch.undo()


def test_bronze_is_a_view_not_a_table(bronze_db):
    """ADR 014: Bronze is the raw file exposed as a view, never a materialised copy."""
    con = duckdb.connect(bronze_db, read_only=True)
    try:
        row = con.execute(
            """SELECT table_type FROM information_schema.tables
               WHERE table_schema = 'bronze' AND table_name = 'service_requests'"""
        ).fetchone()
    finally:
        con.close()

    assert row is not None, (
        "bronze.service_requests does not exist after stage2_bronze() ran. "
        "Stage 2 no longer creates the relation this whole layer is named for."
    )
    assert row[0] == "VIEW", (
        f"bronze.service_requests is a {row[0]}, expected VIEW. A materialised "
        f"Bronze means raw data is being written into the warehouse and read "
        f"back out again — the round-trip ADR 014 removed. Row counts are "
        f"identical either way, so nothing else in the suite catches this."
    )


def test_bronze_view_exposes_the_fields_gold_drops(bronze_db):
    """The view's practical justification: raw stays reachable without re-fetching."""
    con = duckdb.connect(bronze_db, read_only=True)
    try:
        cols = {c.lower() for c in
                con.execute("SELECT * FROM bronze.service_requests LIMIT 0").df().columns}
    finally:
        con.close()

    # Non-vacuity: assert the fixture actually carries these fields before
    # asserting the view surfaces them. Otherwise a fixture edit that dropped
    # them would turn this test into a check on nothing.
    raw = json.loads(open(os.path.join(os.path.dirname(str(bronze_db)),
                                       "raw", "nyc311_raw.json")).read())
    for field in FIELDS_GOLD_DROPS:
        assert field in raw[0], (
            f"the fixture raw record has no {field} — this test would pass "
            f"vacuously. Restore it."
        )

    for field in FIELDS_GOLD_DROPS:
        assert field in cols, (
            f"{field} is missing from the Bronze view. Gold drops it, so the "
            f"view is the only path back to it short of re-fetching the API."
        )


def test_silver_reads_the_raw_file_not_the_bronze_relation():
    """'Transform before load' — Silver's input is the file, not a warehouse read."""
    src = open(RUNNER).read()
    start = src.index("def stage3_silver")
    end = src.index("def ", start + 10)
    body = "\n".join(line.split("#", 1)[0] for line in src[start:end].splitlines())

    assert "FROM bronze.service_requests" not in body, (
        "stage3_silver reads from bronze.service_requests. That restores the "
        "load-then-transform round-trip: raw enters the warehouse, comes back "
        "out into pandas, and returns as Silver. Read RAW_FILE instead (ADR 014)."
    )
    assert "RAW_FILE" in body, (
        "stage3_silver no longer references RAW_FILE — it must transform the "
        "raw file directly, before anything is loaded (ADR 014)."
    )
