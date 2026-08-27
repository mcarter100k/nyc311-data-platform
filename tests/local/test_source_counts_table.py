"""
What `silver.source_counts` actually CONTAINS after a real stage3_silver run.

The probe evidence added on 2026-08-27 (`probe_count`, `source_count_min`,
`probes_disagreed` — ADR 016) is only useful if it survives the trip through
DuckDB, and the way it could fail to is specific and easy to miss:

`stage3_silver` creates the table with `CREATE TABLE IF NOT EXISTS`, and the
scheduled daily run PERSISTS its DuckDB file across runs. Against a database
written before those columns existed, the CREATE is a no-op — so the table keeps
its old three-column shape forever and every INSERT of a six-value row fails on
arity. Nothing in the fetch-side unit tests can see that; they never touch a
database. These tests run the real stage 3 against a database deliberately built
in the OLD shape.

They also pin the honest-NULL contract: a capture file written before the probe
columns existed has no values for them, and NULL there means "captured without
probe evidence", which a reader must be able to tell apart from `probe_count = 1`.
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

LEGACY_ROW = ("2026-08-20", 11061, "2026-08-27 03:31:07")

# The 2026-08-27 capture, verbatim: a settled day, a still-settling day, and the
# 3-day-old day whose 112-row spread is SLO-2's largest loss term.
CAPTURE = [
    {"target_date": "2026-08-21", "source_count": 11521, "captured_at": "2026-08-27 05:30:00",
     "source_count_min": 11519, "probe_count": 11, "probes_disagreed": True},
    {"target_date": "2026-08-24", "source_count": 11627, "captured_at": "2026-08-27 05:30:00",
     "source_count_min": 11515, "probe_count": 11, "probes_disagreed": True},
    {"target_date": "2026-08-20", "source_count": 11061, "captured_at": "2026-08-27 05:30:00",
     "source_count_min": 11061, "probe_count": 11, "probes_disagreed": False},
]


def _run_stage3(workdir, capture):
    """Run the REAL stage3_silver against a database in the PRE-migration shape.

    local_runner resolves its paths from module-level constants, so they are
    rebound around the call and restored afterwards — the developer's own
    local/data/ database is never touched.
    """
    import local_runner

    raw_file = workdir / "nyc311_raw.json"
    raw_file.write_text(json.dumps(RAW_RECORDS))
    count_file = workdir / "source_count.json"
    count_file.write_text(json.dumps(capture))
    db_path = workdir / "nyc311_local.duckdb"

    # The shape a database written before 2026-08-27 has, with a row already in
    # it. `CREATE TABLE IF NOT EXISTS` cannot fix this; only the ALTER can.
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("""CREATE TABLE silver.source_counts (
                       target_date DATE, source_count BIGINT, captured_at TIMESTAMP)""")
    con.execute("INSERT INTO silver.source_counts VALUES (?, ?, ?)", list(LEGACY_ROW))
    con.close()

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
    return cols, rows


@pytest.fixture(scope="module")
def migrated(tmp_path_factory):
    return _rows(_run_stage3(tmp_path_factory.mktemp("source_counts"), CAPTURE))


def test_stage3_migrates_a_pre_existing_table_to_carry_probe_evidence(migrated):
    """The daily run persists its database, so stage 3 must be the migration.

    Without `ADD COLUMN IF NOT EXISTS` this test fails at the INSERT — six
    values into a three-column table — which is exactly what a scheduled run
    would have done on the morning after this change shipped.
    """
    cols, _ = migrated
    assert cols == ["target_date", "source_count", "captured_at",
                    "source_count_min", "probe_count", "probes_disagreed"]


def test_the_recorded_spread_is_what_makes_the_denominator_auditable(migrated):
    """`source_count - source_count_min` IS the settling spread for that day.

    112 rows at 3 days old is the term that consumes most of SLO-2's loss
    budget (ADR 016). A reader who cannot recompute it from the table has to
    take the denominator on trust.
    """
    _, rows = migrated
    day = rows["2026-08-24"]
    assert day["source_count"] == 11627
    assert day["source_count_min"] == 11515
    assert day["source_count"] - day["source_count_min"] == 112
    assert day["probes_disagreed"] is True
    assert day["probe_count"] == 11

    settled = rows["2026-08-20"]
    assert settled["probes_disagreed"] is False, (
        "A day every probe agreed on is settled, and must be distinguishable "
        "from one that was contested."
    )
    assert settled["source_count"] == settled["source_count_min"]


def test_a_row_captured_without_probe_evidence_stays_distinguishable(tmp_path):
    """NULL means "we did not record this", which is not `probe_count = 1`.

    Two ways a row gets there, both covered: it predates the columns (the
    legacy row seeded above, which is 2026-08-21 — deliberately NOT re-captured
    by this run so it survives untouched), or its capture file does. Defaulting
    either to a made-up value would let an unaudited denominator read as an
    audited one.
    """
    legacy_payload = [{"target_date": "2026-08-22", "source_count": 10047,
                       "captured_at": "2026-08-27 05:30:00"}]
    _, rows = _rows(_run_stage3(tmp_path, legacy_payload))

    old = rows["2026-08-20"]        # written before the ALTER ran
    assert old["source_count"] == 11061
    assert old["probe_count"] is None
    assert old["source_count_min"] is None
    assert old["probes_disagreed"] is None

    from_old_capture = rows["2026-08-22"]
    assert from_old_capture["source_count"] == 10047
    assert from_old_capture["probe_count"] is None, (
        "A capture file with no probe metadata must record NULL, not a "
        "fabricated probe count."
    )


def test_a_day_is_refreshed_in_place_rather_than_duplicated(tmp_path):
    """Overwrite-by-date is what makes a still-settling day re-reconcilable.

    The same day captured on a later run must REPLACE the earlier capture —
    including its probe evidence — or the gate would join to two rows for one
    day and `silver.source_counts` would stop being one row per day.
    """
    workdir = tmp_path
    db_path = _run_stage3(workdir, CAPTURE)

    import local_runner
    # A second run: 2026-08-24 has aged a day and settled.
    settled = [{"target_date": "2026-08-24", "source_count": 11627,
                "captured_at": "2026-08-28 05:30:00",
                "source_count_min": 11627, "probe_count": 11,
                "probes_disagreed": False}]
    (workdir / "source_count.json").write_text(json.dumps(settled))
    names = ("RAW_FILE", "DUCKDB_PATH", "DATA_DIR", "RAW_DIR", "SOURCE_COUNT_FILE")
    saved = {n: getattr(local_runner, n) for n in names}
    local_runner.RAW_FILE = workdir / "nyc311_raw.json"
    local_runner.DUCKDB_PATH = db_path
    local_runner.DATA_DIR = workdir
    local_runner.RAW_DIR = workdir
    local_runner.SOURCE_COUNT_FILE = workdir / "source_count.json"
    try:
        local_runner.stage3_silver()
    finally:
        for n, v in saved.items():
            setattr(local_runner, n, v)

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        n = con.execute("SELECT count(*) FROM silver.source_counts "
                        "WHERE target_date = '2026-08-24'").fetchone()[0]
        disagreed = con.execute("SELECT probes_disagreed FROM silver.source_counts "
                                "WHERE target_date = '2026-08-24'").fetchone()[0]
    finally:
        con.close()

    assert n == 1, "One row per day — the capture refreshes, it does not append."
    assert disagreed is False, (
        "The refreshed row must carry the LATER capture's evidence; a day that "
        "has settled since must stop reading as contested."
    )
