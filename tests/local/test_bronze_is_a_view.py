"""
The Bronze contract from ADR 014: raw is a view over the file, never a copy.

Two properties are asserted, and they fail for different reasons:

  1. `bronze.service_requests` is a VIEW. If someone restores the materialised
     table, the pipeline silently goes back to writing raw data into the
     warehouse and reading it straight back out — the round-trip ADR 014
     removed. Nothing else in the suite would notice, because the row counts
     and every downstream number stay identical either way. That is exactly
     what makes it worth a test: the regression is invisible in the output.

  2. Stage 3 reads the raw FILE, not the Bronze relation. This is the actual
     "transform before load" property. A future edit could keep Bronze a view
     and still point Silver at it, which would restore the ambiguity while
     leaving property 1 satisfied.

Property 2 is asserted against the source text rather than behaviour: both
paths produce identical Silver rows, so there is no observable difference to
assert on. A source assertion is the weaker instrument and is used here only
because the stronger one does not exist.

HOW PROPERTY 1 IS REACHED — and why it used to be unreachable. These two tests
were `skipif(not os.path.exists(local/data/nyc311_local.duckdb))`, a database
that only a full local pipeline run against the live Socrata API produces. No
CI job has ever built it: ci.yml's behavioral-duckdb job installs and runs
`pytest tests/local` with no ingest step, and its own logs say so — run
33039271264 on main reported `61 passed, 2 skipped`, the two skips being
exactly these tests, and the job was green. They had therefore never once
asserted that Bronze is a view. Verified inert before this rewrite: with
stage2_bronze changed to CREATE OR REPLACE TABLE — the precise regression
described above — the file still reported `1 passed, 2 skipped`.

They now RUN stage2_bronze() for real, against a fixture raw file in a tmp
directory, so the property is checked on every machine and in every CI job with
no network, no credentials and no prior pipeline run. Nothing here skips. The
fixture is deliberately a stand-in for the API payload, so these tests prove
what the CODE does with a raw file (exposes it as a view, projects nothing
away); they do not and cannot prove what fields Socrata currently serves.
"""

import json
import os
import sys

import duckdb
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RUNNER = os.path.join(ROOT, "local", "local_runner.py")

# local/ is not a package — same sys.path idiom as test_live_fetch.py. Imported
# unconditionally rather than via importorskip: a broken import here must be a
# RED test, not a skip. The directory-wide duckdb/dbt-duckdb gate lives in
# conftest.py and is the only skip this tier is allowed.
if os.path.join(ROOT, "local") not in sys.path:
    sys.path.insert(0, os.path.join(ROOT, "local"))

import local_runner  # noqa: E402

# Fields Gold drops. The Bronze view is the only way back to them short of
# re-fetching, which is the whole practical argument for the view existing.
FIELDS_GOLD_DROPS = ("council_district", "bbl", "police_precinct")


@pytest.fixture(scope="module")
def bronze_db(tmp_path_factory):
    """Run the REAL stage2_bronze() against a fixture raw file.

    local_runner resolves its paths into module-level constants at import time,
    so they are repointed at a tmp directory here. That is what lets the actual
    shipped function — not a copy of it — build the relation these tests then
    inspect.
    """
    workdir = tmp_path_factory.mktemp("bronzeview")
    raw_dir = workdir / "raw"
    raw_dir.mkdir()
    raw_file = raw_dir / "nyc311_raw.json"

    # Two records shaped like the Socrata payload, carrying every field Gold
    # drops. read_json_auto infers the schema from the file, so if stage 2 ever
    # starts projecting an explicit column list these fields disappear and the
    # second test below goes red.
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
