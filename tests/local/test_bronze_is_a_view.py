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
"""

import os

import duckdb
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(ROOT, "local", "data", "nyc311_local.duckdb")
RUNNER = os.path.join(ROOT, "local", "local_runner.py")


@pytest.mark.skipif(not os.path.exists(DB_PATH), reason="no local database built")
def test_bronze_is_a_view_not_a_table():
    """ADR 014: Bronze is the raw file exposed as a view, never a materialised copy."""
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        row = con.execute(
            """SELECT table_type FROM information_schema.tables
               WHERE table_schema = 'bronze' AND table_name = 'service_requests'"""
        ).fetchone()
    finally:
        con.close()

    assert row is not None, "bronze.service_requests does not exist — run stage 2."
    assert row[0] == "VIEW", (
        f"bronze.service_requests is a {row[0]}, expected VIEW. A materialised "
        f"Bronze means raw data is being written into the warehouse and read "
        f"back out again — the round-trip ADR 014 removed. Row counts are "
        f"identical either way, so nothing else in the suite catches this."
    )


@pytest.mark.skipif(not os.path.exists(DB_PATH), reason="no local database built")
def test_bronze_view_exposes_the_fields_gold_drops():
    """The view's practical justification: raw stays reachable without re-fetching."""
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        cols = {c.lower() for c in
                con.execute("SELECT * FROM bronze.service_requests LIMIT 0").df().columns}
    finally:
        con.close()

    for field in ("council_district", "bbl", "police_precinct"):
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
