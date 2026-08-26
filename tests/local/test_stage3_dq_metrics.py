"""
What silver.data_quality_log actually CONTAINS after a real stage3_silver run.

Why this file exists, stated plainly, because it is the whole point:
tests/unit/test_silver_transformations.py::test_data_quality_metrics calls
compute_dq_metrics directly, hands it correctly-separated frames, and passes.
It passed on the day this bug shipped and every day after. The function was
never wrong — the CALLER was. local_runner.stage3_silver passed the
POST-quarantine frame into the slot the duplicate count is derived from, so:

    duplicate_rate.records_failed = |bronze| - |post-quarantine|
                                  = duplicates + quarantined rows

and on live data, which contains no duplicate unique_keys at all, that number
was purely the quarantine count. `duplicate_rate` had never once measured a
duplicate. Alongside it, invalid_resolution_days and unrecognized_borough were
given the post-quarantine population as their denominator — a denominator that
excludes, by construction, the very rows their numerators count.

A unit test on the function is structurally incapable of seeing any of that.
So these tests run the real stage3_silver against a fixture raw file, with a
raw file whose duplicate count (2), quarantine count (3) and survivor count (4)
are all DIFFERENT numbers, and read the answers back out of DuckDB. If the
argument bug is reintroduced, duplicate_rate reports 5 instead of 2 and the
two deduped-population checks report a denominator of 4 instead of 7.
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


def _rec(unique_key, created, closed, status, borough):
    return {
        "unique_key": unique_key,
        "created_date": created,
        "closed_date": closed,
        "status": status,
        "borough": borough,
        "complaint_type": "Noise - Residential",
        "agency": "NYPD",
    }


# The fixture is built so that the three counts a correct run must distinguish
# are three DIFFERENT numbers. That is not decoration: with the shipped live
# data (0 duplicates, N quarantined) the buggy and correct duplicate counts
# happen to differ only by N, and with a fixture where duplicates == quarantine
# the buggy denominator for the other two checks would still look plausible.
#
#   9 raw rows  ->  7 after dedup (2 duplicate rows removed)
#               ->  4 written to Silver (3 quarantined)
#   2 of the 7 deduped rows carry an unrecognized borough
RAW_RECORDS = [
    # k1 and k2 each arrive twice — the ONLY true duplicates in this file.
    _rec("k1", "2024-01-01T09:00:00", "2024-01-05T09:00:00", "Closed", "BROOKLYN"),
    _rec("k1", "2024-01-01T09:00:00", "2024-01-05T09:00:00", "Closed", "BROOKLYN"),
    _rec("k2", "2024-01-02T09:00:00", None, "Open", "QUEENS"),
    _rec("k2", "2024-01-02T09:00:00", None, "Open", "QUEENS"),
    # k3/k4/k5 close BEFORE they were created — quarantined, never duplicated.
    _rec("k3", "2024-01-10T09:00:00", "2024-01-05T09:00:00", "Closed", "BRONX"),
    _rec("k4", "2024-01-11T09:00:00", "2024-01-06T09:00:00", "Closed", "MANHATTAN"),
    _rec("k5", "2024-01-12T09:00:00", "2024-01-07T09:00:00", "Closed", "ATLANTIS"),
    # k6 survives the quality filter but its borough is unrecognized, so the
    # unrecognized_borough numerator straddles both sides of the quarantine.
    _rec("k6", "2024-01-03T09:00:00", "2024-01-04T09:00:00", "Closed", "NOT A BOROUGH"),
    _rec("k7", "2024-01-04T09:00:00", "2024-01-04T09:00:00", "Closed", "STATEN ISLAND"),
]

N_BRONZE = 9
N_DEDUPED = 7          # k1..k7
N_DUPLICATE_ROWS = 2   # the second copy of k1 and of k2
N_QUARANTINED = 3      # k3, k4, k5
N_SILVER = 4           # k1, k2, k6, k7
N_UNRECOGNIZED = 2     # k5 (ATLANTIS, quarantined) and k6 (NOT A BOROUGH, kept)

CHECK_NAMES = {
    "null_rate_unique_key", "null_rate_created_date", "duplicate_rate",
    "invalid_resolution_days", "unrecognized_borough",
}


@pytest.fixture(scope="module")
def stage3_run(tmp_path_factory):
    """Run the REAL stage3_silver against a fixture raw file in a temp DuckDB.

    local_runner resolves its paths from module-level constants, so they are
    rebound around the call and restored afterwards — the developer's own
    local/data/ database is never touched. SOURCE_COUNT_FILE is pointed at a
    path that does not exist, which is the same branch a non-live run takes.
    """
    import local_runner

    workdir = tmp_path_factory.mktemp("stage3_dq")
    raw_file = workdir / "nyc311_raw.json"
    raw_file.write_text(json.dumps(RAW_RECORDS))
    db_path = workdir / "nyc311_local.duckdb"

    names = ("RAW_FILE", "DUCKDB_PATH", "DATA_DIR", "RAW_DIR", "SOURCE_COUNT_FILE")
    saved = {n: getattr(local_runner, n) for n in names}
    local_runner.RAW_FILE = raw_file
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
        log = {
            r[0]: {"records_checked": r[1], "records_failed": r[2],
                   "failure_rate": r[3], "pipeline_stage": r[4], "run_date": r[5]}
            for r in con.execute(
                "SELECT check_name, records_checked, records_failed, failure_rate,"
                " pipeline_stage, run_date FROM silver.data_quality_log"
            ).fetchall()
        }
        tables = {
            "silver": con.execute(
                "SELECT count(*) FROM silver.service_requests").fetchone()[0],
            "quarantine": con.execute(
                "SELECT count(*) FROM silver.quarantine").fetchone()[0],
        }
    finally:
        con.close()

    return {"log": log, "tables": tables}


def test_duplicate_rate_counts_duplicates_and_not_quarantined_rows(stage3_run):
    """The headline defect: duplicate_rate reported the quarantine count.

    2 duplicate rows and 3 quarantined rows, deliberately different numbers.
    The shipped bug computes |bronze| - |post-quarantine| = 9 - 4 = 5, which is
    duplicates + quarantined; on live data (0 duplicates) that is the quarantine
    count exactly, which is what silver.data_quality_log has always logged.
    """
    log = stage3_run["log"]
    assert stage3_run["tables"] == {"silver": N_SILVER, "quarantine": N_QUARANTINED}, (
        "Fixture drift — the rest of these assertions are calibrated to "
        f"{N_SILVER} written and {N_QUARANTINED} quarantined rows."
    )

    dup = log["duplicate_rate"]
    assert dup["records_failed"] == N_DUPLICATE_ROWS, (
        f"duplicate_rate reported {dup['records_failed']} duplicates; the raw "
        f"file contains {N_DUPLICATE_ROWS} duplicate rows and "
        f"{N_QUARANTINED} closed-before-created rows. A value of "
        f"{N_DUPLICATE_ROWS + N_QUARANTINED} means the POST-quarantine frame is "
        f"being passed to compute_dq_metrics again, so the check is measuring "
        f"quarantine rather than duplication."
    )
    assert dup["records_checked"] == N_BRONZE, (
        "duplicate_rate is measured against BRONZE — the rows deduplication "
        "actually saw."
    )

    # The other check that moves on quarantined rows must not agree with it.
    # Identical numerators across these two checks is the exact signature the
    # bug left in the shipped database.
    assert dup["records_failed"] != log["invalid_resolution_days"]["records_failed"], (
        "duplicate_rate and invalid_resolution_days report the same failure "
        "count on a fixture built to make them differ — duplicate_rate is "
        "echoing the quarantine count."
    )


def test_deduped_checks_are_measured_against_the_frame_they_checked(stage3_run):
    """A denominator must contain its own numerator.

    invalid_resolution_days and unrecognized_borough are evaluated on the
    deduped, PRE-quarantine frame. Handing them the post-quarantine population
    as records_checked gives 4 — a population from which all 3 invalid rows
    have already been removed, so the check reports failures that are not in
    the set it claims to have checked.
    """
    log = stage3_run["log"]

    invalid = log["invalid_resolution_days"]
    assert invalid["records_failed"] == N_QUARANTINED
    assert invalid["records_checked"] == N_DEDUPED, (
        f"invalid_resolution_days checked {invalid['records_checked']} records "
        f"while failing {invalid['records_failed']}. The rule ran over the "
        f"{N_DEDUPED} deduped rows; a denominator of {N_SILVER} is the "
        f"post-quarantine survivors, which by construction contains none of "
        f"the rows in this check's numerator."
    )

    boro = log["unrecognized_borough"]
    assert boro["records_failed"] == N_UNRECOGNIZED, (
        "One unrecognized borough is on a quarantined row and one is on a "
        "surviving row; both are real, and both are counted pre-quarantine."
    )
    assert boro["records_checked"] == N_DEDUPED, (
        "unrecognized_borough shares the deduped population with "
        "invalid_resolution_days — borough standardization runs on the same frame."
    )
    assert invalid["records_checked"] == boro["records_checked"], (
        "The two deduped-population checks must share one denominator."
    )


def test_dq_log_shape_bronze_denominators_and_rate_arithmetic(stage3_run):
    """The remaining contract fct_data_quality depends on, asserted end to end."""
    log = stage3_run["log"]
    assert set(log) == CHECK_NAMES, (
        "fct_data_quality's accepted_values test expects exactly these five checks."
    )

    # Null rates are measured on BRONZE: dedup collapses null unique_keys, so a
    # post-dedup measurement would be blind to them. This fixture has none.
    for name in ("null_rate_unique_key", "null_rate_created_date"):
        assert log[name]["records_checked"] == N_BRONZE, (
            f"{name} must be measured against the pre-dedup population."
        )
        assert log[name]["records_failed"] == 0

    for name, row in log.items():
        assert row["pipeline_stage"] == "silver"
        assert row["records_failed"] <= row["records_checked"], (
            f"{name} failed {row['records_failed']} of {row['records_checked']} "
            f"— a check cannot fail more rows than it checked, and a denominator "
            f"that excludes its own numerator is how that happens."
        )
        expected = round(row["records_failed"] / row["records_checked"], 6)
        assert row["failure_rate"] == pytest.approx(expected), (
            f"{name}: failure_rate {row['failure_rate']} does not equal "
            f"{row['records_failed']}/{row['records_checked']}."
        )
