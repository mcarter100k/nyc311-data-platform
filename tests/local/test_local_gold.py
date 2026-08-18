"""
Behavioral tests for Gold-layer semantics, executed against the real dbt
project in local/ on a seeded DuckDB database (fixture: tests/local/conftest.py).

These cover the gaps the structural suite cannot: it asserts what the compiled
manifest *declares*; these assert what the models *do* when built twice over
changing data — the incremental (only-new-rows) watermark, snapshot rename
detection, and the SCD2 point-in-time agency join.
"""


def test_incremental_lookback_picks_up_late_arriving_row(gold_db):
    """A row with an OLD business date but a fresh _silver_timestamp must be
    merged by the incremental run — the watermark keys on pipeline time
    (_loaded_at), not created_date. If this fails, the watermark is filtering
    on the wrong column and late-arriving history is silently lost."""
    fct = gold_db["incremental"]["fct"]
    assert "r3" in fct, (
        "Late-arriving row r3 (created 2020, loaded in phase 2) missing from "
        "fct_service_requests — the incremental watermark dropped it."
    )


def test_rows_older_than_lookback_are_not_merged(gold_db):
    """A row whose _silver_timestamp predates (previous watermark - 1h) is NOT
    picked up. This is the documented boundary of the lookback contract: any
    future Silver→warehouse sync must land rows within one hour, or they are
    lost exactly like r4 here (see sources.yml and ADR 008)."""
    fct = gold_db["incremental"]["fct"]
    assert "r4" not in fct, (
        "r4 carries a pipeline timestamp older than the lookback window and "
        "should have been excluded — if it appears, the incremental filter is "
        "not applying the watermark at all."
    )


def test_scd2_rename_versions_and_point_in_time_assignment(gold_db):
    """The rename lands as a second dim_agency version (snapshot dedup takes
    the most recent name), and each fact row carries the version in effect on
    its created_date: r2 (created before the rename was observed) keeps v1;
    r5 (created on the rename's effective day — the boundary day) gets v2."""
    res = gold_db["incremental"]
    nypd = [row for row in res["dim_agency"] if row[0] == "NYPD"]
    assert len(nypd) == 2, (
        f"Expected 2 NYPD versions after the rename, found {len(nypd)}: {nypd}. "
        "Either the snapshot dedup suppressed the new name or the check "
        "strategy did not open a version."
    )
    v1, v2 = nypd  # ordered by valid_from
    assert v1[5] is False and v2[5] is True, "is_current must move to the new version"

    fct = res["fct"]
    assert fct["r2"]["agency_id"] == v1[2], (
        "r2 was created before the rename took effect and must keep the v1 "
        "agency_key — reassignment means the join is not point-in-time."
    )
    assert fct["r5"]["agency_id"] == v2[2], (
        "r5 was created on the new version's effective day (boundary day) and "
        "must resolve to v2 under the half-open [valid_from, expiry) window."
    )


def test_upsert_propagates_status_change(gold_db):
    """r2 closes between the two loads (same natural key, fresh pipeline
    timestamp). The incremental upsert must UPDATE the existing fact row, not
    duplicate it or leave it stale. This path only receives data at all
    because the ingest watermark re-fetches updated rows (ingest_config.py) —
    it was dead code under the old created_date predicate.

    NOTE: this runs under dbt-duckdb's delete+insert strategy; the Snowflake
    project's `merge` strategy is spec-level and unverified (no warehouse)."""
    fct = gold_db["incremental"]["fct"]
    assert fct["r2"]["status"] == "Closed", (
        f"r2 status is {fct['r2']['status']!r} after the incremental run — "
        "the merge update path did not propagate the status change."
    )
    assert fct["r2"]["is_resolved"] is True


def test_no_fanout_and_full_refresh_idempotent(gold_db):
    """Grain (one row per service request) survives the SCD2 join: row count
    equals the silver population. And idempotency: a --full-refresh assigns
    exactly the same agency keys as the incremental path — same inputs, same
    outputs, regardless of build mode."""
    inc, full = gold_db["incremental"], gold_db["full_refresh"]

    # r4 is excluded by the watermark, so incremental has silver_count - 1 rows.
    assert inc["fct_rowcount"] == inc["silver_count"] - 1, (
        f"Row count {inc['fct_rowcount']} != silver {inc['silver_count']} - 1: "
        "the SCD2 join fanned out (or dropped) fact rows."
    )
    # Full refresh sees everything including r4.
    assert full["fct_rowcount"] == full["silver_count"], (
        "Full refresh must contain the entire silver population."
    )

    inc_keys = {k: v["agency_id"] for k, v in inc["fct"].items()}
    full_keys = {k: v["agency_id"] for k, v in full["fct"].items() if k in inc_keys}
    assert inc_keys == full_keys, (
        "agency_id assignment differs between incremental and --full-refresh "
        "builds — the join is not a pure function of the inputs."
    )
