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


def test_correction_reconciliation_deletes_disqualified_row(gold_db):
    """r6 was VALID in phase 1 (present in the fact) and corrected in phase 2
    so it now fails the closed-before-created quality filter. Without the
    reconciliation post_hook the merge would never touch it again and the
    fact would serve its stale pre-correction values forever — diverging from
    --full-refresh. Asserts the full lifecycle: built, then deleted, and both
    build modes agree."""
    assert "r6" in gold_db["phase1_fct_keys"], (
        "Precondition broken: r6 must exist in the fact after phase 1 — "
        "otherwise this test proves nothing about deletion."
    )
    assert "r6" not in gold_db["incremental"]["fct"], (
        "r6 was corrected to closed-before-created but still has a fact row "
        "after the incremental run — the reconciliation post_hook did not "
        "delete it, so incremental and full-refresh now diverge."
    )
    assert "r6" not in gold_db["full_refresh"]["fct"], (
        "r6 must not survive a full refresh — the quality filter in "
        "int_service_requests_cleaned should exclude it at build time."
    )


def test_no_fanout_and_full_refresh_idempotent(gold_db):
    """Grain (one row per service request) survives the SCD2 join: row count
    equals the silver population. And idempotency: a --full-refresh assigns
    exactly the same agency keys as the incremental path — same inputs, same
    outputs, regardless of build mode."""
    inc, full = gold_db["incremental"], gold_db["full_refresh"]

    # r4 is excluded by the watermark and r6 by the quality filter (corrected
    # to closed-before-created in phase 2), so incremental has
    # silver_count - 2 rows.
    assert inc["fct_rowcount"] == inc["silver_count"] - 2, (
        f"Row count {inc['fct_rowcount']} != silver {inc['silver_count']} - 2: "
        "the SCD2 join fanned out (or dropped) fact rows."
    )
    # Full refresh sees everything including r4 — but never quarantined r6.
    assert full["fct_rowcount"] == full["silver_count"] - 1, (
        "Full refresh must contain the entire silver population minus the "
        "quality-quarantined r6."
    )

    inc_keys = {k: v["agency_id"] for k, v in inc["fct"].items()}
    full_keys = {k: v["agency_id"] for k, v in full["fct"].items() if k in inc_keys}
    assert inc_keys == full_keys, (
        "agency_id assignment differs between incremental and --full-refresh "
        "builds — the join is not a pure function of the inputs."
    )


def test_recurrence_detects_a_repeat_and_respects_censoring(gold_db):
    """fct_complaint_recurrence must find a genuine repeat and must NOT claim
    a ticket did not recur when there was no time left to observe it.

    The seeded timeline gives r1 a closure followed by a same-address,
    same-type complaint, which is the signal the model exists to detect.
    observation_days must never be negative — a negative value would mean the
    horizon predates the closure, which would silently invert every rate
    computed from this table."""
    rows = gold_db["incremental"]["recurrence"]
    assert rows, "fct_complaint_recurrence built no rows for the seeded closures."

    by_key = {r["unique_key"]: r for r in rows}
    assert "r7" in by_key, "r7 closed with an address and must appear."
    assert by_key["r7"]["days_to_next"] == 2, (
        f"r7 closed 2024-01-02 and the same complaint was refiled at the same "
        f"address on 2024-01-04; expected days_to_next=2, got "
        f"{by_key['r7']['days_to_next']}."
    )

    for r in rows:
        assert r["observation_days"] >= 0, (
            f"{r['unique_key']} has observation_days={r['observation_days']}; a "
            "negative observation window inverts every rate built on this table."
        )
        if r["days_to_next"] is not None:
            assert r["days_to_next"] >= 0, (
                f"{r['unique_key']} recurs {r['days_to_next']} days BEFORE it closed."
            )
