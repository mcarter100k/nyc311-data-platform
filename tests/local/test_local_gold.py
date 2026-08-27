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


def test_silver_quarantine_reconciliation_deletes_stranded_row(gold_db):
    """r9 was VALID in phase 1 and QUARANTINED BY SILVER in phase 2.

    This is the case r6 above cannot cover. r6 stays in silver.service_requests
    and merely fails the dbt quality filter, so it is present in staging and
    absent from int — exactly the shape the original post_hook looks for.

    r9 leaves silver entirely, because quarantine runs in pandas before dbt sees
    anything. It is therefore absent from staging too, which makes the original
    post_hook structurally blind to it: a row loaded before it became invalid
    stayed in the fact table indefinitely, and the serving layer kept publishing
    a record the pipeline's own quality rules had rejected.

    Found 2026-08-22 by an end-to-end run after the fetch window moved — two
    real requests served as "In Progress" in Gold while the source reported them
    closed seconds before they were created. Nothing failed at the time: row
    counts stayed plausible and the fact table simply undercounted closures by
    two. Asserts the whole lifecycle, because "never built" would prove nothing.
    """
    assert "r9" in gold_db["phase1_fct_keys"], (
        "Precondition broken: r9 must exist in the fact after phase 1 — "
        "otherwise this test proves nothing about deletion."
    )
    assert "r9" not in gold_db["incremental"]["fct"], (
        "r9 was quarantined by Silver in phase 2 but still has a fact row after "
        "the incremental run. The quarantine post_hook on fct_service_requests "
        "did not delete it, so Gold is serving a row Silver rejected."
    )
    assert "r9" not in gold_db["full_refresh"]["fct"], (
        "r9 must not survive a full refresh either — it is absent from silver, "
        "so no build mode should produce it."
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


# ── The observation horizon ──────────────────────────────────────────────────
# observation_days was measured against max(created_date) over the load. That
# reads as obviously right and is systematically wrong: the source publishes on
# a ~23.5h lag, so its newest created_date is never a whole day — measured on
# live loads it held 358, 372, 382 and 832 rows against a ~10,500 median. Every
# closure was therefore credited with up to a full day of observation that had
# not happened, and the error was differential across closure_type, which is the
# one dimension this table exists to compare.
#
# The fixture timeline is built so the two horizons differ loudly: only Jan 4 is
# a complete day, while the newest CREATED day is TODAY. Old horizon → ~950;
# correct horizon → 1 and 2.

def test_completeness_marks_only_the_fully_published_day(gold_db):
    """int_load_completeness must judge each day on its own tail coverage, not
    on being the newest day. The fixture publishes Jan 4 through 23:50 and every
    other day only into working hours."""
    rows = {r["load_day"]: r for r in gold_db["incremental"]["completeness"]}
    assert rows, "int_load_completeness built no rows."

    assert rows["2024-01-04"]["is_complete_day"] is True, (
        "Jan 4 is covered to 23:50 — inside the 60-minute tail window — and must "
        f"be complete. Got {rows['2024-01-04']}."
    )
    for day in ("2024-01-01", "2024-01-02", "2024-01-03"):
        assert rows[day]["is_complete_day"] is False, (
            f"{day}'s newest request is a working-hours one, leaving hundreds of "
            f"minutes of the day unpublished; it must not read complete. "
            f"Got {rows[day]}."
        )

    newest = max(rows)
    assert rows[newest]["is_complete_day"] is False, (
        f"The newest loaded day ({newest}) reads as complete. That is the exact "
        "shape of the defect: under a ~23.5h publish lag the newest day is "
        "always partial, and the horizon must not sit on it."
    )


def test_observation_days_measure_to_the_last_complete_day(gold_db):
    """The regression test for the defect itself, in numbers.

    Horizon = Jan 4 (the only complete day), NOT the newest created day. r1
    closed Jan 3 → 1 day observed. r2 closed Jan 4 → 0, floored because nothing
    published follows it. Under the old max(created_date) horizon both would be
    in the high hundreds."""
    by_key = {r["unique_key"]: r for r in gold_db["incremental"]["recurrence"]}

    assert by_key["r1"]["observation_days"] == 1, (
        f"r1 closed 2024-01-03 and the last COMPLETE published day is 2024-01-04, "
        f"so exactly one day of history follows it. Got "
        f"{by_key['r1']['observation_days']} — a value in the hundreds means the "
        f"horizon is back on max(created_date), which is a partial day."
    )
    assert by_key["r2"]["observation_days"] == 0, (
        f"r2 closed 2024-01-04, the last complete day itself, so no published "
        f"history follows it. Got {by_key['r2']['observation_days']}."
    )
    assert by_key["r7"]["observation_days"] == 2, (
        f"r7 closed 2024-01-02, two days before the horizon. Got "
        f"{by_key['r7']['observation_days']}."
    )


def test_horizon_tests_pass_on_a_clean_build(gold_db):
    """Precondition for the two sabotage tests below: both guards are green
    before anything is broken, so a failure there is attributable."""
    clean = gold_db["guards"]["clean"]
    assert clean["returncode"] == 0, (
        "The horizon guards fail on an unsabotaged build — they are not "
        "measuring what they claim:\n" + clean["output"][-4000:]
    )


def test_horizon_guard_fires_when_the_horizon_runs_past_the_last_complete_day(gold_db):
    """Non-vacuity, direction one. Advancing the horizon by a day is the defect
    verbatim. It floors nothing, so the floor guard must stay GREEN and only the
    horizon guard may fire — otherwise the two tests are one test."""
    sabotage = gold_db["guards"]["horizon_advanced"]
    assert sabotage["returncode"] != 0, (
        "observation_days was advanced by a day — the horizon now sits one day "
        "past the last completely published day, which is precisely the bug this "
        "model was shipped with — and dbt test still passed.\n"
        + sabotage["output"][-4000:]
    )
    assert "assert_recurrence_horizon_is_last_complete_day" in sabotage["output"], (
        "dbt failed, but not in the horizon test — the guard is proving "
        "something other than what it claims.\n" + sabotage["output"][-4000:]
    )
    assert "PASS assert_observation_days_floor_is_explained" in sabotage["output"], (
        "The floor guard also fired on an over-advanced horizon. It is supposed "
        "to catch the opposite failure; if it fires on both, one of the two "
        "tests is redundant.\n" + sabotage["output"][-4000:]
    )


def test_floor_guard_fires_when_every_row_is_floored(gold_db):
    """Non-vacuity, direction two — and the specific thing the old test could
    not see. `observation_days >= 0` sat on GREATEST(0, ...), so a horizon that
    froze or fell behind floored every row and the test reported PASS while the
    sample silently drained out of every `observation_days >= N` filter."""
    sabotage = gold_db["guards"]["all_floored"]
    assert sabotage["returncode"] != 0, (
        "Every observation_days was set to 0 — the state a frozen or backdated "
        "horizon produces — and dbt test still passed. This is exactly the "
        "condition the old `>= 0` test was blind to.\n"
        + sabotage["output"][-4000:]
    )
    assert "assert_observation_days_floor_is_explained" in sabotage["output"], (
        "dbt failed, but not in the floor test.\n" + sabotage["output"][-4000:]
    )


def test_horizon_guards_recover_after_the_model_is_rebuilt(gold_db):
    """Both failures above must be caused by the sabotage and nothing else:
    rebuild fct_complaint_recurrence from its source and they go green again."""
    reverted = gold_db["guards"]["reverted"]
    assert reverted["returncode"] == 0, (
        "The horizon guards stayed red after fct_complaint_recurrence was "
        "rebuilt, so the earlier failures cannot be attributed to the "
        "sabotage:\n" + reverted["output"][-4000:]
    )


# ── Referential integrity across a moving Silver window ──────────────────────
# fct_service_requests accumulates; Silver carries a rolling 7-day window. Any
# dimension rebuilt from that window therefore forgets members the fact still
# references. Measured on the production artifact before the fix: 88 dangling
# location_id values, every one of them predating the window, growing ~15-20 a
# day, and completely silent — fct_daily_volume coalesced the failed join to
# borough 'UNSPECIFIED' and the row counts stayed plausible.

def test_dim_location_keeps_members_whose_rows_left_the_window(location_retention_db):
    """A location must not disappear from dim_location because its source rows
    aged out of Silver. This is the actual decay mechanism: nothing rejected
    those rows, the window simply moved past them."""
    phase1, phase2 = location_retention_db["phase1"], location_retention_db["phase2"]

    assert "QUEENS" in phase1["boroughs"], (
        "Precondition broken: Queens must be in dim_location after phase 1, "
        "otherwise this test proves nothing about retention."
    )
    assert "QUEENS" in phase2["boroughs"], (
        "Queens left Silver's window in phase 2 and its dim_location member was "
        "dropped with it. dim_location is being rebuilt from the window instead "
        "of accumulating, so every fact row at that location now has a dangling "
        f"location_id. dim_location went from {phase1['dim_rowcount']} members "
        f"to {phase2['dim_rowcount']}."
    )


def test_no_orphaned_location_ids_after_the_window_moves(location_retention_db):
    """The failure this whole fixture exists for: fact rows pointing at a
    location_id that is no longer in dim_location."""
    phase2 = location_retention_db["phase2"]
    assert phase2["orphans"] == 0, (
        f"{phase2['orphans']} fact rows reference a location_id absent from "
        "dim_location after the window moved. The FK is dangling: joins to "
        "dim_location silently drop these rows, and fct_daily_volume reports "
        "their volume under borough 'UNSPECIFIED'."
    )


def test_window_move_does_not_shrink_accumulated_gold(location_retention_db):
    """The fix must not be 'delete the orphans'. Rows that left Silver's window
    were not rejected by anything — their accumulated fact rows stay, which is
    the entire point of the fact being incremental."""
    phase1, phase2 = location_retention_db["phase1"], location_retention_db["phase2"]

    for key in ("q1", "q2"):
        assert key in phase1["fct_keys"], f"Precondition broken: {key} missing after phase 1."
        assert key in phase2["fct_keys"], (
            f"{key} left Silver's window and its fact row was deleted. Nothing "
            "rejected that row — it is outside the current fetch, not invalid. "
            "Deleting it silently rewrites history."
        )
    assert phase2["fct_rowcount"] == phase1["fct_rowcount"] + 1, (
        f"Expected the phase-2 fact to hold phase 1's {phase1['fct_rowcount']} rows "
        f"plus the one new request, got {phase2['fct_rowcount']}."
    )


def test_phase2_build_passes_its_own_relationships_tests(location_retention_db):
    """`dbt build` runs each model's tests as part of the build, so a dangling
    FK must redden the run that created it — not a later audit."""
    assert location_retention_db["phase2_build_returncode"] == 0, (
        "The phase-2 dbt build failed:\n"
        + location_retention_db["phase2_build_output"][-4000:]
    )


def test_relationships_test_on_location_id_actually_fires(location_retention_db):
    """Non-vacuity. Every assertion above stays green if someone deletes the
    relationships test from marts.yml — that is exactly how the guard was lost
    the first time, on the reasoning that a dimension sharing the fact's source
    could never dangle. Drop a dim_location member by hand and dbt must fail."""
    assert location_retention_db["guard_returncode"] != 0, (
        "A dim_location member was deleted while fact rows still referenced it, "
        "and `dbt test` still passed. The relationships test on "
        "fct_service_requests.location_id is missing or not selecting these rows "
        "— referential decay would go unnoticed again.\n"
        + location_retention_db["guard_output"][-4000:]
    )
    assert "relationships_fct_service_requests_location_id" in location_retention_db["guard_output"], (
        "dbt failed, but not in the relationships test on location_id — the "
        "guard is proving something other than what it claims."
    )
