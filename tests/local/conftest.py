"""
Fixtures for the local-gold behavioral tests.

Builds the real dbt project in local/ against a seeded DuckDB database, twice,
so the tests can assert incremental semantics that structural tests cannot:
the _loaded_at watermark with its 1-hour lookback, snapshot rename detection,
and the SCD2 point-in-time agency join.

Skips wholesale when duckdb or the dbt-duckdb adapter is not installed
(mirrors the importorskip pattern used by the unit tier).
"""

import os
import subprocess
import sys
from datetime import datetime, timezone

import pytest

duckdb = pytest.importorskip("duckdb", reason="duckdb not installed — skipping local gold tests")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb not installed — skipping local gold tests")

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOCAL_PROJECT = os.path.join(ROOT, "local")

# `python -m dbt` does not work; local/dbt_exec.py holds the single definition
# of how the dbt console script is resolved, shared with local/local_runner.py
# and run_tests.sh. local/ is not a package, hence the sys.path idiom used by
# test_module_imports.py and test_live_fetch.py.
if LOCAL_PROJECT not in sys.path:
    sys.path.insert(0, LOCAL_PROJECT)
from dbt_exec import dbt_executable  # noqa: E402

# Silver contract columns consumed by local/models/staging/stg_service_requests.sql
SILVER_COLUMNS = """
    unique_key VARCHAR, created_date TIMESTAMP, closed_date TIMESTAMP,
    resolution_action_updated_date TIMESTAMP, agency VARCHAR, agency_name VARCHAR,
    complaint_type VARCHAR, descriptor VARCHAR, location_type VARCHAR,
    incident_zip VARCHAR, incident_address VARCHAR, street_name VARCHAR,
    city VARCHAR, borough VARCHAR, community_board VARCHAR,
    latitude DOUBLE, longitude DOUBLE, status VARCHAR,
    resolution_description VARCHAR, open_data_channel_type VARCHAR,
    _silver_timestamp TIMESTAMP
"""

# Timeline: T0 predates the phase-1 watermark by more than the 1-hour lookback;
# T1 is the phase-1 load; T2 is the phase-2 load.
T0 = "2024-01-01 00:00:00"
T1 = "2024-01-03 06:00:00"
T2 = "2024-01-04 05:30:00"

TODAY_UTC = datetime.now(timezone.utc).date().isoformat()


def _row(unique_key, created, closed, agency, agency_name, status, ts,
         address="100 MAIN STREET", complaint="Noise - Residential",
         borough="BROOKLYN", community_board="02 BROOKLYN", incident_zip="11201"):
    """One silver row. address and complaint default to a shared pair so most
    rows land at the same location — fct_complaint_recurrence keys on
    (address, complaint_type), and a fixture with NULL addresses would build an
    empty table and silently pass any test written against it.

    borough / community_board / incident_zip are the dim_location grain. They
    default to a single shared combination for the same reason, and are
    overridable so the retention fixture below can put a row at a SECOND
    location and then take that location out of the window."""
    return (
        f"('{unique_key}', TIMESTAMP '{created}', "
        + (f"TIMESTAMP '{closed}'" if closed else "NULL")
        + f", NULL, '{agency}', '{agency_name}', '{complaint}', NULL, NULL, "
        f"'{incident_zip}', '{address}', NULL, NULL, '{borough}', "
        f"'{community_board}', 40.69, -73.99, "
        f"'{status}', NULL, 'PHONE', TIMESTAMP '{ts}')"
    )


def _dbt(args, profiles_dir, check=True):
    """Run dbt against the real local/ project. check=False returns the result
    for the caller to inspect instead of asserting success — used where a
    NON-zero exit is the thing under test."""
    cmd = [
        dbt_executable(), *args,
        "--profiles-dir", str(profiles_dir),
        "--project-dir", LOCAL_PROJECT,
        "--no-version-check",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=LOCAL_PROJECT,
                            check=False)
    if check:
        assert result.returncode == 0, (
            f"dbt {' '.join(args)} failed:\n{result.stdout[-4000:]}\n{result.stderr[-2000:]}"
        )
    return result


@pytest.fixture(scope="module")
def gold_db(tmp_path_factory):
    """Seed silver → dbt build → mutate silver → dbt build (incremental) →
    dbt build --full-refresh. Returns a dict of captured result sets."""
    workdir = tmp_path_factory.mktemp("localdbt")
    # The file stem is the DuckDB catalog name; sources.yml expects nyc311_local.
    db_path = workdir / "nyc311_local.duckdb"

    (workdir / "profiles.yml").write_text(
        "nyc311_local:\n"
        "  target: local\n"
        "  outputs:\n"
        "    local:\n"
        "      type: duckdb\n"
        f"      path: \"{db_path}\"\n"
        "      schema: gold\n"
        "      threads: 1\n"
    )

    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(f"CREATE TABLE silver.service_requests ({SILVER_COLUMNS})")
    # Empty in phase 1: r9 is a valid row that Silver rejects in phase 2, which
    # is the case the dbt quality filter CANNOT model — a Silver-quarantined row
    # never reaches staging at all.
    con.execute("""
        CREATE TABLE silver.quarantine (
            unique_key VARCHAR, created_date TIMESTAMP, closed_date TIMESTAMP,
            resolution_days BIGINT, quarantine_reason VARCHAR,
            _silver_timestamp VARCHAR)
    """)
    con.execute("""
        CREATE TABLE silver.data_quality_log (
            run_date VARCHAR, check_name VARCHAR, records_checked BIGINT,
            records_failed BIGINT, failure_rate DOUBLE, pipeline_stage VARCHAR)
    """)
    # Seeded at TODAY so the mirrored singular test assert_dq_log_is_current
    # (max(run_date) within a day) passes during every dbt build below.
    con.execute(f"""
        INSERT INTO silver.data_quality_log
        VALUES ('{TODAY_UTC}', 'null_rate_unique_key', 100, 0, 0.0, 'silver')
    """)

    # ── Phase 1: three requests — r6 is valid now, corrected-invalid later ────
    con.execute("INSERT INTO silver.service_requests VALUES " + ",".join([
        _row("r1", "2024-01-02 10:00:00", "2024-01-03 01:00:00",
             "HPD", "Housing Preservation And Development", "Closed", T1),
        _row("r2", "2024-01-03 09:00:00", None,
             "NYPD", "New York City Police Dept", "Open", T1),
        _row("r6", "2024-01-02 12:00:00", "2024-01-02 18:00:00",
             "HPD", "Housing Preservation And Development", "Closed", T1),
        # Recurrence pair: r7 closes on Jan 2; r8 reports the SAME complaint at
        # the SAME address on Jan 4. fct_complaint_recurrence must measure 2 days.
        #
        # r8's time of day is load-bearing, not decoration. int_load_completeness
        # judges a day COMPLETE by whether the source's coverage of it reaches
        # the last complete_day_tail_minutes (60) of that day, and the recurrence
        # horizon is the newest complete day. 23:50 makes Jan 4 the only complete
        # day in this fixture, which is what gives the timeline a horizon at all
        # — and it deliberately leaves Jan 1-3 and the TODAY row incomplete, so
        # the tests below are asserting against a horizon that is NOT simply the
        # newest loaded day. Under the old max(created_date) horizon every
        # observation_days here would be ~950 (TODAY minus Jan 2), not 1 and 2.
        _row("r7", "2024-01-01 09:00:00", "2024-01-02 09:00:00",
             "DSNY", "Department of Sanitation", "Closed", T1,
             address="9 RECURRING WAY", complaint="Dirty Condition"),
        _row("r8", "2024-01-04 23:50:00", None,
             "DSNY", "Department of Sanitation", "Open", T1,
             address="9 RECURRING WAY", complaint="Dirty Condition"),
        # r9 is valid now. In phase 2 SILVER rejects it, so it vanishes from
        # silver.service_requests entirely rather than merely failing the dbt
        # filter the way r6 does.
        _row("r9", "2024-01-02 07:00:00", "2024-01-02 19:00:00",
             "DOT", "Department of Transportation", "Closed", T1),
    ]))
    con.close()

    _dbt(["deps"], workdir)
    _dbt(["build"], workdir)          # snapshot v1 + full first build

    # Capture the phase-1 fact keys so tests can prove r6 existed before its
    # correction — and was therefore DELETED by the reconciliation post_hook,
    # not merely never built.
    con = duckdb.connect(str(db_path))
    phase1_fct_keys = {
        r[0] for r in con.execute(
            "SELECT unique_key FROM gold.fct_service_requests").fetchall()
    }
    con.close()

    # ── Phase 2 mutations ────────────────────────────────────────────────────
    con = duckdb.connect(str(db_path))
    con.execute("INSERT INTO silver.service_requests VALUES " + ",".join([
        # Late arriver: old business date, fresh pipeline timestamp.
        _row("r3", "2020-06-15 08:00:00", "2020-06-20 08:00:00",
             "HPD", "Housing Preservation And Development", "Closed", T2),
        # Beyond the lookback: pipeline timestamp older than watermark - 1h.
        _row("r4", "2024-01-01 08:00:00", None,
             "HPD", "Housing Preservation And Development", "Open", T0),
        # Agency renamed; created today so it falls in the new version's window.
        _row("r5", f"{TODAY_UTC} 12:00:00", None,
             "NYPD", "New York City Police Department", "Open", T2),
    ]))
    # r2 closes between the two runs — same natural key, fresh timestamp.
    con.execute(f"""
        UPDATE silver.service_requests
        SET status = 'Closed',
            closed_date = TIMESTAMP '2024-01-04 04:00:00',
            _silver_timestamp = TIMESTAMP '{T2}'
        WHERE unique_key = 'r2'
    """)
    # r6 is CORRECTED so it now fails the quality filter (closed before
    # created): it disappears from int_service_requests_cleaned, and the
    # reconciliation post_hook must delete its stale fact row.
    con.execute(f"""
        UPDATE silver.service_requests
        SET closed_date = TIMESTAMP '2024-01-01 06:00:00',
            _silver_timestamp = TIMESTAMP '{T2}'
        WHERE unique_key = 'r6'
    """)
    # r9 is QUARANTINED BY SILVER: the pandas transform drops it before dbt sees
    # anything, so unlike r6 it leaves silver.service_requests completely. The
    # original post_hook (present in staging, absent from int) is structurally
    # blind to this, which is why the quarantine table and the second post_hook
    # exist. Its stale fact row must still disappear.
    con.execute("""
        INSERT INTO silver.quarantine
        SELECT unique_key, created_date, closed_date, -1,
               'negative_resolution_days', CAST(_silver_timestamp AS VARCHAR)
        FROM silver.service_requests WHERE unique_key = 'r9'
    """)
    con.execute("DELETE FROM silver.service_requests WHERE unique_key = 'r9'")
    con.close()

    _dbt(["build"], workdir)          # snapshot v2 + incremental merge

    def capture(con):
        return {
            "fct": {
                r[0]: {"agency_id": r[1], "status": r[2], "is_resolved": r[3]}
                for r in con.execute("""
                    SELECT unique_key, agency_id, status, is_resolved
                    FROM gold.fct_service_requests
                """).fetchall()
            },
            "fct_rowcount": con.execute(
                "SELECT COUNT(*) FROM gold.fct_service_requests").fetchone()[0],
            "dim_agency": con.execute("""
                SELECT agency_abbreviation, agency_name, agency_key,
                       valid_from, expiry_date, is_current
                FROM gold.dim_agency ORDER BY agency_abbreviation, valid_from
            """).fetchall(),
            "recurrence": [
                {"unique_key": r[0], "closure_type": r[1],
                 "days_to_next": r[2], "observation_days": r[3]}
                for r in con.execute("""
                    SELECT unique_key, closure_type,
                           days_to_next_same_complaint, observation_days
                    FROM gold.fct_complaint_recurrence
                """).fetchall()
            ],
            "completeness": [
                {"load_day": str(r[0]), "requests_created": r[1],
                 "minutes_short_of_midnight": r[2], "is_complete_day": r[3]}
                for r in con.execute("""
                    SELECT load_day, requests_created,
                           minutes_short_of_midnight, is_complete_day
                    FROM gold.int_load_completeness ORDER BY load_day
                """).fetchall()
            ],
            "silver_count": con.execute(
                "SELECT COUNT(*) FROM silver.service_requests").fetchone()[0],
        }

    con = duckdb.connect(str(db_path))
    incremental = capture(con)
    con.close()

    _dbt(["build", "--full-refresh"], workdir)

    con = duckdb.connect(str(db_path))
    full_refresh = capture(con)
    con.close()

    # ── Non-vacuity guards for the two horizon tests ─────────────────────────
    # The tests they replace could not fail. `observation_days >= 0` sat on a
    # column produced by GREATEST(0, ...), so sabotaging the horizon to
    # DATE '1999-01-01' drove every raw value thousands of days negative and the
    # test still reported PASS. Anything written to replace that has to be shown
    # failing on the thing it guards, in both directions, and then recovering.
    #
    # The sabotage is applied to the BUILT TABLE rather than to the model file:
    # it isolates what the tests can detect from how the model happens to be
    # written today, and it leaves the repo untouched if the run dies midway.
    horizon_tests = ["assert_recurrence_horizon_is_last_complete_day",
                     "assert_observation_days_floor_is_explained"]

    def run_horizon_tests():
        return _dbt(["test", "--select", *horizon_tests], workdir, check=False)

    guards = {"clean": run_horizon_tests()}

    # Horizon one day too far forward — literally the defect: the newest loaded
    # day is partial, and treating it as the horizon over-credits every row.
    # Chosen because it floors NOTHING, so only the horizon test can catch it.
    con = duckdb.connect(str(db_path))
    con.execute("UPDATE gold.fct_complaint_recurrence "
                "SET observation_days = observation_days + 1")
    con.close()
    guards["horizon_advanced"] = run_horizon_tests()

    # Horizon frozen or backdated: every row floors, the sample silently
    # shrinks out of every `observation_days >= N` filter, nothing errors.
    con = duckdb.connect(str(db_path))
    con.execute("UPDATE gold.fct_complaint_recurrence SET observation_days = 0")
    con.close()
    guards["all_floored"] = run_horizon_tests()

    # And back: rebuilding the model from source must restore both to green,
    # so the failures above are attributable to the sabotage and not to drift
    # accumulated by this fixture.
    _dbt(["build", "--select", "fct_complaint_recurrence"], workdir)
    guards["reverted"] = run_horizon_tests()

    return {
        "phase1_fct_keys": phase1_fct_keys,
        "incremental": incremental,
        "full_refresh": full_refresh,
        "guards": {k: {"returncode": v.returncode, "output": v.stdout}
                   for k, v in guards.items()},
    }


# The second location, used only by location_retention_db below. It exists in
# phase 1 and leaves Silver's window in phase 2 WITHOUT being quarantined —
# which is what a rolling window does every single day, and the one case the
# gold_db fixture above cannot express (every row there shares one location).
QUEENS = {"borough": "QUEENS", "community_board": "04 QUEENS", "incident_zip": "11373"}


def _seed_silver(con):
    """The three Silver tables the local dbt project reads, plus a current DQ
    log row so the mirrored singular test assert_dq_log_is_current passes."""
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute(f"CREATE TABLE silver.service_requests ({SILVER_COLUMNS})")
    con.execute("""
        CREATE TABLE silver.quarantine (
            unique_key VARCHAR, created_date TIMESTAMP, closed_date TIMESTAMP,
            resolution_days BIGINT, quarantine_reason VARCHAR,
            _silver_timestamp VARCHAR)
    """)
    con.execute("""
        CREATE TABLE silver.data_quality_log (
            run_date VARCHAR, check_name VARCHAR, records_checked BIGINT,
            records_failed BIGINT, failure_rate DOUBLE, pipeline_stage VARCHAR)
    """)
    con.execute(f"""
        INSERT INTO silver.data_quality_log
        VALUES ('{TODAY_UTC}', 'null_rate_unique_key', 100, 0, 0.0, 'silver')
    """)


def _write_profile(workdir, db_path):
    (workdir / "profiles.yml").write_text(
        "nyc311_local:\n"
        "  target: local\n"
        "  outputs:\n"
        "    local:\n"
        "      type: duckdb\n"
        f"      path: \"{db_path}\"\n"
        "      schema: gold\n"
        "      threads: 1\n"
    )


@pytest.fixture(scope="module")
def location_retention_db(tmp_path_factory):
    """Referential integrity of fct_service_requests.location_id across a
    MOVING Silver window — the condition that produced silent decay in
    production and that no other fixture reaches.

    fct_service_requests is incremental and accumulates history. dim_location
    was `materialized: table`, rebuilt every run from
    int_service_requests_cleaned, which carries only Silver's rolling window.
    So a location whose rows aged out of the window was dropped from the
    dimension while fact rows kept pointing at it.

    Phase 1 puts rows at two locations. Phase 2 removes one of those locations
    from Silver the way the window does — deleted, NOT quarantined, so the
    reconciliation post_hooks correctly leave its fact rows alone. Its
    dimension member must survive.

    The last step is the non-vacuity guard: it drops a dim_location member by
    hand and records that `dbt test` then FAILS. Without it, deleting the
    relationships test from marts.yml would leave every assertion here green.
    """
    workdir = tmp_path_factory.mktemp("locretention")
    db_path = workdir / "nyc311_local.duckdb"
    _write_profile(workdir, db_path)

    con = duckdb.connect(str(db_path))
    _seed_silver(con)
    # Phase 1: two requests in Brooklyn, two in Queens.
    con.execute("INSERT INTO silver.service_requests VALUES " + ",".join([
        _row("k1", "2024-01-02 10:00:00", "2024-01-03 01:00:00",
             "HPD", "Housing Preservation And Development", "Closed", T1),
        # 23:50 makes Jan 3 a COMPLETE day for int_load_completeness, which is
        # what gives fct_complaint_recurrence a horizon here. k2 is the row that
        # carries it because k2 survives phase 2 — the Queens rows do not, and a
        # fixture whose only complete day is deleted mid-run has no horizon in
        # its second half and fails on a null observation_days.
        _row("k2", "2024-01-03 23:50:00", None,
             "NYPD", "New York City Police Dept", "Open", T1),
        _row("q1", "2024-01-02 11:00:00", "2024-01-03 02:00:00",
             "DSNY", "Department of Sanitation", "Closed", T1,
             address="7 QUEENS BOULEVARD", **QUEENS),
        _row("q2", "2024-01-02 15:00:00", None,
             "DSNY", "Department of Sanitation", "Open", T1,
             address="7 QUEENS BOULEVARD", **QUEENS),
    ]))
    con.close()

    _dbt(["deps"], workdir)
    _dbt(["build"], workdir)

    def snapshot(con):
        return {
            "fct_keys": {r[0] for r in con.execute(
                "SELECT unique_key FROM gold.fct_service_requests").fetchall()},
            "fct_rowcount": con.execute(
                "SELECT COUNT(*) FROM gold.fct_service_requests").fetchone()[0],
            "boroughs": {r[0] for r in con.execute(
                "SELECT borough FROM gold.dim_location").fetchall()},
            "dim_rowcount": con.execute(
                "SELECT COUNT(*) FROM gold.dim_location").fetchone()[0],
            "orphans": con.execute("""
                SELECT COUNT(*) FROM gold.fct_service_requests f
                WHERE f.location_id IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM gold.dim_location d
                                  WHERE d.location_id = f.location_id)
            """).fetchone()[0],
            "recurrence_rowcount": con.execute(
                "SELECT COUNT(*) FROM gold.fct_complaint_recurrence").fetchone()[0],
        }

    con = duckdb.connect(str(db_path))
    phase1 = snapshot(con)
    con.close()

    # ── Phase 2: the window moves past Queens ────────────────────────────────
    # Deleted, not quarantined: these rows are simply no longer inside the
    # trailing window Silver reloads. Nothing in the pipeline rejected them, so
    # their accumulated fact rows must stay — and their dimension member with
    # them. A fresh Brooklyn row keeps the incremental run non-empty, exactly
    # as a real daily run would.
    con = duckdb.connect(str(db_path))
    con.execute("INSERT INTO silver.service_requests VALUES " + ",".join([
        _row("k3", f"{TODAY_UTC} 09:00:00", None,
             "NYPD", "New York City Police Dept", "Open", T2),
    ]))
    con.execute("DELETE FROM silver.service_requests WHERE unique_key IN ('q1', 'q2')")
    con.close()

    build = _dbt(["build"], workdir, check=False)

    con = duckdb.connect(str(db_path))
    phase2 = snapshot(con)
    con.close()

    # ── Non-vacuity guard ────────────────────────────────────────────────────
    # Drop a dimension member by hand — precisely what the old table rebuild
    # did on its own every run — and confirm dbt notices.
    con = duckdb.connect(str(db_path))
    con.execute("""
        DELETE FROM gold.dim_location
        WHERE location_id IN (SELECT location_id FROM gold.fct_service_requests
                              WHERE location_id IS NOT NULL LIMIT 1)
    """)
    con.close()
    guard = _dbt(["test", "--select", "fct_service_requests,test_name:relationships"],
                 workdir, check=False)

    return {
        "phase1": phase1,
        "phase2": phase2,
        "phase2_build_returncode": build.returncode,
        "phase2_build_output": build.stdout,
        "guard_returncode": guard.returncode,
        "guard_output": guard.stdout,
    }
