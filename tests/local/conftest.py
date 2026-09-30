"""
Fixtures for the local behavioral tests: the real local/ dbt project built
against seeded DuckDB databases, and a runner for the real Silver stage.

Skips wholesale when duckdb or the dbt-duckdb adapter is not installed.
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

# local/dbt_exec.py resolves the dbt executable (`python -m dbt` does not
# work). local/ is not a package, hence the sys.path insert.
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
    """One silver row. Address, complaint and location default to one shared
    value so fct_complaint_recurrence and dim_location have rows to test (NULL
    addresses would build an empty table that passes anything). The location
    columns are overridable so the retention fixture can use a second one."""
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


def _seed_silver(con):
    """The three Silver tables the local dbt project reads, plus a DQ log row
    dated today so assert_dq_log_is_current passes."""
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
    """A DuckDB profile for local/ pointing at db_path. The file stem is the
    DuckDB catalog name, which sources.yml expects to be nyc311_local."""
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


@pytest.fixture(scope="session")
def run_stage3():
    """Return a function that runs the real local_runner.stage3_silver() against
    temp paths. local_runner reads its paths from module constants, so they are
    rebound for the call and restored afterwards; the developer's own
    local/data/ database is never touched."""
    def _run(workdir, raw_file, db_path, source_count_file):
        import local_runner

        names = ("RAW_FILE", "DUCKDB_PATH", "DATA_DIR", "RAW_DIR", "SOURCE_COUNT_FILE")
        saved = {n: getattr(local_runner, n) for n in names}
        local_runner.RAW_FILE = raw_file
        local_runner.DUCKDB_PATH = db_path
        local_runner.DATA_DIR = workdir
        local_runner.RAW_DIR = workdir
        local_runner.SOURCE_COUNT_FILE = source_count_file
        try:
            local_runner.stage3_silver()
        finally:
            for n, v in saved.items():
                setattr(local_runner, n, v)
    return _run


@pytest.fixture(scope="module")
def gold_db(tmp_path_factory):
    """Seed silver → dbt build → mutate silver → dbt build (incremental) →
    dbt build --full-refresh. Returns a dict of captured result sets."""
    workdir = tmp_path_factory.mktemp("localdbt")
    db_path = workdir / "nyc311_local.duckdb"
    _write_profile(workdir, db_path)

    con = duckdb.connect(str(db_path))
    _seed_silver(con)  # quarantine stays empty until phase 2 rejects r9

    # ── Phase 1: r6 and r9 are valid now and become invalid in phase 2 ───────
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
        # r8 at 23:50 makes Jan 4 the only complete day (within the 60-minute
        # tail), so the recurrence horizon is Jan 4 and NOT the newest loaded
        # day (TODAY, incomplete).
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
    # r9 is QUARANTINED BY SILVER: unlike r6 it leaves silver.service_requests
    # completely, so only the quarantine post_hook can delete its fact row.
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
    # Each must fail on the thing it guards, in both directions, then recover.
    # The sabotage edits the built table, not the model file, so the repo is
    # untouched if the run dies midway.
    horizon_tests = ["assert_recurrence_horizon_is_last_complete_day",
                     "assert_observation_days_floor_is_explained"]

    def run_horizon_tests():
        return _dbt(["test", "--select", *horizon_tests], workdir, check=False)

    guards = {"clean": run_horizon_tests()}

    # Horizon one day too far forward. It floors nothing, so only the horizon
    # test can catch it.
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

    # Rebuilding the model must restore both to green, so the failures above
    # are attributable to the sabotage.
    _dbt(["build", "--select", "fct_complaint_recurrence"], workdir)
    guards["reverted"] = run_horizon_tests()

    return {
        "phase1_fct_keys": phase1_fct_keys,
        "incremental": incremental,
        "full_refresh": full_refresh,
        "guards": {k: {"returncode": v.returncode, "output": v.stdout}
                   for k, v in guards.items()},
    }


# A second location for location_retention_db: present in phase 1, then out
# of Silver's window in phase 2 without being quarantined.
QUEENS = {"borough": "QUEENS", "community_board": "04 QUEENS", "incident_zip": "11373"}


@pytest.fixture(scope="module")
def location_retention_db(tmp_path_factory):
    """Referential integrity of fct_service_requests.location_id across a
    moving Silver window.

    Phase 1 puts rows at two locations. Phase 2 removes one location from
    Silver the way the window does (deleted, not quarantined, so the
    reconciliation post_hooks leave its fact rows alone); its dim_location
    member must survive. The last step drops a member by hand and records that
    `dbt test` then fails, so the relationships test is proven live.
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
        # 23:50 makes Jan 3 complete, giving fct_complaint_recurrence a
        # horizon. It is k2 because k2 survives phase 2; without a complete
        # day, observation_days is NULL and the build fails.
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
    # Deleted, not quarantined: the rows just left the window, so their fact
    # rows and dimension member must stay. A fresh Brooklyn row keeps the
    # incremental run non-empty.
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

    # ── Non-vacuity guard: drop a dimension member and confirm dbt notices ───
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
