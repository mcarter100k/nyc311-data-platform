"""
Fixtures for the local-gold behavioral tests.

Builds the real dbt project in local/ against a seeded DuckDB database, twice,
so the tests can assert incremental semantics that structural tests cannot:
the _loaded_at watermark with its 1-hour lookback, snapshot rename detection,
and the SCD2 point-in-time agency join.

Skips wholesale when duckdb or the dbt-duckdb adapter is not installed
(mirrors the pyspark importorskip pattern in tests/unit/conftest.py).
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


def _row(unique_key, created, closed, agency, agency_name, status, ts):
    return (
        f"('{unique_key}', TIMESTAMP '{created}', "
        + (f"TIMESTAMP '{closed}'" if closed else "NULL")
        + f", NULL, '{agency}', '{agency_name}', 'Noise - Residential', NULL, NULL, "
        f"'11201', NULL, NULL, NULL, 'BROOKLYN', '02 BROOKLYN', 40.69, -73.99, "
        f"'{status}', NULL, 'PHONE', TIMESTAMP '{ts}')"
    )


def _dbt_executable():
    # dbt-core 1.7 ships no __main__, so `python -m dbt` does not work; use the
    # console script installed next to the current interpreter, falling back to
    # whatever is on PATH.
    import shutil
    candidate = os.path.join(os.path.dirname(sys.executable), "dbt")
    return candidate if os.path.exists(candidate) else shutil.which("dbt")


def _dbt(args, profiles_dir):
    cmd = [
        _dbt_executable(), *args,
        "--profiles-dir", str(profiles_dir),
        "--project-dir", LOCAL_PROJECT,
        "--no-version-check",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=LOCAL_PROJECT)
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
    con.execute("""
        CREATE TABLE silver.data_quality_log (
            run_date VARCHAR, check_name VARCHAR, records_checked BIGINT,
            records_failed BIGINT, failure_rate DOUBLE, pipeline_stage VARCHAR)
    """)
    con.execute("""
        INSERT INTO silver.data_quality_log
        VALUES ('2024-01-03', 'null_rate_unique_key', 100, 0, 0.0, 'silver')
    """)

    # ── Phase 1: two requests, one agency name each ──────────────────────────
    con.execute("INSERT INTO silver.service_requests VALUES " + ",".join([
        _row("r1", "2024-01-02 10:00:00", "2024-01-03 01:00:00",
             "HPD", "Housing Preservation And Development", "Closed", T1),
        _row("r2", "2024-01-03 09:00:00", None,
             "NYPD", "New York City Police Dept", "Open", T1),
    ]))
    con.close()

    _dbt(["deps"], workdir)
    _dbt(["build"], workdir)          # snapshot v1 + full first build

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

    return {"incremental": incremental, "full_refresh": full_refresh}
