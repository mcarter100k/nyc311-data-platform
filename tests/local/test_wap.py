"""
Write-audit-publish on the local DuckDB path (ADR 018), against the real local/
dbt project.

One module-scoped scenario on a fresh database:
  A. first build: audited in gold_audit, then published
  B. a failing dbt test (a NULL agency in Silver): nothing may reach Gold
  C. the window moves and an agency is renamed: incremental history and SCD2
     versions must survive the publish
"""

import shutil

import duckdb
import pytest

from conftest import T1, T2, TODAY_UTC, _dbt, _row, _seed_silver, _write_profile

import wap

STATE_SCHEMAS = ("gold", "snapshots")
AUDIT_SCHEMAS = ("gold_audit", "snapshots_audit")


def _fingerprint(db_path, schemas):
    """{schema-free table name: (rows, sum of row hashes)} for every table."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        out = {}
        for schema in schemas:
            for (table,) in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name = ? ORDER BY 1", [schema]
            ).fetchall():
                out[f"{schema.replace('_audit', '')}.{table}"] = con.execute(
                    f"SELECT count(*), sum(hash(t)) FROM {schema}.{table} t").fetchone()
        return out
    finally:
        con.close()


def _schemas(db_path):
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return {r[0] for r in con.execute("SELECT schema_name FROM duckdb_schemas()").fetchall()}
    finally:
        con.close()


def _query(db_path, sql):
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


@pytest.fixture(scope="module")
def wap_run(tmp_path_factory):
    workdir = tmp_path_factory.mktemp("wap")
    db = workdir / "nyc311_local.duckdb"
    _write_profile(workdir, db)
    con = duckdb.connect(str(db))
    _seed_silver(con)
    con.execute("INSERT INTO silver.service_requests VALUES " + ",".join([
        _row("r1", "2024-01-02 10:00:00", "2024-01-03 01:00:00",
             "HPD", "Housing Preservation And Development", "Closed", T1),
        _row("r2", "2024-01-03 09:00:00", None, "NYPD", "New York City Police Dept", "Open", T1),
        # r8 at 23:50 makes Jan 4 a complete day, which the recurrence tests need.
        _row("r7", "2024-01-01 09:00:00", "2024-01-02 09:00:00", "DSNY", "Department of Sanitation",
             "Closed", T1, address="9 RECURRING WAY", complaint="Dirty Condition"),
        _row("r8", "2024-01-04 23:50:00", None, "DSNY", "Department of Sanitation",
             "Open", T1, address="9 RECURRING WAY", complaint="Dirty Condition"),
    ]))
    con.close()
    _dbt(["deps"], workdir)

    def run_dbt(args):
        return _dbt(args, workdir, check=False).returncode

    out = {"db": db}

    # A. Build and test in audit, fingerprint it, then publish.
    wap.prepare_audit(db)
    out["a_build_rc"] = run_dbt(["build", "--vars", wap.DBT_VARS, "--full-refresh"])
    out["a_audit"] = _fingerprint(db, AUDIT_SCHEMAS)
    wap.publish(db)
    out["a_gold"] = _fingerprint(db, STATE_SCHEMAS)
    out["a_schemas"] = _schemas(db)

    # B. A NULL agency fails the not_null source test.
    con = duckdb.connect(str(db))
    con.execute("INSERT INTO silver.service_requests VALUES "
                + _row("bad", "2024-01-03 11:00:00", None, "NYPD", "x", "Open", T2).replace("'NYPD'", "NULL"))
    con.close()
    out["b_rc"] = wap.run(db, run_dbt)
    out["b_gold"] = _fingerprint(db, STATE_SCHEMAS)
    out["b_schemas"] = _schemas(db)
    con = duckdb.connect(str(db))
    con.execute("DELETE FROM silver.service_requests WHERE unique_key = 'bad'")
    con.close()

    # C. r1 and r7 leave the window; r5 is new; NYPD is renamed (a new SCD2 version).
    con = duckdb.connect(str(db))
    con.execute("DELETE FROM silver.service_requests WHERE unique_key IN ('r1', 'r7')")
    con.execute("INSERT INTO silver.service_requests VALUES " + _row(
        "r5", f"{TODAY_UTC} 12:00:00", None, "NYPD", "New York City Police Department", "Open", T2))
    con.close()
    out["c_rc"] = wap.run(db, run_dbt)
    out["c_gold"] = _fingerprint(db, STATE_SCHEMAS)
    out["c_schemas"] = _schemas(db)
    out["c_fact_keys"] = {r[0] for r in _query(db, "SELECT unique_key FROM gold.fct_service_requests")}
    out["c_nypd_versions"] = _query(
        db, "SELECT agency_name, dbt_valid_to IS NULL FROM snapshots.agency_snapshot "
            "WHERE agency_abbreviation = 'NYPD' ORDER BY dbt_valid_from")
    return out


def test_a_passing_build_publishes_exactly_the_audited_build(wap_run):
    assert wap_run["a_build_rc"] == 0
    assert wap_run["a_gold"] == wap_run["a_audit"], "Gold differs from the build that passed its tests"
    assert "gold.fct_service_requests" in wap_run["a_gold"]
    assert "snapshots.agency_snapshot" in wap_run["a_gold"]
    assert not set(AUDIT_SCHEMAS) & wap_run["a_schemas"], "audit schemas were not dropped after publish"


def test_a_failing_test_leaves_gold_exactly_as_it_was(wap_run):
    assert wap_run["b_rc"] != 0, "the NULL agency should fail the build"
    assert wap_run["b_gold"] == wap_run["a_gold"], "a failed build changed Gold"
    assert set(AUDIT_SCHEMAS) <= wap_run["b_schemas"], "the failed build's audit schemas should be kept"


def test_incremental_and_snapshot_history_survive_a_publish(wap_run):
    assert wap_run["c_rc"] == 0
    keys = wap_run["c_fact_keys"]
    assert {"r1", "r7"} <= keys, "rows that left Silver's window were lost from the incremental fact"
    assert "r5" in keys
    rows_a = wap_run["a_gold"]["gold.fct_service_requests"][0]
    assert wap_run["c_gold"]["gold.fct_service_requests"][0] == rows_a + 1
    assert wap_run["c_nypd_versions"] == [("New York City Police Dept", False),
                                          ("New York City Police Department", True)], (
        "the SCD2 snapshot lost its history or did not version the rename")


def test_a_publish_that_fails_midway_rolls_back(wap_run, tmp_path):
    """Tables are copied before views, so a bad view fails the transaction after
    every Gold table was already replaced inside it."""
    db = tmp_path / "nyc311_local.duckdb"
    shutil.copy(wap_run["db"], db)
    before = _fingerprint(db, STATE_SCHEMAS)
    wap.prepare_audit(db)
    con = duckdb.connect(str(db))
    con.execute("DELETE FROM gold_audit.fct_service_requests")  # would empty Gold if it leaked
    con.execute("CREATE VIEW gold_audit.reads_audit AS SELECT * FROM gold_audit.dim_date")
    con.close()
    with pytest.raises(RuntimeError, match="cannot publish view"):
        wap.publish(db)
    assert _fingerprint(db, STATE_SCHEMAS) == before, "a failed publish left part of the new build in Gold"
    assert set(AUDIT_SCHEMAS) <= _schemas(db)
