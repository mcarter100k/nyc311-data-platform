"""Write-audit-publish for the local Gold layer (ADR 018).

dbt builds and tests into audit schemas (gold_audit, snapshots_audit) instead
of Gold. Only a build whose every test passed is published, in one DuckDB
transaction, so Gold is either the old build or the new one and never a mix.

DuckDB cannot rename a schema or move a table between schemas ("not yet
supported" in 1.5), so publishing copies each audited table and view into Gold
inside the transaction. A failed build leaves Gold untouched and keeps the
audit schemas for debugging; the next run drops them before it starts.
"""

import re

import duckdb

AUDIT_SUFFIX = "_audit"
DBT_VARS = '{"audit_suffix": "' + AUDIT_SUFFIX + '"}'

# The schemas dbt writes. Bronze and Silver are written by local_runner, not dbt.
SCHEMAS = ("gold", "snapshots")

_VIEW_BODY = re.compile(r"^\s*CREATE\s+VIEW\s+\S+\s+AS\s+", re.IGNORECASE)


def _tables(con, schema):
    return [r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE schema_name = ? ORDER BY 1", [schema]
    ).fetchall()]


def _views(con, schema):
    return con.execute(
        "SELECT view_name, sql FROM duckdb_views() WHERE schema_name = ? AND NOT internal ORDER BY 1",
        [schema],
    ).fetchall()


def prepare_audit(db_path) -> None:
    """Drop leftover audit schemas, then copy every Gold and snapshot table into
    them, so incremental models and the SCD2 snapshot build on real history."""
    con = duckdb.connect(str(db_path))
    try:
        for schema in SCHEMAS:
            audit = schema + AUDIT_SUFFIX
            con.execute(f"DROP SCHEMA IF EXISTS {audit} CASCADE")
            con.execute(f"CREATE SCHEMA {audit}")
            for table in _tables(con, schema):
                con.execute(f"CREATE TABLE {audit}.{table} AS SELECT * FROM {schema}.{table}")
    finally:
        con.close()


def publish(db_path) -> None:
    """Copy every audited table and view into Gold in one transaction, then drop
    the audit schemas. Any error rolls back, leaving Gold as it was."""
    con = duckdb.connect(str(db_path))
    try:
        con.execute("BEGIN TRANSACTION")
        try:
            for schema in SCHEMAS:
                audit = schema + AUDIT_SUFFIX
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                for table in _tables(con, audit):
                    con.execute(f"CREATE OR REPLACE TABLE {schema}.{table} AS SELECT * FROM {audit}.{table}")
                for view, sql in _views(con, audit):
                    body = _VIEW_BODY.sub("", sql, count=1)
                    # The staging views read only Silver. A view that reads the
                    # audit schema would break once that schema is dropped.
                    if body == sql or AUDIT_SUFFIX in body:
                        raise RuntimeError(f"cannot publish view {audit}.{view}: {sql[:200]}")
                    con.execute(f"CREATE OR REPLACE VIEW {schema}.{view} AS {body}")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        for schema in SCHEMAS:
            con.execute(f"DROP SCHEMA IF EXISTS {schema + AUDIT_SUFFIX} CASCADE")
    finally:
        con.close()


def run(db_path, run_dbt, full_refresh: bool = False) -> int:
    """Write, audit, publish. run_dbt(args) -> exit code runs dbt against db_path.
    Returns dbt's exit code; Gold changes only when it is 0."""
    prepare_audit(db_path)
    args = ["build", "--vars", DBT_VARS] + (["--full-refresh"] if full_refresh else [])
    rc = run_dbt(args)
    if rc == 0:
        publish(db_path)
    return rc
