#!/usr/bin/env python3
"""
Local NYC 311 pipeline runner.

Pulls real data from the NYC Open Data Socrata API and runs the full
Bronze → Silver → Gold transformation pipeline on-laptop using DuckDB.
No cloud credentials, no Databricks, no Snowflake required.

Usage:
    python local_runner.py                  # all 5 stages, 10,000 rows
    python local_runner.py --rows 50000     # larger dataset
    python local_runner.py --stage 3        # resume from stage 3 forward
    python local_runner.py --stage 5        # just reprint results
"""

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import requests

LOCAL_DIR   = Path(__file__).parent.resolve()
DATA_DIR    = LOCAL_DIR / "data"
RAW_DIR     = DATA_DIR / "raw"
DUCKDB_PATH = DATA_DIR / "nyc311_local.duckdb"
RAW_FILE    = RAW_DIR / "nyc311_raw.json"

SOCRATA_ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE        = 1_000

BOROUGH_MAP = {
    "BROOKLYN": "BROOKLYN", "BKLYN": "BROOKLYN", "BK": "BROOKLYN",
    "KINGS": "BROOKLYN", "KINGS COUNTY": "BROOKLYN",
    "MANHATTAN": "MANHATTAN", "MN": "MANHATTAN", "NEW YORK": "MANHATTAN",
    "NEW YORK CITY": "MANHATTAN", "NYC": "MANHATTAN", "NY": "MANHATTAN",
    "QUEENS": "QUEENS", "QN": "QUEENS", "QNS": "QUEENS",
    "QUEENS COUNTY": "QUEENS",
    "BRONX": "BRONX", "THE BRONX": "BRONX", "BX": "BRONX",
    "BRONX COUNTY": "BRONX",
    "STATEN ISLAND": "STATEN ISLAND", "SI": "STATEN ISLAND",
    "S.I.": "STATEN ISLAND", "STATEN IS": "STATEN ISLAND",
    "RICHMOND": "STATEN ISLAND",
}


def _banner(msg: str) -> None:
    print(f"\n{'─' * 64}")
    print(f"  {msg}")
    print(f"{'─' * 64}")


# ── Stage 1: Ingest ────────────────────────────────────────────────────────────

def stage1_ingest(rows: int) -> None:
    _banner(f"Stage 1 — Ingest  ({rows:,} rows from Socrata API)")
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    records: list = []
    offset = 0
    while len(records) < rows:
        limit = min(PAGE_SIZE, rows - len(records))
        params = {"$limit": limit, "$offset": offset, "$order": ":id"}
        resp = requests.get(SOCRATA_ENDPOINT, params=params, timeout=30)
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        records.extend(page)
        offset += len(page)
        print(f"  fetched {len(records):,} / {rows:,} rows", end="\r", flush=True)

    print(f"\n  total fetched: {len(records):,} rows")
    RAW_FILE.write_text(json.dumps(records, indent=2))
    print(f"  written: {RAW_FILE.relative_to(LOCAL_DIR)}")


# ── Stage 2: Bronze ────────────────────────────────────────────────────────────

def stage2_bronze() -> None:
    _banner("Stage 2 — Bronze  (JSON → DuckDB bronze.service_requests)")
    if not RAW_FILE.exists():
        sys.exit(f"  ERROR: {RAW_FILE} not found — run stage 1 first")

    with open(RAW_FILE) as fh:
        raw = json.load(fh)

    df = pd.DataFrame(raw)
    now = datetime.now(timezone.utc).isoformat()
    df["_ingest_timestamp"] = now
    df["_source_file"]      = RAW_FILE.name
    print(f"  loaded {len(df):,} rows from {RAW_FILE.name}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE OR REPLACE TABLE bronze.service_requests AS SELECT * FROM df")
    n = con.execute("SELECT COUNT(*) FROM bronze.service_requests").fetchone()[0]
    con.close()
    print(f"  bronze.service_requests: {n:,} rows")


# ── Stage 3: Silver ────────────────────────────────────────────────────────────

def _standardize_borough(val) -> str:
    if pd.isna(val) or str(val).strip() == "":
        return "UNSPECIFIED"
    return BOROUGH_MAP.get(str(val).upper().strip(), "UNSPECIFIED")


def stage3_silver() -> None:
    _banner("Stage 3 — Silver  (clean, dedup, derive → DuckDB silver schema)")
    con = duckdb.connect(str(DUCKDB_PATH))

    df = con.execute("SELECT * FROM bronze.service_requests").df()
    n_bronze = len(df)
    print(f"  bronze rows: {n_bronze:,}")

    # Null rates on raw bronze data (pre-dedup)
    n_null_uk = df["unique_key"].isna().sum() if "unique_key" in df.columns else n_bronze
    n_null_cd = df["created_date"].isna().sum() if "created_date" in df.columns else 0

    # Deduplication: keep latest _ingest_timestamp per unique_key
    if "unique_key" in df.columns:
        df = (
            df.sort_values("_ingest_timestamp", ascending=False)
              .drop_duplicates(subset=["unique_key"], keep="first")
              .reset_index(drop=True)
        )
    n_deduped = len(df)
    print(f"  after dedup: {n_deduped:,} rows ({n_bronze - n_deduped:,} duplicates removed)")

    # Borough standardization (before timestamp parsing for unrecognized count)
    df["borough"] = df.get("borough", pd.Series(dtype=str)).apply(_standardize_borough)
    n_unrecognized = (df["borough"] == "UNSPECIFIED").sum()

    # Timestamp parsing — coerce bad values to NaT
    for col in ("created_date", "closed_date", "resolution_action_updated_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        else:
            df[col] = pd.NaT

    # resolution_days: integer, NULL for open requests
    has_both = df["closed_date"].notna() & df["created_date"].notna()
    df["resolution_days"] = pd.NA
    if has_both.any():
        df.loc[has_both, "resolution_days"] = (
            (df.loc[has_both, "closed_date"] - df.loc[has_both, "created_date"])
            .dt.days
            .astype("Int64")
        )

    # is_resolved
    df["is_resolved"] = df.get("status", pd.Series(dtype=str)) == "Closed"

    # Quarantine records with negative resolution_days (data entry errors)
    invalid_mask = df["resolution_days"].notna() & (df["resolution_days"].astype(float) < 0)
    n_invalid = int(invalid_mask.sum())
    if n_invalid:
        print(f"  quarantining {n_invalid:,} records with negative resolution_days")
    df = df[~invalid_mask].reset_index(drop=True)

    # Silver timestamp
    df["_silver_timestamp"] = datetime.now(timezone.utc).isoformat()

    # Write silver.service_requests
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE OR REPLACE TABLE silver.service_requests AS SELECT * FROM df")
    n_silver = con.execute("SELECT COUNT(*) FROM silver.service_requests").fetchone()[0]
    print(f"  silver.service_requests: {n_silver:,} rows")

    # Write DQ log
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dq_rows = [
        {"run_date": run_date, "check_name": "null_rate_unique_key",
         "records_checked": n_bronze, "records_failed": int(n_null_uk),
         "failure_rate": round(int(n_null_uk) / max(n_bronze, 1), 6),
         "pipeline_stage": "silver"},
        {"run_date": run_date, "check_name": "null_rate_created_date",
         "records_checked": n_bronze, "records_failed": int(n_null_cd),
         "failure_rate": round(int(n_null_cd) / max(n_bronze, 1), 6),
         "pipeline_stage": "silver"},
        {"run_date": run_date, "check_name": "duplicate_rate",
         "records_checked": n_bronze, "records_failed": n_bronze - n_deduped,
         "failure_rate": round((n_bronze - n_deduped) / max(n_bronze, 1), 6),
         "pipeline_stage": "silver"},
        {"run_date": run_date, "check_name": "invalid_resolution_days",
         "records_checked": n_deduped, "records_failed": n_invalid,
         "failure_rate": round(n_invalid / max(n_deduped, 1), 6),
         "pipeline_stage": "silver"},
        {"run_date": run_date, "check_name": "unrecognized_borough",
         "records_checked": n_deduped, "records_failed": int(n_unrecognized),
         "failure_rate": round(int(n_unrecognized) / max(n_deduped, 1), 6),
         "pipeline_stage": "silver"},
    ]
    dq_df = pd.DataFrame(dq_rows)
    con.execute("CREATE OR REPLACE TABLE silver.data_quality_log AS SELECT * FROM dq_df")
    print(f"  silver.data_quality_log: {len(dq_rows)} checks recorded for {run_date}")
    con.close()


# ── Stage 4: Gold (dbt) ────────────────────────────────────────────────────────

def _dbt_executable() -> str:
    # dbt-core 1.7 ships no __main__, so `python -m dbt` fails with
    # "'dbt' is a package and cannot be directly executed". Use the console
    # script installed next to this interpreter, falling back to PATH.
    import shutil
    candidate = Path(sys.executable).parent / "dbt"
    return str(candidate) if candidate.exists() else (shutil.which("dbt") or "dbt")


def _run_dbt(args: list[str]) -> int:
    cmd = [
        _dbt_executable(), *args,
        "--profiles-dir", str(LOCAL_DIR),
        "--project-dir",  str(LOCAL_DIR),
        "--no-version-check",
    ]
    print(f"\n  $ dbt {' '.join(args)}")
    return subprocess.run(cmd, cwd=LOCAL_DIR).returncode


def stage4_gold() -> None:
    _banner("Stage 4 — Gold  (dbt snapshot → run → test)")

    print("\n  Installing dbt packages...")
    _run_dbt(["deps"])

    print("\n  Running agency snapshot (SCD Type 2)...")
    _run_dbt(["snapshot"])

    print("\n  Building Gold models (full refresh)...")
    rc_run = _run_dbt(["run", "--full-refresh"])
    if rc_run != 0:
        print(f"\n  WARNING: dbt run exited {rc_run}")

    print("\n  Running dbt tests...")
    rc_test = _run_dbt(["test"])
    if rc_test != 0:
        print(f"\n  NOTE: dbt test exited {rc_test} — some tests failed (see above)")
    else:
        print("\n  All dbt tests passed.")


# ── Stage 5: Results ───────────────────────────────────────────────────────────

_QUERIES = [
    (
        "Top 10 complaint types",
        """
        SELECT complaint_type, COUNT(*) AS requests
        FROM gold.fct_service_requests
        GROUP BY complaint_type
        ORDER BY requests DESC
        LIMIT 10
        """,
    ),
    (
        "Avg resolution days by borough (closed only)",
        """
        SELECT
            l.borough,
            ROUND(AVG(f.resolution_days), 1)  AS avg_days,
            COUNT(*)                           AS closed_requests
        FROM gold.fct_service_requests f
        JOIN gold.dim_location l ON f.location_id = l.location_id
        WHERE f.resolution_days IS NOT NULL
        GROUP BY l.borough
        ORDER BY avg_days
        """,
    ),
    (
        "Complaints per year (most recent 10)",
        """
        SELECT d.year, COUNT(*) AS complaints
        FROM gold.fct_service_requests f
        JOIN gold.dim_date d ON f.created_date_id = d.date_id
        GROUP BY d.year
        ORDER BY d.year DESC
        LIMIT 10
        """,
    ),
    (
        "Open vs closed requests",
        """
        SELECT
            status,
            COUNT(*)                                                  AS total,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1)       AS pct
        FROM gold.fct_service_requests
        GROUP BY status
        ORDER BY total DESC
        """,
    ),
    (
        "Data quality check results",
        """
        SELECT
            check_name,
            records_checked,
            records_failed,
            ROUND(failure_rate * 100, 3) AS failure_pct
        FROM silver.data_quality_log
        ORDER BY check_name
        """,
    ),
]


def stage5_results() -> None:
    _banner("Stage 5 — Results")
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    for title, sql in _QUERIES:
        print(f"\n  {title}:")
        try:
            df = con.execute(sql.strip()).df()
            if df.empty:
                print("    (no rows)")
            else:
                for line in df.to_string(index=False).splitlines():
                    print(f"    {line}")
        except Exception as exc:
            print(f"    ERROR: {exc}")

    con.close()


# ── Entrypoint ─────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Local NYC 311 pipeline — no cloud credentials required",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=10_000,
        metavar="N",
        help="Rows to fetch from the Socrata API (default: 10000)",
    )
    parser.add_argument(
        "--stage",
        type=int,
        choices=[1, 2, 3, 4, 5],
        metavar="N",
        help="Start from stage N and run through stage 5 (skips earlier stages)",
    )
    args = parser.parse_args()
    start = args.stage or 1

    if start <= 1:
        stage1_ingest(args.rows)
    if start <= 2:
        stage2_bronze()
    if start <= 3:
        stage3_silver()
    if start <= 4:
        stage4_gold()
    if start <= 5:
        stage5_results()

    _banner("Complete")


if __name__ == "__main__":
    main()
