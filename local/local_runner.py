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
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import requests

LOCAL_DIR   = Path(__file__).parent.resolve()
DATA_DIR    = LOCAL_DIR / "data"
RAW_DIR     = DATA_DIR / "raw"
DUCKDB_PATH = DATA_DIR / "nyc311_local.duckdb"
RAW_FILE    = RAW_DIR / "nyc311_raw.json"
SOURCE_COUNT_FILE = RAW_DIR / "source_count.json"

SOCRATA_ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE        = 1_000

# ── Live mode (--live): trailing-window fetch for the scheduled daily run ─────
# A normal week of NYC 311 is ~60–90k rows including re-fetched updates; the
# cap is ~2× that. Hitting it is treated as an upstream anomaly (e.g. a
# dataset-wide metadata touch re-stamping :updated_at) and FAILS the run —
# a capped fetch would silently undercount and corrupt the completeness SLO.
# See ADR 010.
LIVE_DAYS    = 7
LIVE_ROW_CAP = 150_000

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


def fetch_live_records(days: int = LIVE_DAYS, cap: int = LIVE_ROW_CAP, get=None) -> list:
    """Fetch rows created-or-updated in the trailing `days` window.

    Query parameters come from the ONE existing param builder
    (databricks/notebooks/ingest_config.build_page_params), in its
    created_window mode: :updated_at is mass re-stamped nightly (~540k
    rows/day measured vs ~53k/week created — ADR 010), so the daily run
    windows on created_date and re-pulls the whole window, which still
    captures status updates for rows inside it. `get` is injectable for
    tests; nothing here retries more than once, caps are hard failures, and
    zero rows is a failure — the scheduled run must be red or fully green,
    never partially loaded.
    """
    sys.path.insert(0, str(LOCAL_DIR.parent / "databricks" / "notebooks"))
    from ingest_config import SOCRATA_URL, build_page_params

    if get is None:
        get = requests.get

    headers = {"Accept": "application/json"}
    token = os.environ.get("SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    run_date = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    records: list = []
    page = 0
    while True:
        params = build_page_params("created_window", run_date, page)
        for attempt in (1, 2):
            try:
                resp = get(SOCRATA_URL, params=params, headers=headers, timeout=60)
                break
            except Exception as exc:
                if attempt == 2:
                    raise RuntimeError(
                        f"Socrata fetch failed after one retry on page {page}: {exc}"
                    ) from exc
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        records.extend(batch)
        page += 1
        if len(records) > cap:
            raise RuntimeError(
                f"Live fetch exceeded the row cap ({len(records):,} > {cap:,} in "
                f"{days} days). This signals an upstream anomaly (mass re-stamp of "
                f":updated_at or a volume spike) — investigate before raising "
                f"LIVE_ROW_CAP in local_runner.py / ADR 010."
            )
    if not records:
        raise RuntimeError(
            f"Live fetch returned zero rows for the trailing {days} days — the "
            f"source is not publishing or the window predicate is wrong. "
            f"Refusing to continue with an empty load."
        )
    return records


def fetch_source_count_yesterday(get=None) -> dict:
    """Ask the source how many requests IT has for yesterday (UTC).

    This single number is what turns SLO-2 into a reconciliation: our
    completeness is measured against what the city actually published, not
    against a historical volume guess. Captured at fetch time — the same
    API, the same auth, the same one-retry/fail-loud contract as
    fetch_live_records: if the count query fails, the run fails, because a
    missing capture would otherwise silently degrade the SLO gate.
    `get` is injectable for tests.
    """
    sys.path.insert(0, str(LOCAL_DIR.parent / "databricks" / "notebooks"))
    from ingest_config import SOCRATA_URL

    if get is None:
        get = requests.get

    headers = {"Accept": "application/json"}
    token = os.environ.get("SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token

    target = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    params = {
        "$select": "count(*) as n",
        "$where": (f"created_date between '{target}T00:00:00.000'"
                   f" and '{target}T23:59:59.999'"),
    }
    for attempt in (1, 2):
        try:
            resp = get(SOCRATA_URL, params=params, headers=headers, timeout=60)
            break
        except Exception as exc:
            if attempt == 2:
                raise RuntimeError(
                    f"Socrata source-count query failed after one retry: {exc}"
                ) from exc
    resp.raise_for_status()
    payload = resp.json()
    if not payload or "n" not in payload[0]:
        raise RuntimeError(f"Socrata source-count query returned no count: {payload!r}")
    return {
        "target_date": target,
        "source_count": int(payload[0]["n"]),
        "captured_at": datetime.now(timezone.utc).isoformat(),
    }


def stage1_live() -> None:
    _banner(f"Stage 1 — Live ingest  (trailing {LIVE_DAYS} days, cap {LIVE_ROW_CAP:,})")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    records = fetch_live_records()
    # Compact JSON, unlike the sample mode's indent=2: a full week is an order
    # of magnitude larger and this file is a pipeline intermediate, not a
    # human-reading surface.
    RAW_FILE.write_text(json.dumps(records))
    print(f"  fetched {len(records):,} rows created since "
          f"{(datetime.now(timezone.utc) - timedelta(days=LIVE_DAYS)).date()}")
    print(f"  written: {RAW_FILE.relative_to(LOCAL_DIR)}")

    # Source-side truth for SLO-2's reconciliation (loaded into DuckDB by
    # stage 3, read by scripts/slo/slo2_completeness.sql).
    count = fetch_source_count_yesterday()
    SOURCE_COUNT_FILE.write_text(json.dumps(count))
    print(f"  source reports {count['source_count']:,} requests created "
          f"{count['target_date']} (written: {SOURCE_COUNT_FILE.relative_to(LOCAL_DIR)})")


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

    # Timestamp parsing — coerce bad values to NaT. Parse NAIVE, not utc=True:
    # Socrata sends naive NYC-local timestamps, and the cloud spec stores them
    # as TIMESTAMP_NTZ (no zone). Labeling them UTC here made the downstream
    # ::timestamp cast shift every value by the machine's UTC offset — caught
    # by source reconciliation as wrong calendar dates for after-midnight rows.
    for col in ("created_date", "closed_date", "resolution_action_updated_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
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

    # Quarantine records with negative resolution_days (data entry errors).
    # to_numeric, not astype(float): the column is nullable Int64 and astype
    # raises on pd.NA; to_numeric keeps NA as NaN, which compares False.
    invalid_mask = df["resolution_days"].notna() & (
        pd.to_numeric(df["resolution_days"], errors="coerce") < 0
    )
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
    # Append, don't replace: the DQ log accumulates across runs (mirroring the
    # cloud spec, where 03_silver.py appends per run) so fct_data_quality's
    # 7-day rolling window has real history when the database persists between
    # scheduled runs. Idempotent per run_date: re-running today replaces
    # today's checks instead of duplicating the (run_date, check_name) grain.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver.data_quality_log (
            run_date VARCHAR, check_name VARCHAR, records_checked BIGINT,
            records_failed BIGINT, failure_rate DOUBLE, pipeline_stage VARCHAR)
    """)
    con.execute("DELETE FROM silver.data_quality_log WHERE run_date = ?", [run_date])
    con.execute("""
        INSERT INTO silver.data_quality_log
        SELECT run_date, check_name, records_checked,
               records_failed, failure_rate, pipeline_stage
        FROM dq_df
    """)
    n_dq = con.execute("SELECT COUNT(*) FROM silver.data_quality_log").fetchone()[0]
    print(f"  silver.data_quality_log: {len(dq_rows)} checks recorded for "
          f"{run_date} ({n_dq} rows across all runs)")

    # Source counts captured in stage 1 (live mode only) — the reconciliation
    # target for SLO-2. Accumulates across runs like the DQ log; idempotent
    # per target_date. NOT a dbt source: no model reads it — it exists solely
    # for scripts/slo/slo2_completeness.sql.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver.source_counts (
            target_date DATE, source_count BIGINT, captured_at TIMESTAMP)
    """)
    if SOURCE_COUNT_FILE.exists():
        sc = json.loads(SOURCE_COUNT_FILE.read_text())
        con.execute("DELETE FROM silver.source_counts WHERE target_date = ?",
                    [sc["target_date"]])
        con.execute("INSERT INTO silver.source_counts VALUES (?, ?, ?)",
                    [sc["target_date"], sc["source_count"], sc["captured_at"]])
        print(f"  silver.source_counts: {sc['source_count']:,} for {sc['target_date']}")
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


def stage4_gold(incremental: bool = False) -> None:
    _banner("Stage 4 — Gold  (dbt build: models + snapshot + tests in DAG order)")

    print("\n  Installing dbt packages...")
    _run_dbt(["deps"])

    # dbt build resolves the whole DAG: the agency snapshot runs AFTER the
    # intermediate model it reads (a bare `dbt snapshot` first fails on a
    # fresh database — the model does not exist yet), and each model's tests
    # run right after it builds.
    #
    # Incremental mode (DB existed before this run): plain `dbt build`, so the
    # fact merges only fresh rows, snapshot history accumulates across runs,
    # and the scheduled daily run exercises the SAME incremental path the
    # Snowflake spec describes — not a daily from-scratch rebuild.
    if incremental:
        print("\n  Building Gold (incremental — existing database)...")
        rc_build = _run_dbt(["build"])
    else:
        print("\n  Building Gold (full refresh — fresh database)...")
        rc_build = _run_dbt(["build", "--full-refresh"])
    if rc_build != 0:
        print(f"\n  ERROR: dbt build exited {rc_build} — see output above")
        sys.exit(rc_build)
    print("\n  Gold built; all dbt tests passed (model, source, and singular"
          " tests run inside dbt build — see local/models/*.yml and local/tests/).")


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
    parser.add_argument(
        "--live",
        action="store_true",
        help=f"Fetch the trailing {LIVE_DAYS} days of live data (row-capped, "
             f":updated_at watermark) instead of the oldest --rows sample",
    )
    args = parser.parse_args()
    start = args.stage or 1

    # Captured BEFORE any stage runs: stage 2/3 create the file, so testing
    # later would always report an existing DB. An existing database means a
    # prior run's Gold state is present → build incrementally on top of it.
    db_existed = DUCKDB_PATH.exists()

    if start <= 1:
        if args.live:
            stage1_live()
        else:
            stage1_ingest(args.rows)
    if start <= 2:
        stage2_bronze()
    if start <= 3:
        stage3_silver()
    if start <= 4:
        stage4_gold(incremental=db_existed)
    if start <= 5:
        stage5_results()

    _banner("Complete")


if __name__ == "__main__":
    main()
