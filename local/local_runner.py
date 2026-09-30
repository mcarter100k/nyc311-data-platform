#!/usr/bin/env python3
"""
Local NYC 311 pipeline runner: Socrata API -> Bronze -> Silver -> Gold in DuckDB.

No cloud credentials needed.

Usage:
    python local_runner.py                  # all 5 stages, 10,000 most recent rows
    python local_runner.py --rows 50000     # larger sample
    python local_runner.py --live           # the daily run: trailing LIVE_DAYS window
    python local_runner.py --stage 3        # resume from stage 3 forward
    python local_runner.py --stage 5        # just reprint results
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, UTC
from pathlib import Path

import duckdb
import pandas as pd
import requests

# Sibling modules. Silver logic lives in silver_transformations so it can be
# unit-tested without a database; this module owns I/O.
from dbt_exec import dbt_executable
from ingest_config import SOCRATA_URL, build_page_params
from silver_transformations import (
    compute_dq_metrics,
    compute_resolution_days,
    deduplicate_on_unique_key,
    drop_quarantined,
    parse_timestamps,
    quarantine_mask,
    select_quarantine,
    standardize_borough,
)

LOCAL_DIR   = Path(__file__).parent.resolve()
DATA_DIR    = LOCAL_DIR / "data"
RAW_DIR     = DATA_DIR / "raw"
DUCKDB_PATH = DATA_DIR / "nyc311_local.duckdb"
RAW_FILE    = RAW_DIR / "nyc311_raw.json"
SOURCE_COUNT_FILE = RAW_DIR / "source_count.json"

SAMPLE_PAGE_SIZE = 1_000

# ── Live mode (--live): trailing-window fetch for the scheduled daily run ─────
# 37 days = the 30-day closure window plus the 7-day settling horizon (ADR 016),
# so a request's closure is re-fetched until its 30-day metric is final. With 7
# days the published 30-day closure rate read 68.4% instead of 89.5% (ADR 010).
LIVE_DAYS    = 37
# A 37-day window is ~385k rows (measured); the cap is ~2x that. Hitting it
# means an upstream volume spike and FAILS the run, because a capped fetch
# would undercount SLO-2.
LIVE_ROW_CAP = 800_000

# Socrata serves each query from one of two replicas, one of which lags (ADR 016).
# Source counts are probed N times and the per-day MAX kept. A day's count is
# wrong only if every probe hits the stale replica: 0.65^N at the worst
# measured stale share. 11 is the smallest N that keeps that under 1% (0.0088).
SOURCE_COUNT_PROBES        = 11
SOURCE_COUNT_PAUSE_SECONDS = 0.6

# Retry only transient faults (connection errors, 429, 5xx); any other non-2xx
# fails at once. 3 attempts = 2 retries, backoff 1s then 2s. Running out of
# retries raises (ADR 010).
HTTP_ATTEMPTS          = 3
HTTP_BACKOFF_SECONDS   = 1.0
HTTP_RETRYABLE_STATUS  = frozenset({429, 500, 502, 503, 504})


def _banner(msg: str) -> None:
    print(f"\n{'─' * 64}")
    print(f"  {msg}")
    print(f"{'─' * 64}")


def _socrata_headers() -> dict:
    """Request headers, with the app token when SOCRATA_APP_TOKEN is set."""
    headers = {"Accept": "application/json"}
    token = os.environ.get("SOCRATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token
    return headers


def _get_with_retry(get, url, *, params, headers=None, timeout=60, what="Socrata request"):
    """One HTTP GET with bounded retries on transient faults.

    Returns the response on success. A non-retryable status raises from
    raise_for_status on the first response; exhausted retries raise a
    RuntimeError that names `what`.
    """
    reason: BaseException | None = None
    for attempt in range(1, HTTP_ATTEMPTS + 1):
        try:
            resp = get(url, params=params, headers=headers, timeout=timeout)
        except Exception as exc:                       # connection-level fault
            reason = exc
        else:
            # Test fakes may omit status_code; raise_for_status still decides.
            if getattr(resp, "status_code", None) in HTTP_RETRYABLE_STATUS:
                reason = RuntimeError(f"HTTP {resp.status_code} from the source")
            else:
                resp.raise_for_status()                # non-retryable: fail now
                return resp

        if attempt == HTTP_ATTEMPTS:
            raise RuntimeError(
                f"{what} failed after {HTTP_ATTEMPTS} attempts "
                f"({HTTP_ATTEMPTS - 1} retries): {reason}"
            ) from reason
        time.sleep(HTTP_BACKOFF_SECONDS * 2 ** (attempt - 1))


# ── Stage 1: Ingest ────────────────────────────────────────────────────────────

# Sample mode takes the NEWEST rows so it looks like the data --live sees.
# Paging is keyset (created_date <= cursor), because offset paging on this sort
# was measured to skip rows. :id breaks ties; the seen-set drops rows repeated
# at page boundaries.
SAMPLE_ORDER = "created_date DESC, :id"


def stage1_ingest(rows: int) -> None:
    _banner(f"Stage 1 — Ingest  ({rows:,} most recent rows from Socrata API)")
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    records: list = []
    seen: set = set()
    cursor: str | None = None
    while len(records) < rows:
        params = {"$limit": SAMPLE_PAGE_SIZE, "$order": SAMPLE_ORDER}
        if cursor is not None:
            params["$where"] = f"created_date <= '{cursor}'"
        resp = _get_with_retry(requests.get, SOCRATA_URL, params=params,
                               headers=_socrata_headers(), timeout=30,
                               what="Socrata sample fetch")
        page = resp.json()
        if not page:
            break
        fresh = [r for r in page if r.get("unique_key") not in seen]
        if not fresh:
            print(f"\n  no rows older than {cursor} — stopping at {len(records):,}")
            break
        seen.update(r.get("unique_key") for r in fresh)
        records.extend(fresh)
        cursor = min(r["created_date"] for r in page)
        print(f"  fetched {len(records):,} / {rows:,} rows", end="\r", flush=True)

    del records[rows:]
    print(f"\n  total fetched: {len(records):,} rows")
    RAW_FILE.write_text(json.dumps(records, indent=2))
    print(f"  written: {RAW_FILE.relative_to(LOCAL_DIR)}")


def fetch_live_records(days: int = LIVE_DAYS, cap: int = LIVE_ROW_CAP, get=None) -> list:
    """Fetch every row created in the trailing `days` window.

    The whole window is re-pulled each run, so status changes inside it are
    captured. Hitting `cap` and fetching zero rows both raise: the daily run is
    red or fully loaded, never partly loaded. `get` is injectable for tests.
    """
    if get is None:
        get = requests.get
    headers = _socrata_headers()

    run_date = (datetime.now(UTC) - timedelta(days=days)).date().isoformat()
    records: list = []
    page = 0
    while True:
        resp = _get_with_retry(get, SOCRATA_URL, params=build_page_params(run_date, page),
                               headers=headers, what=f"Socrata fetch on page {page}")
        batch = resp.json()
        if not batch:
            break
        records.extend(batch)
        page += 1
        if len(records) > cap:
            raise RuntimeError(
                f"Live fetch exceeded the row cap ({len(records):,} > {cap:,} in "
                f"{days} days). This signals an upstream volume spike — investigate "
                f"before raising LIVE_ROW_CAP in local_runner.py / ADR 010."
            )
    if not records:
        raise RuntimeError(
            f"Live fetch returned zero rows for the trailing {days} days — the "
            f"source is not publishing or the window predicate is wrong. "
            f"Refusing to continue with an empty load."
        )
    return records


def fetch_source_counts_window(days: int = LIVE_DAYS, get=None) -> list[dict]:
    """Source row count for every day in the window, for SLO-2.

    The SLO gate chooses which days to check later (ADR 015), so every day is
    captured. Days the source has no rows for are stored as 0, not left out.
    source_count_min, probe_count and probes_disagreed record whether the
    replicas disagreed. Fails loudly like fetch_live_records. `get` is
    injectable for tests.
    """
    if get is None:
        get = requests.get
    headers = _socrata_headers()

    today = datetime.now(UTC).date()
    start = today - timedelta(days=days)
    # One grouped request returns every day of the window.
    params = {
        "$select": "date_trunc_ymd(created_date) as day, count(*) as n",
        "$where": f"created_date >= '{start.isoformat()}T00:00:00'",
        "$group": "date_trunc_ymd(created_date)",
        "$limit": 5000,
    }

    # Replicas lag and only ever under-count, so keep the per-day MAX over the
    # probes (ADR 016). This makes SLO-2 stricter. It cannot help when no
    # replica has the day yet; SLO-2 fails on a zero count.
    samples: dict[str, list[int]] = {}
    for probe in range(SOURCE_COUNT_PROBES):
        resp = _get_with_retry(get, SOCRATA_URL, params=params, headers=headers,
                               what="Socrata source-count query")
        payload = resp.json()
        if payload is None or not isinstance(payload, list):
            raise RuntimeError(f"Socrata source-count query returned no counts: {payload!r}")
        for row in payload:
            if "day" not in row or "n" not in row:
                raise RuntimeError(f"Socrata source-count row is missing columns: {row!r}")
            samples.setdefault(str(row["day"])[:10], []).append(int(row["n"]))
        if probe < SOURCE_COUNT_PROBES - 1:
            time.sleep(SOURCE_COUNT_PAUSE_SECONDS)

    captured_at = datetime.now(UTC).isoformat()
    counts: list[dict] = []
    day = start
    while day <= today:
        key = day.isoformat()
        # A day missing from a probe's response is that probe's zero; without
        # the padding, one replica's sighting would read as unanimous.
        seen = samples.get(key, [])
        seen = seen + [0] * (SOURCE_COUNT_PROBES - len(seen))
        n = max(seen)
        lo = min(seen)
        disagreed = lo != n
        if disagreed:
            print(f"  NOTE: source replicas disagreed on {key}: "
                  f"{lo}..{n} over {len(seen)} probes — taking {n}")
        counts.append({
            "target_date":      key,
            "source_count":     n,
            "captured_at":      captured_at,
            "source_count_min": lo,
            "probe_count":      len(seen),
            "probes_disagreed": disagreed,
        })
        day += timedelta(days=1)
    return counts


def stage1_live(days: int = LIVE_DAYS) -> None:
    """Fetch the trailing `days` window and the source's per-day counts.

    A wider window replays a range: every row in it is re-fetched and upserted
    (see the window_days input in .github/workflows/daily-run.yml).
    """
    _banner(f"Stage 1 — Live ingest  (trailing {days} days, cap {LIVE_ROW_CAP:,})")
    if days != LIVE_DAYS:
        print(f"  NOTE: non-default window ({days}d vs {LIVE_DAYS}d) — replay or backfill run")
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    records = fetch_live_records(days=days)
    RAW_FILE.write_text(json.dumps(records))   # compact: this file is large
    print(f"  fetched {len(records):,} rows created since "
          f"{(datetime.now(UTC) - timedelta(days=days)).date()}")
    print(f"  written: {RAW_FILE.relative_to(LOCAL_DIR)}")

    # SLO-2's reference counts, loaded by stage 3.
    counts = fetch_source_counts_window(days=days)
    SOURCE_COUNT_FILE.write_text(json.dumps(counts))
    total = sum(c["source_count"] for c in counts)
    print(f"  source reports {total:,} requests created across {len(counts)} days "
          f"{counts[0]['target_date']}..{counts[-1]['target_date']} "
          f"(written: {SOURCE_COUNT_FILE.relative_to(LOCAL_DIR)})")


# ── Stage 2: Bronze ────────────────────────────────────────────────────────────

def _sql_str(value: str) -> str:
    """Quote a value as a SQL string literal, doubling embedded single quotes."""
    return "'" + str(value).replace("'", "''") + "'"


def raw_ingest_timestamp() -> str:
    """The raw file's mtime (UTC), so the stamp describes the data, not the run."""
    return datetime.fromtimestamp(RAW_FILE.stat().st_mtime, UTC).isoformat()


def stage2_bronze() -> None:
    _banner("Stage 2 — Bronze  (register a view over the raw file)")
    if not RAW_FILE.exists():
        sys.exit(f"  ERROR: {RAW_FILE} not found — run stage 1 first")

    # Bronze is a view over the raw file, not a copy: the pipeline transforms
    # before it loads (ADR 014), and the view keeps fields Gold drops
    # (council_district, bbl, police_precinct) queryable without a re-fetch.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    # Drop by actual type: DROP TABLE IF EXISTS raises when the object is a view.
    existing = con.execute(
        """SELECT table_type FROM information_schema.tables
           WHERE table_schema = 'bronze' AND table_name = 'service_requests'"""
    ).fetchone()
    if existing:
        kind = "VIEW" if existing[0] == "VIEW" else "TABLE"
        con.execute(f"DROP {kind} IF EXISTS bronze.service_requests")
    # Values are inlined because DuckDB cannot prepare a CREATE VIEW. All are
    # internal, and _sql_str escapes quotes.
    con.execute(
        f"""
        CREATE OR REPLACE VIEW bronze.service_requests AS
        SELECT *,
               {_sql_str(raw_ingest_timestamp())} AS _ingest_timestamp,
               {_sql_str(RAW_FILE.name)}          AS _source_file
        FROM read_json_auto({_sql_str(str(RAW_FILE))})
        """
    )
    n = con.execute("SELECT COUNT(*) FROM bronze.service_requests").fetchone()[0]
    con.close()
    print(f"  bronze.service_requests (view over {RAW_FILE.name}): {n:,} rows")


# ── Stage 3: Silver ────────────────────────────────────────────────────────────

def stage3_silver() -> None:
    _banner("Stage 3 — Silver  (transform the raw file, then load the clean result)")
    if not RAW_FILE.exists():
        sys.exit(f"  ERROR: {RAW_FILE} not found — run stage 1 first")
    con = duckdb.connect(str(DUCKDB_PATH))

    # Transform from the raw file; the first write is the CREATE TABLE below (ADR 014).
    with open(RAW_FILE) as fh:
        df_bronze = pd.DataFrame(json.load(fh))
    df_bronze["_ingest_timestamp"] = raw_ingest_timestamp()
    df_bronze["_source_file"] = RAW_FILE.name
    print(f"  raw rows: {len(df_bronze):,}")

    df = deduplicate_on_unique_key(df_bronze)
    print(f"  after dedup: {len(df):,} rows "
          f"({len(df_bronze) - len(df):,} duplicates removed)")

    # Three populations: df_bronze (every fetched row), df_derived (one row per
    # unique_key, pre-quarantine: what every DQ rule is measured on) and df
    # (post-quarantine: what Silver stores).
    df_derived = compute_resolution_days(parse_timestamps(standardize_borough(df)))
    n_invalid = int(quarantine_mask(df_derived).sum())
    if n_invalid:
        print(f"  quarantining {n_invalid:,} records with negative resolution_days")
    df = drop_quarantined(df_derived)

    # _borough_raw and resolution_days are inputs to the DQ checks and the
    # quarantine, not Silver columns. Gold defines resolution_days itself.
    df = df.drop(columns=["_borough_raw", "resolution_days"], errors="ignore")
    df["_silver_timestamp"] = datetime.now(UTC).isoformat()

    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE OR REPLACE TABLE silver.service_requests AS SELECT * FROM df")
    n_silver = con.execute("SELECT COUNT(*) FROM silver.service_requests").fetchone()[0]
    print(f"  silver.service_requests: {n_silver:,} rows")

    # Keep rejected rows so dbt can delete them from Gold. Replaced each run: it
    # means "rows the current fetch rejects". noqa: DuckDB reads df_quarantined
    # by name from the SQL below.
    df_quarantined = select_quarantine(df_derived)  # noqa: F841
    con.execute("""
        CREATE OR REPLACE TABLE silver.quarantine AS
        SELECT unique_key, created_date, closed_date, resolution_days,
               'negative_resolution_days' AS quarantine_reason,
               ? AS _silver_timestamp
        FROM df_quarantined
    """, [df["_silver_timestamp"].iloc[0] if len(df) else datetime.now(UTC).isoformat()])
    n_q = con.execute("SELECT COUNT(*) FROM silver.quarantine").fetchone()[0]
    print(f"  silver.quarantine: {n_q:,} rows retained for inspection")

    # Measure DQ on df_derived (pre-quarantine), the rows the rules were applied to.
    run_date = datetime.now(UTC).strftime("%Y-%m-%d")
    dq_rows = compute_dq_metrics(df_bronze, df_derived, run_date)
    dq_df = pd.DataFrame(dq_rows)  # noqa: F841 — read by name in the INSERT below
    # Append so fct_data_quality has 7 days of history; re-running a day
    # replaces that day.
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

    # Source counts from stage 1 (live mode only), read by SLO-2; not a dbt
    # source. One row per day: a later capture replaces the earlier one, so a
    # day first captured while still settling is re-checked. Days that leave
    # the window keep their last capture.
    con.execute("""
        CREATE TABLE IF NOT EXISTS silver.source_counts (
            target_date DATE, source_count BIGINT, captured_at TIMESTAMP,
            source_count_min BIGINT, probe_count INTEGER, probes_disagreed BOOLEAN)
    """)
    if SOURCE_COUNT_FILE.exists():
        rows = json.loads(SOURCE_COUNT_FILE.read_text())
        for sc in rows:
            con.execute("DELETE FROM silver.source_counts WHERE target_date = ?",
                        [sc["target_date"]])
            # Probe columns are NULL for a capture file written before they existed.
            con.execute("""
                INSERT INTO silver.source_counts
                    (target_date, source_count, captured_at,
                     source_count_min, probe_count, probes_disagreed)
                VALUES (?, ?, ?, ?, ?, ?)
            """, [sc["target_date"], sc["source_count"], sc["captured_at"],
                  sc.get("source_count_min"), sc.get("probe_count"),
                  sc.get("probes_disagreed")])
        total = sum(r["source_count"] for r in rows)
        contested = sum(1 for r in rows if r.get("probes_disagreed"))
        print(f"  silver.source_counts: {len(rows)} day(s) refreshed, "
              f"{total:,} source rows {rows[0]['target_date']}..{rows[-1]['target_date']} "
              f"({contested} still settling — replicas disagreed)")
    con.close()


# ── Stage 4: Gold (dbt) ────────────────────────────────────────────────────────

def _run_dbt(args: list[str]) -> int:
    cmd = [
        dbt_executable() or "dbt", *args,   # not `python -m dbt`: see dbt_exec.py
        "--profiles-dir", str(LOCAL_DIR),
        "--project-dir",  str(LOCAL_DIR),
        "--no-version-check",
    ]
    print(f"\n  $ dbt {' '.join(args)}")
    return subprocess.run(cmd, cwd=LOCAL_DIR, check=False).returncode


def stage4_gold(incremental: bool = False) -> None:
    _banner("Stage 4 — Gold  (dbt build: models + snapshot + tests in DAG order)")

    print("\n  Installing dbt packages...")
    # A failed deps would otherwise surface later as a missing macro in a model.
    rc_deps = _run_dbt(["deps"])
    if rc_deps != 0:
        print(f"\n  ERROR: dbt deps exited {rc_deps} — packages not installed")
        sys.exit(rc_deps)

    # `dbt build` runs models, the snapshot and tests in DAG order. On an
    # existing database it is incremental: the fact merges fresh rows and
    # snapshot history accumulates across runs.
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
        help="Most recent rows to fetch from the Socrata API (default: 10000)",
    )
    parser.add_argument(
        "--stage",
        type=int,
        choices=[1, 2, 3, 4, 5],
        metavar="N",
        help="Start from stage N and run through stage 5 (skips earlier stages)",
    )
    parser.add_argument(
        "--only",
        type=int,
        choices=[1, 2, 3, 4, 5],
        metavar="N",
        help="Run ONLY stage N and stop, instead of running N through 5. Used by "
             "the Airflow DAG, which maps one task per stage so a failure names "
             "the failing stage directly.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help=f"Fetch the whole trailing {LIVE_DAYS}-day window of live data "
             f"(row-capped, filtered on created_date) instead of an --rows sample",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=LIVE_DAYS,
        help=f"Width of the --live window in days (default {LIVE_DAYS}). Widen it to "
             f"replay a range: every row in the window is re-fetched and upserted.",
    )
    args = parser.parse_args()

    # --only runs a single stage. Stage 4 run this way is always incremental,
    # because earlier stages have already created the DB file. On a fresh DB that
    # is harmless: dbt builds incremental models from scratch the first time.
    if args.only:
        db_existed = DUCKDB_PATH.exists()
        if args.only == 1:
            stage1_live(args.days) if args.live else stage1_ingest(args.rows)
        elif args.only == 2:
            stage2_bronze()
        elif args.only == 3:
            stage3_silver()
        elif args.only == 4:
            stage4_gold(incremental=db_existed)
        elif args.only == 5:
            stage5_results()
        _banner("Complete")
        return

    start = args.stage or 1

    # Checked before stages 2-3 create the file: an existing DB holds a prior
    # run's Gold, so stage 4 builds incrementally on top of it.
    db_existed = DUCKDB_PATH.exists()

    if start <= 1:
        if args.live:
            stage1_live(args.days)
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
