#!/usr/bin/env python3
"""
Local NYC 311 pipeline runner.

Pulls real data from the NYC Open Data Socrata API and runs the full
Bronze → Silver → Gold transformation pipeline on-laptop using DuckDB.
No cloud credentials, no Databricks, no Snowflake required.

Usage:
    python local_runner.py                  # all 5 stages, 10,000 most recent rows
    python local_runner.py --rows 50000     # larger dataset
    python local_runner.py --stage 3        # resume from stage 3 forward
    python local_runner.py --stage 5        # just reprint results
"""

import argparse
import json
import os
import subprocess
import sys
import time
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

# Socrata serves identical queries from replicas at different indexing states
# (measured 2026-08-26: six identical count calls returned 0,0,358,358,358,0).
# The source count is therefore sampled and maximised — see
# fetch_source_count_yesterday for why a single sample is unsafe.
SOURCE_COUNT_PROBES        = 5
SOURCE_COUNT_PAUSE_SECONDS = 0.6

# Silver transformation logic lives in silver_transformations.py so it can be
# unit-tested without a database. This module owns I/O only.
from dbt_exec import dbt_executable          # noqa: E402
from silver_transformations import (          # noqa: E402
    compute_dq_metrics,
    compute_resolution_days,
    deduplicate_on_unique_key,
    drop_quarantined,
    parse_timestamps,
    quarantine_mask,
    select_quarantine,
    standardize_borough,
)


def _banner(msg: str) -> None:
    print(f"\n{'─' * 64}")
    print(f"  {msg}")
    print(f"{'─' * 64}")


# ── Stage 1: Ingest ────────────────────────────────────────────────────────────

# Sample mode takes the NEWEST rows, contiguously.
#
# It used to read `$order=":id"` from offset 0 — the top of the dataset, which
# is its OLDEST rows (2020 onward). That sample is not the population this
# platform reports on. Every downstream rule (the complaint taxonomy in
# int_service_requests_cleaned, the closure_type text patterns) was derived from
# the recent live window `--live` fetches, and the city's complaint mix has
# moved since 2020: a 2020-first sample lands ~14.7% of rows in 'Other' against
# the taxonomy's 5% guard, dominated by types the recent window barely contains
# (`Request Large Bulky Item Collection`, `NonCompliance with Phased Reopening`).
# The right fix is the sample, not the guard — the default invocation should
# exercise the same data shape the daily run does. The guard stays at 5% and
# still fires on the old sample; see local/README_LOCAL.md.
#
# Paging is KEYSET, not offset. `$order=created_date DESC` with `$offset` is not
# stable on this dataset — measured on 2026-08-25, three offset pages skipped
# ~20 hours of 24 Aug entirely while the equivalent keyset walk did not. That
# matters beyond tidiness: a sample that omits the newest day while keeping
# rows closed on that day drives `observation_days` in fct_complaint_recurrence
# negative, because the loaded horizon (max created_date) falls behind the
# closures inside it. Walking `created_date <= cursor` instead keeps the sample
# a contiguous slice ending at the newest published row.
#
# `:id` is the tiebreak, not the sort key: it makes the ordering total so rows
# sharing a created_date cannot reshuffle between pages. The cursor is
# inclusive (`<=`) so tied rows on the page boundary are not skipped; the
# unique_key seen-set drops the resulting overlap, and a page that yields
# nothing new ends the walk rather than looping.
SAMPLE_ORDER = "created_date DESC, :id"


def stage1_ingest(rows: int) -> None:
    _banner(f"Stage 1 — Ingest  ({rows:,} most recent rows from Socrata API)")
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    records: list = []
    seen: set = set()
    cursor: str | None = None
    while len(records) < rows:
        params = {"$limit": PAGE_SIZE, "$order": SAMPLE_ORDER}
        if cursor is not None:
            params["$where"] = f"created_date <= '{cursor}'"
        resp = requests.get(SOCRATA_ENDPOINT, params=params, timeout=30)
        resp.raise_for_status()
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
    """Fetch rows created-or-updated in the trailing `days` window.

    Query parameters come from the ONE existing param builder
    (local/ingest_config.build_page_params), in its
    created_window mode: :updated_at is mass re-stamped nightly (~540k
    rows/day measured vs ~53k/week created — ADR 010), so the daily run
    windows on created_date and re-pulls the whole window, which still
    captures status updates for rows inside it. `get` is injectable for
    tests; nothing here retries more than once, caps are hard failures, and
    zero rows is a failure — the scheduled run must be red or fully green,
    never partially loaded.
    """
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
    # Sample the count SEVERAL times and keep the maximum.
    #
    # Socrata is not read-consistent: identical queries are served by replicas at
    # different indexing states. Measured 2026-08-26, six identical calls for the
    # same day returned 0, 0, 358, 358, 358, 0 — the denominator of SLO-2 varying
    # by 358 rows with no change at the source.
    #
    # A single sample was therefore a coin flip, and losing it is worse than
    # noisy: slo2_completeness.sql treats a zero denominator as a PASS, so a
    # capture landing on a lagging replica makes the completeness gate certify a
    # comparison against nothing. That is the gate the 2026-08-18 postmortem
    # exists to defend.
    #
    # Maximum, not mean or last: a row visible on ANY replica exists, so the
    # highest count is the most complete view available. The spread is returned
    # rather than discarded, because "the replicas disagreed" is a fact about the
    # source worth carrying into silver.source_counts, not smoothing away.
    samples: list[int] = []
    for probe in range(SOURCE_COUNT_PROBES):
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
        samples.append(int(payload[0]["n"]))
        if probe < SOURCE_COUNT_PROBES - 1:
            time.sleep(SOURCE_COUNT_PAUSE_SECONDS)

    source_count = max(samples)
    if len(set(samples)) > 1:
        print(f"  NOTE: source replicas disagreed on {target}: {samples} — taking {source_count}")
    return {
        "target_date": target,
        "source_count": source_count,
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

def _sql_str(value: str) -> str:
    """Quote a value as a SQL string literal, doubling embedded single quotes."""
    return "'" + str(value).replace("'", "''") + "'"


def raw_ingest_timestamp() -> str:
    """The moment the raw file was written, as Bronze's ingest stamp.

    Taken from the file's mtime rather than `now()`, so the stamp describes the
    DATA and not the run that happened to look at it. Re-running stage 2 no
    longer changes what Bronze says about when its rows arrived.
    """
    return datetime.fromtimestamp(RAW_FILE.stat().st_mtime, timezone.utc).isoformat()


def stage2_bronze() -> None:
    _banner("Stage 2 — Bronze  (register a view over the raw file)")
    if not RAW_FILE.exists():
        sys.exit(f"  ERROR: {RAW_FILE} not found — run stage 1 first")

    # Bronze is the raw file, exposed through a VIEW rather than copied into a
    # table. Two reasons, one architectural and one practical.
    #
    # Architectural: this pipeline transforms BEFORE it loads. The only
    # service-request data written into the warehouse is the cleaned Silver
    # table. Materialising a Bronze table would mean loading raw data and then
    # transforming it in-warehouse, which is the opposite pattern — and it is
    # what this stage used to do, with Silver reading the table straight back
    # out again into pandas.
    #
    # Practical: the view costs nothing and keeps raw SQL-queryable, which is
    # how fields Gold drops (council_district, bbl, police_precinct) stay
    # reachable without re-fetching from the API.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    # Drop by ACTUAL type. `DROP TABLE IF EXISTS` does not tolerate the object
    # already being a view — IF EXISTS suppresses "not found", not "wrong type"
    # — so it raised on every run after the first, when bronze had already been
    # converted. Found by running stage 2 twice.
    existing = con.execute(
        """SELECT table_type FROM information_schema.tables
           WHERE table_schema = 'bronze' AND table_name = 'service_requests'"""
    ).fetchone()
    if existing:
        kind = "VIEW" if existing[0] == "VIEW" else "TABLE"
        con.execute(f"DROP {kind} IF EXISTS bronze.service_requests")
    # Literals are inlined rather than bound: DuckDB cannot prepare a CREATE
    # VIEW ("Unexpected prepared parameter"), because the view definition is
    # stored as text and a placeholder would have nothing to bind to later.
    # All three values are internal, and _sql_str still doubles any quote.
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

    # Read the RAW FILE, not a Bronze table. The transform happens here, before
    # anything is written to the warehouse — the load is the `CREATE TABLE
    # silver...` at the end of this function and nothing before it.
    #
    # This previously read `SELECT * FROM bronze.service_requests`, which meant
    # the same rows were written into DuckDB by stage 2 and pulled straight back
    # out again here. The round-trip bought nothing and made the layer boundary
    # ambiguous: data was in the warehouse, then out of it, then in again.
    #
    # Every transformation below is a call into silver_transformations, which is
    # unit-tested in tests/unit/. This function owns only I/O and logging.
    with open(RAW_FILE) as fh:
        df_bronze = pd.DataFrame(json.load(fh))
    df_bronze["_ingest_timestamp"] = raw_ingest_timestamp()
    df_bronze["_source_file"] = RAW_FILE.name
    print(f"  raw rows: {len(df_bronze):,}")

    df = deduplicate_on_unique_key(df_bronze)
    print(f"  after dedup: {len(df):,} rows "
          f"({len(df_bronze) - len(df):,} duplicates removed)")

    df = standardize_borough(df)
    df = compute_resolution_days(parse_timestamps(df))

    df_derived = df
    n_invalid = int(quarantine_mask(df_derived).sum())
    if n_invalid:
        print(f"  quarantining {n_invalid:,} records with negative resolution_days")
    df = drop_quarantined(df_derived)

    # Silver timestamp
    df["_silver_timestamp"] = datetime.now(timezone.utc).isoformat()

    # Write silver.service_requests
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE OR REPLACE TABLE silver.service_requests AS SELECT * FROM df")
    n_silver = con.execute("SELECT COUNT(*) FROM silver.service_requests").fetchone()[0]
    print(f"  silver.service_requests: {n_silver:,} rows")

    # Write silver.quarantine — the rows dropped above, kept rather than discarded.
    #
    # Until now `select_quarantine` was written and unit-tested but never
    # imported here, so "quarantine" meant "delete" and only a count survived in
    # fct_data_quality. That left Gold unable to correct itself: a row loaded by
    # an earlier run and rejected by a later one stayed in the fact table
    # forever, because quarantine happens before dbt sees anything and the fact
    # table's reconciliation post_hook can only see rows that reached staging.
    #
    # REPLACED, not appended, on purpose. This table means "rows the CURRENT
    # fetch rejects", and Silver re-pulls the whole window every run. Appending
    # would be actively wrong: a row quarantined today and corrected by the city
    # tomorrow would stay listed, and the post_hook would then delete a valid
    # row from Gold on every subsequent run.
    # noqa for the same reason as dq_df above: DuckDB's replacement scan
    # resolves `FROM df_quarantined` in the statement below against this local
    # variable, so the name IS the interface and static analysis cannot see it.
    df_quarantined = select_quarantine(df_derived)  # noqa: F841
    con.execute("""
        CREATE OR REPLACE TABLE silver.quarantine AS
        SELECT unique_key, created_date, closed_date, resolution_days,
               'negative_resolution_days' AS quarantine_reason,
               ? AS _silver_timestamp
        FROM df_quarantined
    """, [df["_silver_timestamp"].iloc[0] if len(df) else datetime.now(timezone.utc).isoformat()])
    n_q = con.execute("SELECT COUNT(*) FROM silver.quarantine").fetchone()[0]
    print(f"  silver.quarantine: {n_q:,} rows retained for inspection")

    # Write DQ log
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dq_rows = compute_dq_metrics(df_bronze, df, df_derived, run_date)
    # noqa is correct here, not a silencer: DuckDB's replacement scan resolves
    # `FROM dq_df` in the INSERT below against this local variable, so the name
    # IS the interface. Static analysis cannot see a reference inside SQL text.
    dq_df = pd.DataFrame(dq_rows)  # noqa: F841
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

def _run_dbt(args: list[str]) -> int:
    cmd = [
        # `python -m dbt` does not work — see local/dbt_exec.py for why, and for
        # the single definition this and tests/local/conftest.py both use.
        dbt_executable() or "dbt", *args,
        "--profiles-dir", str(LOCAL_DIR),
        "--project-dir",  str(LOCAL_DIR),
        "--no-version-check",
    ]
    print(f"\n  $ dbt {' '.join(args)}")
    return subprocess.run(cmd, cwd=LOCAL_DIR, check=False).returncode


def stage4_gold(incremental: bool = False) -> None:
    _banner("Stage 4 — Gold  (dbt build: models + snapshot + tests in DAG order)")

    print("\n  Installing dbt packages...")
    # Check this exit code. A failed `deps` does not stop the build below — it
    # fails later on a missing macro, which reads as a broken model rather than
    # as "the package install failed". Surface the real cause here.
    rc_deps = _run_dbt(["deps"])
    if rc_deps != 0:
        print(f"\n  ERROR: dbt deps exited {rc_deps} — packages not installed")
        sys.exit(rc_deps)

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
             f"(row-capped, created_date watermark) instead of an --rows sample",
    )
    args = parser.parse_args()

    # --only runs a single stage and returns. Stage 1 still honours --live/--rows;
    # stage 4 decides incremental-vs-full-refresh from whether the DB pre-exists,
    # exactly as a full run would.
    if args.only:
        db_existed = DUCKDB_PATH.exists()
        if args.only == 1:
            stage1_live() if args.live else stage1_ingest(args.rows)
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
