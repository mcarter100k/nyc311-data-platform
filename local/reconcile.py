#!/usr/bin/env python3
"""
Source-to-target reconciliation for the local NYC 311 pipeline.

Tests check that the pipeline agrees with itself; this checks that Gold agrees
with the source. Three rungs, weakest to strongest:

  1. CONSERVATION   — every ingested record is accounted for across layers:
                      raw = bronze, silver = deduped - quarantined,
                      gold = silver, and the daily aggregate sums to the fact.
  2. RECOMPUTATION  — headline numbers recomputed straight from the raw JSON
                      with plain Python (no DuckDB, no dbt): closed counts,
                      borough distribution, per-record resolution days, and
                      exact created_date timestamps.
  3. LIVE SOURCE    — a sample of Gold records fetched back from the city's
                      API by unique_key and compared field by field. Skipped
                      (not failed) when the network is unavailable.

Run immediately after a pipeline run, from the repo root or local/:

    python local/reconcile.py            # exit 0 = reconciled, 1 = mismatch

The resolution-days definition is calendar-day difference (date boundaries
crossed), matching datediff('day', ...) in the dbt models.
"""

import json
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

import duckdb
import requests

sys.path.insert(0, str(Path(__file__).parent))
from ingest_config import SOCRATA_URL
from local_runner import DUCKDB_PATH, RAW_FILE, _get_with_retry, raw_ingest_timestamp
from silver_transformations import BOROUGH_MAP

# A key that exists was missed on ~47% of single probes (measured 2026-08-26),
# so 10 probes keep false alarms near 0.05% per key.
SOURCE_PROBES      = 10
SOURCE_PROBE_PAUSE = 0.6

failures = []


def check(label, ok, detail=""):
    print(f"  {'✓' if ok else '✗'} {label}" + (f"  [{detail}]" if detail else ""))
    if not ok:
        failures.append(f"{label}  {detail}")


def parse_ts(ts):
    try:
        return datetime.fromisoformat(ts.replace("Z", "")) if ts else None
    except (ValueError, AttributeError):
        return None


def std_borough(raw_value):
    return BOROUGH_MAP.get(str(raw_value or "").strip().upper(), "UNSPECIFIED")


# Every relation the three rungs below read, in build order so the message
# names the earliest missing layer first.
REQUIRED_RELATIONS = [
    ("bronze", "service_requests"),
    ("silver", "service_requests"),
    ("gold", "fct_service_requests"),
    ("gold", "fct_daily_volume"),
    ("gold", "dim_location"),
]


def _missing_tables(con) -> list:
    """Relations reconciliation needs that this database does not have.

    information_schema.tables covers views too, which matters: Bronze is a
    view over the raw file (ADR 014), not a materialised table.
    """
    present = {
        (s, t) for s, t in con.sql(
            "SELECT table_schema, table_name FROM information_schema.tables"
        ).fetchall()
    }
    return [f"{s}.{t}" for s, t in REQUIRED_RELATIONS if (s, t) not in present]


def main() -> int:
    if not RAW_FILE.exists() or not DUCKDB_PATH.exists():
        print("No pipeline artifacts found — run local_runner.py first.")
        return 1

    raw = json.load(open(RAW_FILE))
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    def one(q):
        return con.sql(q).fetchone()[0]

    # Preflight: a database left by a run that failed in dbt has no Gold.
    missing = _missing_tables(con)
    if missing:
        print("Cannot reconcile — the pipeline has not finished building.")
        print(f"  missing: {', '.join(missing)}")
        print("  The database exists but is incomplete, which usually means the")
        print("  last run failed before or during stage 4 (dbt build).")
        print("  Build the missing layers, then re-run this check:")
        print("      python local/local_runner.py --live    # or --rows N")
        print("      python local/reconcile.py")
        return 1

    # Preflight: a raw file newer than Silver would show up as data mismatches.
    if one("SELECT max(_ingest_timestamp) FROM silver.service_requests") != raw_ingest_timestamp():
        print("Cannot reconcile — the raw file changed after Silver was built.")
        print("  Rebuild stages 2-5 from the current raw file, then re-run this check:")
        print("      python local/local_runner.py --stage 2")
        print("      python local/reconcile.py")
        return 1

    # One row per unique_key, last copy wins, as in Silver (every row of a run
    # shares one ingest timestamp, so the later fetch wins the tie).
    by_key = {r["unique_key"]: r for r in raw if r.get("unique_key") is not None}

    print("── Rung 1: conservation across layers ──────────────────────────")
    n_bronze = one("SELECT count(*) FROM bronze.service_requests")
    n_silver = one("SELECT count(*) FROM silver.service_requests")
    n_fct = one("SELECT count(*) FROM gold.fct_service_requests")
    n_dv = one("SELECT coalesce(sum(total_requests), 0) FROM gold.fct_daily_volume")

    # Quarantine definition is CLOCK-time inversion (closed strictly before
    # created), not calendar-day: a record closed 09:00 and created 10:00 the
    # same day is still a data error even though its calendar-day diff is 0.
    # (Resolution-days in Gold, by contrast, is calendar-day — see rung 2.)
    quarantined = sum(
        1 for r in by_key.values()
        for c1, c2 in [(parse_ts(r.get("created_date")), parse_ts(r.get("closed_date")))]
        if c1 and c2 and c2 < c1
    )
    check("raw file = bronze", len(raw) == n_bronze, f"{len(raw):,} vs {n_bronze:,}")
    check("silver = deduped raw - quarantined",
          n_silver == len(by_key) - quarantined,
          f"{n_silver:,} vs {len(by_key):,} - {quarantined}")

    # Gold accumulates history; Silver holds only the current window. So check
    # containment, and equality inside the window, not total equality.
    window_lo, window_hi = con.sql(
        "SELECT min(cast(created_date AS date)), max(cast(created_date AS date)) "
        "FROM silver.service_requests"
    ).fetchone()
    n_gold_window = one(
        f"SELECT count(*) FROM gold.fct_service_requests "
        f"WHERE cast(created_date AS date) BETWEEN '{window_lo}' AND '{window_hi}'"
    )
    n_missing = one(
        "SELECT count(*) FROM silver.service_requests s "
        "WHERE NOT EXISTS (SELECT 1 FROM gold.fct_service_requests g "
        "                  WHERE g.unique_key = s.unique_key)"
    )
    check("gold contains every silver row", n_missing == 0, f"{n_missing:,} missing")

    # Inside the window Gold and Silver must match exactly: rows Silver
    # quarantines are deleted from Gold by fct_service_requests' stg_quarantine
    # post_hook.
    check("gold within the fetch window = silver",
          n_gold_window == n_silver,
          f"{n_gold_window:,} vs {n_silver:,}")

    n_gold_history = n_fct - n_gold_window
    print(f"  · gold retains {n_gold_history:,} rows older than the window "
          f"({window_lo} → {window_hi}) — intentional history, not drift")

    check("daily_volume sums to the fact grain", int(n_dv) == n_fct,
          f"{int(n_dv):,} vs {n_fct:,}")

    print("── Rung 2: independent recompute from raw JSON ─────────────────")
    gold_keys = {r[0] for r in con.sql(
        "SELECT unique_key FROM gold.fct_service_requests").fetchall()}
    src = {k: r for k, r in by_key.items() if k in gold_keys}

    # Scope every Gold aggregate to the raw file's keys, the same rows src holds.
    IN_SCOPE = (f"f.unique_key IN (SELECT unique_key FROM "
                f"read_json_auto('{str(RAW_FILE)}'))")

    raw_closed = sum(1 for r in src.values() if r.get("status") == "Closed")
    gold_closed = one(
        f"SELECT count(*) FROM gold.fct_service_requests f "
        f"WHERE f.is_resolved AND {IN_SCOPE}")
    check("closed-request count", raw_closed == gold_closed,
          f"{raw_closed:,} vs {gold_closed:,}")

    raw_boro = Counter(std_borough(r.get("borough")) for r in src.values())
    gold_boro = dict(con.sql(f"""
        SELECT l.borough, count(*) FROM gold.fct_service_requests f
        JOIN gold.dim_location l USING (location_id)
        WHERE {IN_SCOPE} GROUP BY 1
    """).fetchall())
    boro_ok = all(raw_boro.get(b, 0) == n for b, n in gold_boro.items())
    check("borough distribution", boro_ok,
          f"{len(gold_boro)} values" if boro_ok else f"{dict(raw_boro)} vs {gold_boro}")

    gold_res = dict(con.sql("""
        SELECT unique_key, resolution_days FROM gold.fct_service_requests
        WHERE resolution_days IS NOT NULL
    """).fetchall())
    res_total = res_match = 0
    for k, r in src.items():
        c1, c2 = parse_ts(r.get("created_date")), parse_ts(r.get("closed_date"))
        if k in gold_res and c1 and c2:
            res_total += 1
            if gold_res[k] == (c2.date() - c1.date()).days:
                res_match += 1
    check("resolution_days per record (calendar-day defn)",
          res_total > 0 and res_match == res_total, f"{res_match:,}/{res_total:,}")

    # Exact timestamp equality — this is the check that catches offset shifts
    # even when interval metrics cancel them out.
    gold_created = dict(con.sql(
        "SELECT unique_key, created_date FROM gold.fct_service_requests").fetchall())
    ts_bad = sum(
        1 for k, r in src.items()
        if k in gold_created and parse_ts(r.get("created_date"))
        and gold_created[k] != parse_ts(r["created_date"])
    )
    check("created_date exact-timestamp match", ts_bad == 0,
          f"{ts_bad} of {len(src):,} differ" if ts_bad else f"all {len(src):,} rows")


    print("── Rung 3: live spot-check against the source API ──────────────")
    try:
        sample = [r[0] for r in con.sql(
            "SELECT unique_key FROM gold.fct_service_requests USING SAMPLE 3").fetchall()]

        # Socrata replicas disagree; a row seen on ANY probe exists. Absence is
        # reported only after every probe misses.
        def _lookup(key):
            for probe in range(1, SOURCE_PROBES + 1):
                try:
                    resp = _get_with_retry(requests.get, SOCRATA_URL,
                                           params={"$where": f"unique_key='{key}'"},
                                           timeout=30, what=f"reconcile lookup {key}")
                except RuntimeError as exc:        # retries exhausted
                    raise requests.ConnectionError(str(exc)) from exc
                rows = resp.json()
                if rows:
                    return rows[0], probe
                if probe < SOURCE_PROBES:
                    time.sleep(SOURCE_PROBE_PAUSE)
            return None, SOURCE_PROBES

        for k in sample:
            a, probes = _lookup(k)
            ours = con.sql(f"""
                SELECT f.complaint_type, l.borough, f.created_date
                FROM gold.fct_service_requests f
                JOIN gold.dim_location l USING (location_id)
                WHERE f.unique_key = '{k}'
            """).fetchone()
            if a is None:
                check(f"unique_key {k} exists at source", False,
                      f"absent on all {SOURCE_PROBES} probes")
                continue
            if probes > 1:
                print(f"  · {k} found on probe {probes}/{SOURCE_PROBES} "
                      f"— source replicas disagreed, not a pipeline fault")
            same = (ours[0] == a.get("complaint_type")
                    and ours[1] == std_borough(a.get("borough"))
                    and ours[2] == parse_ts(a.get("created_date")))
            check(f"unique_key {k} matches source", same,
                  "" if same else f"gold={ours} api={a.get('complaint_type'), a.get('borough'), a.get('created_date')}")
        # Mutable fields (status, closed_date) are excluded: the source may
        # legitimately be newer than our snapshot.
    except requests.RequestException as exc:
        # Only a network or HTTP error is a skip; anything else is a real failure.
        print(f"  ~ skipped (network unavailable: {type(exc).__name__}) — rungs 1–2 stand alone")
    except Exception as exc:
        check("rung 3 completed", False,
              f"{type(exc).__name__}: {exc} — NOT a network fault")

    print("─" * 64)
    if failures:
        print(f"RECONCILIATION FAILED — {len(failures)} mismatch(es):")
        for f in failures:
            print(f"  ✗ {f}")
        return 1
    print("Reconciled: the Gold layer agrees with the source.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
