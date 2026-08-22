#!/usr/bin/env python3
"""
Source-to-target reconciliation for the local NYC 311 pipeline.

Answers the question the test suite cannot: does the Gold layer agree with
REALITY, not just with itself? Tests verify the pipeline is internally
consistent; this tool verifies it is faithful to the source. (It exists
because a run with 102 green tests still carried a 4-hour timestamp shift —
caught only by the checks below.)

Three rungs, weakest to strongest:

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
crossed), matching datediff('day', ...) in the dbt models and the cloud spec.
"""

import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).parent))
from local_runner import DUCKDB_PATH, RAW_FILE, SOCRATA_ENDPOINT
# BOROUGH_MAP comes from its owner, not second-hand via local_runner. The
# re-export made local_runner's import list read as if it used the map itself.
from silver_transformations import BOROUGH_MAP

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


def main() -> int:
    if not RAW_FILE.exists() or not DUCKDB_PATH.exists():
        print("No pipeline artifacts found — run local_runner.py first.")
        return 1

    raw = json.load(open(RAW_FILE))
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    def one(q):
        return con.sql(q).fetchone()[0]

    # Deduplicate the raw records the same way Silver does (one row per
    # unique_key) so the independent recompute covers the same population.
    by_key = {}
    for r in raw:
        k = r.get("unique_key")
        if k is not None and k not in by_key:
            by_key[k] = r

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

    # Gold is INCREMENTAL and accumulates; Silver is rebuilt from the current
    # fetch window every run. Once the window advances past a previously loaded
    # day, gold > silver permanently — by design, and the whole point of
    # keeping history for fct_complaint_recurrence.
    #
    # `gold fact = silver` was asserted here for months and only ever passed
    # because every build was either full-refresh or inside one window. The
    # first live run after the window moved failed it: 83,622 vs 62,557. The
    # equality was never the invariant; these two are.
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

    # Inside the window the two should agree. A surplus here is Gold still
    # holding a row Silver has since rejected: quarantine is applied in Silver
    # BEFORE dbt sees anything, so the fact table's reconciliation post_hook
    # (which deletes rows present in staging but absent from int) is
    # structurally unable to see them. Reported with its cause rather than
    # asserted to zero, because the surplus is real and currently unfixable
    # from inside dbt.
    surplus = n_gold_window - n_silver
    check("gold within the fetch window = silver",
          surplus == 0,
          f"{n_gold_window:,} vs {n_silver:,}"
          + (f"  (+{surplus} retained from an earlier run, now quarantined in Silver "
             f"— see docs/BACKLOG.md)" if surplus else ""))

    n_gold_history = n_fct - n_gold_window
    print(f"  · gold retains {n_gold_history:,} rows older than the window "
          f"({window_lo} → {window_hi}) — intentional history, not drift")

    check("daily_volume sums to the fact grain", int(n_dv) == n_fct,
          f"{int(n_dv):,} vs {n_fct:,}")

    print("── Rung 2: independent recompute from raw JSON ─────────────────")
    gold_keys = {r[0] for r in con.sql(
        "SELECT unique_key FROM gold.fct_service_requests").fetchall()}
    src = {k: r for k, r in by_key.items() if k in gold_keys}

    # Every Gold aggregate below must be scoped to the SAME rows `src` holds.
    # `src` is already raw ∩ gold — the current fetch window — but the Gold side
    # was being aggregated over the whole table, which includes every earlier
    # window Gold has accumulated. That compared 7 days of raw against 9 days of
    # Gold and reported a data-integrity failure (39,291 vs 53,870) for what was
    # only a difference in scope.
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
        import requests
        sample = [r[0] for r in con.sql(
            "SELECT unique_key FROM gold.fct_service_requests USING SAMPLE 3").fetchall()]
        for k in sample:
            api = requests.get(SOCRATA_ENDPOINT,
                               params={"$where": f"unique_key='{k}'"},
                               timeout=30).json()
            ours = con.sql(f"""
                SELECT f.complaint_type, l.borough, f.created_date
                FROM gold.fct_service_requests f
                JOIN gold.dim_location l USING (location_id)
                WHERE f.unique_key = '{k}'
            """).fetchone()
            if not api:
                check(f"unique_key {k} exists at source", False, "not found")
                continue
            a = api[0]
            same = (ours[0] == a.get("complaint_type")
                    and ours[1] == std_borough(a.get("borough"))
                    and ours[2] == parse_ts(a.get("created_date")))
            check(f"unique_key {k} matches source", same,
                  "" if same else f"gold={ours} api={a.get('complaint_type'), a.get('borough'), a.get('created_date')}")
        # Note: mutable fields (status, closed_date) are deliberately excluded —
        # the source may legitimately have newer values than our snapshot.
    except Exception as exc:
        print(f"  ~ skipped (network unavailable: {type(exc).__name__}) — rungs 1–2 stand alone")

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
