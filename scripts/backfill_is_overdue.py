#!/usr/bin/env python3
"""Recompute is_overdue for fact rows written before the status-guard fix (#55).

WHY THIS EXISTS, AND WHY IT IS NOT A --full-refresh.

#55 corrected is_overdue to key on `status`, not on `resolution_days is null`:
the source emits rows carrying a closed_date while status is still Open /
In Progress / Assigned, and those rows were scoring is_overdue = FALSE — counted
as "on time" by the exact expression the three-valued design exists to protect.

The fix corrected FORWARD computation only. gold.fct_service_requests is
incremental, and it accumulates far beyond silver's rolling window: measured on
the 2026-08-29 CI database, the fact held 170,046 rows spanning 2026-08-12..28
while silver held 64,292 spanning 2026-08-22..28. All 3,241 violating rows sat
OUTSIDE silver, so no incremental run could ever revisit them — the daily build
failed on them indefinitely, and because the cache saves only on success, each
run restored the same poisoned database and failed again. A self-sustaining loop.

`dbt build --full-refresh` is the obvious repair and it is WRONG here: it
rebuilds the fact from silver and would delete the 105,754 rows silver no longer
carries — 62% of accumulated history — to fix 3,241.

is_overdue is a pure function of two columns already stored on the fact, so it
can be recomputed in place with no source data. That is what this does.

The CASE below is a deliberate duplicate of the one in
models/marts/fct_service_requests.sql. That duplication is a drift risk, so it
is covered two ways: assert_is_overdue_null_while_open fails if the stored
column ever disagrees with the rule, and test_backfill_matches_model_rule
compares this file's SQL against the model's line by line.

Idempotent: running it twice changes nothing the second time.
"""

import argparse
import os
import sys

import duckdb

# Must stay character-identical to the CASE in
# models/marts/fct_service_requests.sql. tests/test_backfill_is_overdue.py
# enforces that; do not edit one without the other.
IS_OVERDUE_RULE = """
        case
            when status <> 'Closed'      then null
            when resolution_days is null then null
            when resolution_days > 30    then true
            else false
        end
"""

VIOLATION_SQL = """
    select count(*) from gold.fct_service_requests
    where status <> 'Closed' and is_overdue is not null
"""


def rows_needing_backfill(con) -> int:
    """Rows whose stored is_overdue disagrees with the rule — the full blast
    radius, not just the ones the singular test happens to look for."""
    return con.sql(f"""
        select count(*) from gold.fct_service_requests
        where is_overdue is distinct from ({IS_OVERDUE_RULE})
    """).fetchone()[0]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("database", nargs="?", default="local/data/nyc311_local.duckdb")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change and exit without writing")
    args = ap.parse_args()

    if not os.path.exists(args.database):
        print(f"  no database at {args.database}", file=sys.stderr)
        return 2

    con = duckdb.connect(args.database, read_only=args.dry_run)

    before_total = con.sql("select count(*) from gold.fct_service_requests").fetchone()[0]
    before_viol = con.sql(VIOLATION_SQL).fetchone()[0]
    stale = rows_needing_backfill(con)

    print(f"  fact rows                       {before_total:,}")
    print(f"  disagree with the is_overdue rule  {stale:,}")
    print(f"  of which violate the NULL contract {before_viol:,}")

    if stale == 0:
        print("  nothing to backfill.")
        return 0

    if args.dry_run:
        print("  --dry-run: no changes written.")
        return 0

    con.execute(f"""
        update gold.fct_service_requests
        set is_overdue = ({IS_OVERDUE_RULE})
        where is_overdue is distinct from ({IS_OVERDUE_RULE})
    """)

    after_total = con.sql("select count(*) from gold.fct_service_requests").fetchone()[0]
    after_viol = con.sql(VIOLATION_SQL).fetchone()[0]
    after_stale = rows_needing_backfill(con)
    con.close()

    print(f"  backfilled                      {stale:,}")
    print(f"  fact rows after                 {after_total:,}"
          f"  ({'unchanged' if after_total == before_total else 'CHANGED — INVESTIGATE'})")
    print(f"  violations after                {after_viol:,}")
    print(f"  still disagreeing after         {after_stale:,}")

    if after_total != before_total:
        print("  ROW COUNT CHANGED — a backfill must never add or drop rows.", file=sys.stderr)
        return 1
    if after_viol or after_stale:
        print("  backfill did not converge.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
