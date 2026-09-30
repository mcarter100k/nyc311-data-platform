#!/usr/bin/env python3
"""
check_upstream_stall.py — WARNING indicator, deliberately not an SLO gate.

Answers "is the city still publishing, and publishing normally?" — the question
SLO-2 does not ask, because SLO-2 asks whether WE loaded what the city
published. A city publishing outage is not our pipeline's failure, so the run
stays green; but it must stay VISIBLE, because analysts reading the dashboards
need to know the data stops short. The daily-run workflow turns a stall verdict
into a labeled GitHub issue (upstream-stall) while the run itself stays green.

It judges the newest day the load shows as COMPLETE (int_load_completeness)
against SOURCE counts (silver.source_counts), because yesterday is never a
whole day at the source and our own counts cannot see the source (ADR 015).
Either condition warns:

  STALENESS — the newest complete day is more than MAX_COMPLETE_DAY_LAG_DAYS
    behind today (UTC). At the 10:00 UTC run yesterday is partial and the day
    before is complete, so 2 is healthy and 3+ means a missed publish cycle.
    Few observations back this number, which is why it warns rather than gates.

  VOLUME — the newest complete day's source count is below VOLUME_FLOOR of
    the median of the other complete days. The floor sits under NYC 311's
    ~50-60% weekend/holiday troughs; it catches a day the city publishes to
    midnight but only part-fills.

Exit code is 0 either way. The verdict goes to $GITHUB_OUTPUT as
`stall=true|false` when that is set, and is always printed.

Usage:  python scripts/check_upstream_stall.py [db_path] [--report path.md]
"""

import argparse
import os
import sys

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB = os.path.join(ROOT, "local", "data", "nyc311_local.duckdb")

MAX_COMPLETE_DAY_LAG_DAYS = 2
VOLUME_FLOOR = 0.40

# "Today" is taken AT TIME ZONE 'UTC': current_date is the session's date,
# which on a non-UTC laptop differs from the UTC day the source uses.
QUERY = f"""
WITH complete AS (
    SELECT load_day
    FROM gold.int_load_completeness
    WHERE is_complete_day
),
horizon AS (
    SELECT max(load_day) AS newest_complete_day FROM complete
),
source AS (
    SELECT s.target_date AS day, s.source_count AS n
    FROM silver.source_counts s
    JOIN complete c ON c.load_day = s.target_date
)
SELECT
    (SELECT newest_complete_day FROM horizon)                       AS newest_complete_day,
    date_diff('day',
              (SELECT newest_complete_day FROM horizon),
              cast(current_timestamp AT TIME ZONE 'UTC' AS date))   AS days_behind,
    {MAX_COMPLETE_DAY_LAG_DAYS}                                     AS max_days_behind,
    (SELECT n FROM source
      WHERE day = (SELECT newest_complete_day FROM horizon))        AS source_rows_newest_complete_day,
    (SELECT median(n) FROM source
      WHERE day < (SELECT newest_complete_day FROM horizon))        AS median_prior_complete_days,
    {VOLUME_FLOOR}                                                  AS volume_floor,
    (SELECT n FROM source
      WHERE day = (SELECT newest_complete_day FROM horizon))
      >= {VOLUME_FLOOR} * (SELECT median(n) FROM source
                            WHERE day < (SELECT newest_complete_day FROM horizon))
                                                                    AS volume_ok
"""


def verdict(row: dict) -> tuple[bool, list[str]]:
    """(stall, reasons). Split out from main so it is unit-testable."""
    reasons = []
    if row["newest_complete_day"] is None:
        reasons.append("no complete day in the loaded window")
    elif row["days_behind"] is not None and row["days_behind"] > row["max_days_behind"]:
        reasons.append(
            f"newest complete day {row['newest_complete_day']} is "
            f"{row['days_behind']} days behind (max {row['max_days_behind']})"
        )
    # volume_ok is NULL with nothing to compare against; that is not a stall.
    if row["volume_ok"] is False:
        reasons.append(
            f"source published {row['source_rows_newest_complete_day']} rows for "
            f"{row['newest_complete_day']} vs a median of "
            f"{row['median_prior_complete_days']} (floor {row['volume_floor']})"
        )
    return bool(reasons), reasons


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Warn when the city stops publishing normally.")
    ap.add_argument("db_path", nargs="?", default=DEFAULT_DB)
    ap.add_argument("--report", help="also write the verdict to this markdown file")
    args = ap.parse_args(argv)
    db_path, report_path = args.db_path, args.report

    con = duckdb.connect(db_path, read_only=True)
    rel = con.sql(QUERY)
    row = dict(zip(rel.columns, rel.fetchone(), strict=True))

    stall, reasons = verdict(row)
    label = "UPSTREAM STALL SUSPECTED" if stall else "upstream publishing normal"
    line = "  ".join(f"{k}={v}" for k, v in row.items())
    print(f"  {'!' if stall else '✓'} {label}: {line}")
    for r in reasons:
        print(f"      → {r}")

    if report_path:
        with open(report_path, "w") as fh:
            fh.write(
                f"# Upstream publishing check\n\n"
                f"- **Verdict:** {label}\n"
                + "".join(f"- Reason: {r}\n" for r in reasons)
                + f"- {line}\n\n"
                f"This is a WARNING, not an SLO breach: SLO-2 separately confirms "
                f"we loaded everything the source published for every day the load "
                f"shows as complete (see the run's SLO report). This check looks at "
                f"the CITY's publishing — how far behind its newest complete day is, "
                f"and whether that day's volume collapsed. Recovery is automatic "
                f"while the gap stays inside the trailing fetch window, and a day "
                f"that fills in later is re-reconciled by SLO-2 on the next run; a "
                f"day the city never publishes within that window is unrecoverable "
                f"by the daily run.\n"
            )

    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a") as fh:
            fh.write(f"stall={'true' if stall else 'false'}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
