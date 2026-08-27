#!/usr/bin/env python3
"""
check_upstream_stall.py — WARNING indicator, deliberately not an SLO gate.

Answers "is the city still publishing, and publishing normally?" — the question
SLO-2 does not ask, because SLO-2 asks whether WE loaded what the city
published. A city publishing outage is not our pipeline's failure, so the run
stays green; but it must stay VISIBLE, because analysts reading the dashboards
need to know the data stops short. The daily-run workflow turns a stall verdict
into a labeled GitHub issue (upstream-stall) while the run itself stays green.

WHY THIS FILE WAS REWRITTEN (2026-08-27). It used to compare yesterday's row
count in OUR fact table against a trailing 7-day median of our own counts. Both
halves were wrong:

  * "Yesterday" is never a whole day at the source. The publish lag means
    yesterday holds its first ~2 hours or nothing at all, so a healthy run
    scored ~358 against a ~10,500 median — 3.4% of the floor's 40%. The check
    therefore fired on 100% of healthy runs; issue #40 was commented every day
    from 2026-08-20. A daily alert that cannot stay quiet discriminates
    nothing, which is the exact argument ADR 013 makes against signals nobody
    can act on — applied here to the signal ADR 013 chose to keep.
  * It compared our counts against our counts, so it could never see the
    source at all. A day we loaded thinly and a day the city published thinly
    were the same number to it.

Both are fixed by the same population change SLO-2 makes: judge the newest day
the LOAD shows as COMPLETE (int_load_completeness — clock coverage, not a row
threshold), and compare SOURCE counts, captured per day into
silver.source_counts by local_runner.fetch_source_counts_window.

Two conditions, either of which warns:

  STALENESS — the newest complete day is more than MAX_COMPLETE_DAY_LAG_DAYS
    behind today (UTC). On a normal day the run at 10:00 UTC sees yesterday as
    a partial day and the day before as complete, so 2 is the healthy value and
    3+ means the source missed a publish cycle. Measured 2026-08-27: newest
    complete day 3 behind, with the last publish 1.4h old and carrying nothing
    new — the shape of the 2026-08-18 stall. Stated plainly: this rests on few
    observations of "normal", and it is a warning precisely so that being
    somewhat wrong about the threshold costs a notification and not a red run.

  VOLUME — the newest complete day's SOURCE count below VOLUME_FLOOR of the
    median source count of the other complete days in the window. This is the
    old volume cliff, moved onto a real day and onto the source's own numbers.
    The floor sits under NYC 311's natural ~50-60% weekend/holiday troughs. It
    covers the partial-stall gap ADR 013 recorded as a known limit: a day the
    city publishes to midnight but only half fills.

Relationship to ADR 013, which rejected a source-freshness SLO partly as
REDUNDANT with this check: the redundancy argument assumed this check worked.
It did not. Staleness of the complete-day horizon is now the thing this check
measures, so the argument is restored rather than contradicted — and it is
still a warning, not a gate. See ADR 015.

Exit code is 0 in both verdicts (warning, not gate). The verdict is emitted
as `stall=true|false` to $GITHUB_OUTPUT when present (workflow consumption)
and always printed for humans.

Usage:  python scripts/check_upstream_stall.py [db_path] [--report path.md]
"""

import os
import sys

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB = os.path.join(ROOT, "local", "data", "nyc311_local.duckdb")

# See the module docstring for how both numbers were arrived at.
MAX_COMPLETE_DAY_LAG_DAYS = 2
VOLUME_FLOOR = 0.40

# `current_date` is the SESSION's date, which on a non-UTC laptop is a day
# behind the UTC day the source and the capture both use. The scheduled run is
# UTC so it never noticed; taken AT TIME ZONE 'UTC' it is right everywhere.
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
        # No day in the loaded window is fully published. SLO-2 fails closed on
        # this too (it cannot measure); here it is simply the strongest stall
        # signal available.
        reasons.append("no complete day in the loaded window")
    elif row["days_behind"] is not None and row["days_behind"] > row["max_days_behind"]:
        reasons.append(
            f"newest complete day {row['newest_complete_day']} is "
            f"{row['days_behind']} days behind (max {row['max_days_behind']})"
        )
    # volume_ok is NULL when there is no prior complete day to compare against
    # — a one-day window, or a database with no captured source counts. NULL is
    # NOT a stall here: "we cannot compare" is not evidence of a cliff, and the
    # no-data case is already caught above.
    if row["volume_ok"] is False:
        reasons.append(
            f"source published {row['source_rows_newest_complete_day']} rows for "
            f"{row['newest_complete_day']} vs a median of "
            f"{row['median_prior_complete_days']} (floor {row['volume_floor']})"
        )
    return bool(reasons), reasons


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    db_path = args[0] if args else DEFAULT_DB
    report_path = None
    if "--report" in sys.argv:
        report_path = sys.argv[sys.argv.index("--report") + 1]

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
