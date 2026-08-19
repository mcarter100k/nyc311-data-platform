#!/usr/bin/env python3
"""
check_upstream_stall.py — WARNING indicator, deliberately not an SLO gate.

Detects the volume cliff that used to be SLO-2's job: yesterday's
created-request count falling below 40% of the trailing 7-day median. Since
SLO-2 became a source reconciliation (are WE missing anything the city
published?), this cliff no longer reddens the run — a city publishing outage
is not our pipeline's failure — but it must stay VISIBLE, because analysts
reading the dashboards need to know yesterday's data is incomplete at the
source. The daily-run workflow turns a stall verdict into a labeled GitHub
issue (upstream-stall) while the run itself stays green.

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

# The former SLO-2 query, verbatim in spirit: floor-only trailing-median
# comparison, 0.40 sits below NYC 311's natural weekend/holiday troughs
# (~50-60% of weekly median). See docs/SLO.md "Upstream stall warning".
QUERY = """
WITH daily AS (
    SELECT cast(created_date AS date) AS day, count(*) AS n
    FROM gold.fct_service_requests
    WHERE cast(created_date AS date) BETWEEN current_date - 8 AND current_date - 2
    GROUP BY 1
),
yesterday AS (
    SELECT count(*) AS n
    FROM gold.fct_service_requests
    WHERE cast(created_date AS date) = current_date - 1
)
SELECT
    (SELECT n FROM yesterday)                                           AS rows_yesterday,
    (SELECT median(n) FROM daily)                                       AS median_prior_7d,
    0.40                                                                AS floor,
    (SELECT n FROM yesterday) >= 0.40 * (SELECT median(n) FROM daily)   AS volume_ok
"""


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    db_path = args[0] if args else DEFAULT_DB
    report_path = None
    if "--report" in sys.argv:
        report_path = sys.argv[sys.argv.index("--report") + 1]

    con = duckdb.connect(db_path, read_only=True)
    row = con.sql(QUERY).fetchone()
    rows_yesterday, median_7d, floor, volume_ok = row
    # NULL volume_ok (empty history) is treated as a stall: no data at all is
    # the strongest possible signal that something upstream is not arriving.
    stall = volume_ok is not True

    verdict = "UPSTREAM STALL SUSPECTED" if stall else "upstream volume normal"
    line = (f"rows_yesterday={rows_yesterday}  median_prior_7d={median_7d}  "
            f"floor={floor}")
    print(f"  {'!' if stall else '✓'} {verdict}: {line}")

    if report_path:
        with open(report_path, "w") as fh:
            fh.write(
                f"# Upstream publish volume check\n\n"
                f"- **Verdict:** {verdict}\n"
                f"- {line}\n\n"
                f"This is a WARNING, not an SLO breach: SLO-2 confirmed we loaded "
                f"everything the source published (see the run's SLO report). The "
                f"volume cliff points at the city's feed, not this pipeline. "
                f"Recovery is automatic while the gap stays inside the trailing "
                f"7-day fetch window; beyond that, unpublished days are "
                f"unrecoverable by the daily run.\n"
            )

    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a") as fh:
            fh.write(f"stall={'true' if stall else 'false'}\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
