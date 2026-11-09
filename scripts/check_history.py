#!/usr/bin/env python3
"""
check_history.py — WARNING indicator: did this run build on the previous database?

Gold's history exists only because each daily run restores the previous run's
DuckDB from the GitHub Actions cache (ADR 017). If that cache entry is gone,
the run starts from an empty file, rebuilds the last 37 days, and passes both
SLOs: SLO-1 sees fresh data and SLO-2 reconciles every complete day it loaded.
Nothing else would notice the lost history. This check does.

Two commands, run around the pipeline step:

  snapshot DB --out before.json   record what the restored database held,
                                  or that no database was restored
  compare before.json DB          warn when no database was restored, when
                                  Gold lost more than ROW_DROP_TOLERANCE of its
                                  rows, or when its earliest created_date
                                  moved later

Exit code is 0 every time, including when the check itself breaks: a
warning-only step must never fail the run (a crash here stopped every daily
run from 2026-10-21 to 2026-11-08). The verdict goes to $GITHUB_OUTPUT as
`lost=true|false|error` when that is set, and is always printed; `error` means
the check could not run, and the workflow files an issue for it too. A
deliberate reset (deleting the database by hand) also warns; that is intended.

Usage:
  python scripts/check_history.py snapshot DB --out before.json
  python scripts/check_history.py compare before.json DB [--report path.md]
"""

import argparse
import json
import os
import sys

# Quarantine post-hooks can delete a few rows a run, so a small drop is
# normal; a lost cache drops Gold from every request since the first run to
# the fetch window alone (about 520k to 385k on 2026-09-30).
ROW_DROP_TOLERANCE = 0.01

QUERY = """
SELECT count(*)                          AS rows,
       min(created_date)::DATE::VARCHAR  AS first_day,
       max(created_date)::DATE::VARCHAR  AS last_day
FROM gold.fct_service_requests
"""


def snapshot(db_path: str) -> dict:
    """What the database at db_path holds, or {"exists": False} if there is none."""
    if not os.path.exists(db_path):
        return {"exists": False}
    import duckdb  # only here, so the structural tier can import verdict() without it

    con = duckdb.connect(db_path, read_only=True)
    try:
        rows, first_day, last_day = con.execute(QUERY).fetchone()
    except duckdb.CatalogException:  # a file with no Gold layer yet
        return {"exists": True, "rows": 0, "first_day": None, "last_day": None}
    finally:
        con.close()
    return {"exists": True, "rows": rows, "first_day": first_day, "last_day": last_day}


def verdict(before: dict, after: dict) -> tuple[bool, list[str]]:
    """(lost, reasons). Pure, so it is unit-testable without a database."""
    if not before.get("exists"):
        return True, [
            "no previous database was restored, so this run started from empty "
            "and Gold now holds only the fetch window"
        ]
    reasons = []
    if after["rows"] < before["rows"] * (1 - ROW_DROP_TOLERANCE):
        reasons.append(f"Gold has {after['rows']:,} rows, down from {before['rows']:,} before this run")
    if before.get("first_day") and after.get("first_day") and after["first_day"] > before["first_day"]:
        reasons.append(f"Gold's earliest request moved from {before['first_day']} to {after['first_day']}")
    return bool(reasons), reasons


def _output(lost: str) -> None:
    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a") as fh:
            fh.write(f"lost={lost}\n")


def main(argv: list[str] | None = None) -> int:
    try:
        return _main(argv)
    except Exception as exc:  # noqa: BLE001 - any failure is reported, never raised
        msg = f"history check could not run: {type(exc).__name__}: {exc}"
        print(f"::warning::{msg}")
        args = sys.argv[1:] if argv is None else argv
        report = args[args.index("--report") + 1] if "--report" in args[:-1] else None
        if report:
            with open(report, "w") as fh:
                fh.write(f"# History check\n\n- **Verdict:** COULD NOT RUN\n- {msg}\n\n"
                         "Whether history survived this run is unknown. Fix the check "
                         "(scripts/check_history.py); recovery is in ADR 017.\n")
        _output("error")
        return 0


def _main(argv: list[str] | None) -> int:
    ap = argparse.ArgumentParser(description="Warn when a run did not build on the previous database.")
    sub = ap.add_subparsers(dest="command", required=True)
    snap = sub.add_parser("snapshot")
    snap.add_argument("db_path")
    snap.add_argument("--out", required=True)
    comp = sub.add_parser("compare")
    comp.add_argument("before_json")
    comp.add_argument("db_path")
    comp.add_argument("--report")
    args = ap.parse_args(argv)

    if args.command == "snapshot":
        state = snapshot(args.db_path)
        with open(args.out, "w") as fh:
            json.dump(state, fh)
        print(f"  history before this run: {state}")
        return 0

    with open(args.before_json) as fh:
        before = json.load(fh)
    after = snapshot(args.db_path)
    lost, reasons = verdict(before, after)
    label = "HISTORY LOST" if lost else "history intact"
    print(f"  {'!' if lost else '✓'} {label}: before={before} after={after}")
    for r in reasons:
        print(f"      → {r}")

    if args.report:
        with open(args.report, "w") as fh:
            fh.write(
                "# History check\n\n"
                f"- **Verdict:** {label}\n"
                + "".join(f"- Reason: {r}\n" for r in reasons)
                + f"- Before this run: {before}\n- After this run: {after}\n\n"
                "This is a WARNING, not an SLO breach: the run loaded the fetch "
                "window correctly. What may be gone is older history, which lives "
                "only in the GitHub Actions cache. Recovery is in ADR 017.\n"
            )

    _output("true" if lost else "false")
    return 0


if __name__ == "__main__":
    sys.exit(main())
