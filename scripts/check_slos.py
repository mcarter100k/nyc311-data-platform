#!/usr/bin/env python3
"""
check_slos.py — evaluate the service commitments in docs/SLO.md.

Runs every query in scripts/slo/*.sql against a DuckDB file. Those files are
the single executable form of the SLO contract; docs/SLO.md reproduces them
for reading, and scripts/check_claims.py fails the build if the copies drift.

Each query's contract with this script: return exactly one row containing a
boolean `pass` column plus whatever evidence columns explain the verdict.
Anything other than pass=True — including NULL from an empty table — is a
breach.

Usage:  python scripts/check_slos.py [db_path] [--report path.md]

Exit 0 = all SLOs met; exit 1 = breach or empty result. The report file is
written either way — the daily-run workflow attaches it to breach issues so
the issue carries the actual numbers, not a paraphrase.
"""

import argparse
import glob
import os
import sys

import duckdb

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DB = os.path.join(ROOT, "local", "data", "nyc311_local.duckdb")
SLO_DIR = os.path.join(ROOT, "scripts", "slo")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Evaluate scripts/slo/*.sql against a DuckDB file.")
    ap.add_argument("db_path", nargs="?", default=DEFAULT_DB)
    ap.add_argument("--report", help="also write the results to this markdown file")
    args = ap.parse_args(argv)
    db_path, report_path = args.db_path, args.report

    con = duckdb.connect(db_path, read_only=True)
    breaches = []
    report = ["# SLO check", f"database: `{db_path}`", ""]

    sql_files = sorted(glob.glob(os.path.join(SLO_DIR, "*.sql")))
    # Zero SLO queries found is a breach of the gate, not a pass.
    if not sql_files:
        print(f"SLO GATE ERROR: no SLO queries found in {SLO_DIR} — "
              "refusing to pass with zero checks evaluated.")
        return 1

    for sql_file in sql_files:
        name = os.path.basename(sql_file)
        with open(sql_file) as fh:
            rel = con.sql(fh.read())
        cols = rel.columns
        row = rel.fetchone()
        values = dict(zip(cols, row, strict=True)) if row else {}
        ok = values.get("pass") is True  # NULL and missing both count as breach

        line = "  ".join(f"{c}={values.get(c)}" for c in cols)
        print(f"  {'✓' if ok else '✗'} {name}: {line}")
        report.append(f"- {'PASS' if ok else '**BREACH**'} `{name}`: {line}")
        if not ok:
            breaches.append(name)

    if report_path:
        with open(report_path, "w") as fh:
            fh.write("\n".join(report) + "\n")

    if breaches:
        print(f"SLO BREACH: {', '.join(breaches)}")
        return 1
    print("All SLOs met.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
