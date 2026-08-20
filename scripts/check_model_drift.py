#!/usr/bin/env python3
"""
check_model_drift.py — CI guard for the dbt/ ↔ local/ model mirror.

local/ is a hand-synced DuckDB mirror of the Snowflake dbt project — the
duplication that makes the behavioral test tier possible, and the same
numbers-in-two-places hazard the README markers solved for counts: edit one
side, forget the other, and nothing notices.

Same remedy as scripts/check_claims.py — a single machine-readable source of
truth plus a CI check. Every INTENTIONAL divergence between the two projects
(dialect differences: dayofweekiso vs isodow, merge vs delete+insert, files
that exist on one side only) is registered in model_drift_baseline.json.
This script recomputes the actual divergence and fails when it no longer
matches the register — i.e., when someone changed one side without the
other, or changed both sides in a way that alters the registered dialect gap.

The comparison is content-only (the +/- lines of a unified diff, no hunk
headers), so editing BOTH sides identically shifts nothing, while any
one-sided edit changes the divergence and fails the build.

Run:    python scripts/check_model_drift.py            # verify
        python scripts/check_model_drift.py --update   # re-register after an
                                                       # intentional change
                                                       # (reviewed like any diff)
"""

import difflib
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASELINE = os.path.join(ROOT, "scripts", "model_drift_baseline.json")

# Mirrored trees, relative to dbt/ and local/ respectively.
# "tests" is included so one-sided edits to the singular Gold-integrity tests
# are caught — they were invisible to this gate before (found in audit).
SUBDIRS = ("models", "snapshots", "macros", "tests")
EXTS = (".sql", ".yml")

# Project-root files mirrored by relative name. Covers materialization
# configs, vars, and package version pins — a one-sided change to any of
# these diverges the projects just as surely as a model edit.
ROOT_FILES = ("dbt_project.yml", "packages.yml", "package-lock.yml")


def collect(side: str) -> dict:
    """{relative_path: content} for all mirrored files on one side."""
    out = {}
    for sub in SUBDIRS:
        base = os.path.join(ROOT, side, sub)
        for dirpath, _, files in os.walk(base):
            for f in sorted(files):
                if f.endswith(EXTS):
                    full = os.path.join(dirpath, f)
                    rel = os.path.relpath(full, os.path.join(ROOT, side))
                    out[rel] = open(full).read()
    for fname in ROOT_FILES:
        full = os.path.join(ROOT, side, fname)
        if os.path.exists(full):
            out[fname] = open(full).read()
    return out


def divergence(dbt_text: str, local_text: str) -> str:
    """Content-only diff: the +/- lines, without positional hunk headers.

    Headers are excluded by exact sentinel label, NOT by '+++'/'---' prefix:
    a removed SQL comment line diffs as '-' + '--' = '---...', and a prefix
    filter silently discards it — which made the first version of this
    checker blind to one-sided comment edits in SQL files.
    """
    lines = difflib.unified_diff(
        dbt_text.splitlines(), local_text.splitlines(),
        fromfile="::dbt::", tofile="::local::", lineterm="", n=0,
    )
    return "\n".join(
        ln for ln in lines
        if not ln.startswith(("@@", "--- ::dbt::", "+++ ::local::"))
    )


def current_state() -> dict:
    dbt_files, local_files = collect("dbt"), collect("local")
    return {
        "only_in_dbt": sorted(set(dbt_files) - set(local_files)),
        "only_in_local": sorted(set(local_files) - set(dbt_files)),
        "pairs": {
            rel: divergence(dbt_files[rel], local_files[rel])
            for rel in sorted(set(dbt_files) & set(local_files))
        },
    }


def main() -> int:
    state = current_state()

    if "--update" in sys.argv:
        # Loud summary of exactly what the update absorbs, so a reviewer of
        # the baseline commit sees the changed files without decoding the
        # JSON diff. --update is the gate's designed bypass; this printout is
        # the audit trail that keeps it reviewable.
        # The summary is best-effort: an unreadable baseline (corrupt, or
        # carrying conflict markers mid-rebase) must NOT stop the rewrite —
        # that is precisely the situation where regenerating is the fix. An
        # earlier version raised JSONDecodeError here and left the broken
        # file in place, which then got committed.
        old = None
        if os.path.exists(BASELINE):
            try:
                old = json.load(open(BASELINE))
            except (json.JSONDecodeError, OSError) as exc:
                print(f"Existing baseline is unreadable ({exc.__class__.__name__}) — "
                      "regenerating from the working tree; no change summary available.")
        if old is not None:
            changed = sorted(
                rel for rel in set(state["pairs"]) | set(old.get("pairs", {}))
                if state["pairs"].get(rel) != old.get("pairs", {}).get(rel)
            )
            for key in ("only_in_dbt", "only_in_local"):
                for f in sorted(set(state[key]) ^ set(old.get(key, []))):
                    changed.append(f"{f} ({key} membership changed)")
            if changed:
                print("Re-registering divergence for:")
                for rel in changed:
                    print(f"  ~ {rel}")
            else:
                print("No divergence changes — baseline rewritten unchanged.")
        with open(BASELINE, "w") as fh:
            json.dump(state, fh, indent=2)
        print(f"Baseline re-registered: {len(state['pairs'])} mirrored pairs, "
              f"{len(state['only_in_dbt'])} dbt-only, "
              f"{len(state['only_in_local'])} local-only files.")
        return 0

    if not os.path.exists(BASELINE):
        print("No baseline registered — run with --update first.")
        return 1

    baseline = json.load(open(BASELINE))
    errors = []

    for key in ("only_in_dbt", "only_in_local"):
        added = set(state[key]) - set(baseline[key])
        gone = set(baseline[key]) - set(state[key])
        for f in sorted(added):
            errors.append(f"{f}: now exists on one side only ({key}) — unregistered")
        for f in sorted(gone):
            errors.append(f"{f}: was registered {key} but that is no longer true")

    all_pairs = set(state["pairs"]) | set(baseline["pairs"])
    for rel in sorted(all_pairs):
        cur = state["pairs"].get(rel)
        reg = baseline["pairs"].get(rel)
        if cur is None or reg is None:
            continue  # membership change already reported above
        if cur != reg:
            delta = "\n".join(
                f"      {ln}" for ln in difflib.unified_diff(
                    reg.splitlines(), cur.splitlines(),
                    fromfile="registered divergence", tofile="current divergence",
                    lineterm="", n=0)
            )
            errors.append(
                f"{rel}: divergence between dbt/ and local/ changed — one side "
                f"was edited without the other (or the dialect gap moved):\n{delta}"
            )

    if errors:
        print("MODEL DRIFT DETECTED between dbt/ and local/:")
        for e in errors:
            print(f"  ✗ {e}")
        print("\nIf the change is intentional on BOTH sides, re-register with:"
              "\n    python scripts/check_model_drift.py --update")
        return 1

    print(f"  ✓ {len(state['pairs'])} mirrored files match their registered divergence")
    print(f"  ✓ one-sided file sets unchanged "
          f"({len(state['only_in_dbt'])} dbt-only, {len(state['only_in_local'])} local-only)")
    print("Model mirror check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
