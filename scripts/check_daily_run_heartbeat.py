#!/usr/bin/env python3
"""
check_daily_run_heartbeat.py — watches the watcher.

scripts/check_slos.py measures the pipeline from INSIDE a run: it can only
report on a run that happened. The failure mode it is structurally blind to is
a run that never fires at all — and that is not hypothetical. GitHub disables
scheduled workflows on a public repo after 60 days without repository activity,
a maintainer can disable one by hand, and cron delivery is best-effort with no
delivery guarantee. In every one of those cases the daily run is silent, the
Actions tab is green (nothing ran, nothing failed), and the data quietly ages.

This check runs on its own schedule and asks the Actions API two questions
about `daily-run.yml`:

  1. Is the workflow still ACTIVE? A `disabled_*` state means no future run
     will ever fire, so it is a breach immediately — regardless of how recent
     the last success is. This is the failure mode that motivates the check,
     and it is only visible from outside the workflow being watched.
  2. How long since it last CONCLUDED SUCCESSFULLY? Older than the threshold
     is a breach.

Threshold. The default is 26 hours — deliberately the same number as SLO-1
(docs/SLO.md: one daily cycle + 2h grace for run-time variance). SLO-1 measures
`max(_loaded_at)` age from inside the database; this measures wall-clock since
the last green run from outside. They are two views of one commitment — "a run
delivered rows within the last 26 hours" — so sharing the number means the
external watcher fires at exactly the moment the internal SLO would have, had
it been able to run. A smaller number would alert on a merely late run that
SLO-1 would still pass; a larger one would leave a window where the pipeline is
out of contract and nothing says so.

Any successful run ON THE DEFAULT BRANCH counts, scheduled or manually
dispatched: both write the DuckDB cache and both refresh `_loaded_at`, so both
discharge the freshness commitment. Counting only `event=schedule` would file a
breach against a repo whose data is provably fresh. The branch filter is not
cosmetic: Actions cache scoping means a run dispatched from a feature branch
saves into that branch's cache and never advances main's accumulated database,
so counting it would let someone testing this workflow on a branch silence the
alert for a day while main's data actually aged.

The verdict is the EXIT CODE (0 live, 1 breach, 2 the check itself broke), so
the calling workflow files its issue with a plain `if: failure()` — the same
shape daily-run.yml already uses. A markdown report is written for the issue
body whenever --report is given.

Usage:
    python scripts/check_daily_run_heartbeat.py \
        --repo owner/name --workflow daily-run.yml \
        --threshold-hours 26 --report heartbeat_report.md

    # Offline / testing: skip the API and read the two facts from a file.
    python scripts/check_daily_run_heartbeat.py --fixture facts.json --now 2026-08-27T12:00:00Z
"""

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone

DEFAULT_WORKFLOW = "daily-run.yml"
DEFAULT_THRESHOLD_HOURS = 26.0


@dataclass(frozen=True)
class Verdict:
    """The decision, separated from how the facts were obtained."""

    ok: bool
    code: str  # live | workflow-disabled | never-succeeded | stale
    headline: str
    age_hours: float | None


def parse_ts(value: str) -> datetime:
    """Parse a GitHub API timestamp ('2026-08-26T10:30:55Z') as aware UTC."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def evaluate(
    *,
    workflow_state: str,
    last_success_completed_at: str | None,
    now: datetime,
    threshold_hours: float = DEFAULT_THRESHOLD_HOURS,
) -> Verdict:
    """Pure decision function — no network, no clock, no environment.

    Every input is passed in, so the four branches below are directly
    exercisable by a test. Order matters: a disabled workflow is a breach even
    when its last success is minutes old, because 'disabled' is a statement
    about the FUTURE (no run will fire again) while freshness is a statement
    about the past.
    """
    if workflow_state != "active":
        return Verdict(
            ok=False,
            code="workflow-disabled",
            headline=(
                f"`{DEFAULT_WORKFLOW}` is **{workflow_state}**, not active — "
                f"no scheduled run will fire until it is re-enabled."
            ),
            age_hours=None,
        )

    if not last_success_completed_at:
        return Verdict(
            ok=False,
            code="never-succeeded",
            headline="The Actions API reports **no successful run at all** for this workflow.",
            age_hours=None,
        )

    age_hours = (now - parse_ts(last_success_completed_at)).total_seconds() / 3600.0

    if age_hours >= threshold_hours:
        return Verdict(
            ok=False,
            code="stale",
            headline=(
                f"Last successful run concluded **{age_hours:.1f}h** ago "
                f"({last_success_completed_at}) — threshold is {threshold_hours:g}h."
            ),
            age_hours=age_hours,
        )

    return Verdict(
        ok=True,
        code="live",
        headline=(
            f"Last successful run concluded {age_hours:.1f}h ago "
            f"({last_success_completed_at}), inside the {threshold_hours:g}h threshold."
        ),
        age_hours=age_hours,
    )


def gh_api(path: str) -> dict:
    """Read-only GitHub API call through the gh CLI (already on every runner)."""
    out = subprocess.run(
        ["gh", "api", "-H", "Accept: application/vnd.github+json", path],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(out.stdout)


def fetch_facts(repo: str, workflow: str, branch: str) -> tuple[str, str | None]:
    """Return (workflow_state, last_success_completed_at) from the Actions API.

    `updated_at` on the run is when it CONCLUDED; `created_at` is when it was
    queued. Freshness is a claim about when rows landed, so the conclusion
    timestamp is the correct one.
    """
    meta = gh_api(f"repos/{repo}/actions/workflows/{workflow}")
    runs = gh_api(
        f"repos/{repo}/actions/workflows/{workflow}/runs"
        f"?status=success&branch={branch}&per_page=1"
    )["workflow_runs"]
    return meta["state"], (runs[0]["updated_at"] if runs else None)


def render_report(
    verdict: Verdict, repo: str, workflow: str, branch: str, threshold_hours: float
) -> str:
    status = "alive" if verdict.ok else "NOT ALIVE"
    return (
        f"# Daily-run heartbeat — {status}\n\n"
        f"- **Verdict:** {verdict.code}\n"
        f"- {verdict.headline}\n"
        f"- Watched workflow: `{workflow}` on `{branch}` in `{repo}`\n"
        f"- Threshold: {threshold_hours:g}h (same number as SLO-1 freshness — "
        f"one daily cycle plus 2h grace)\n\n"
        f"This check reads the Actions API from outside the daily run, so it "
        f"still speaks when the daily run does not run at all. It cannot see "
        f"its own disablement: the 60-day inactivity rule disables every "
        f"scheduled workflow in the repository at once, this one included.\n"
    )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", ""))
    ap.add_argument("--workflow", default=DEFAULT_WORKFLOW)
    ap.add_argument("--branch", default="main", help="only count successes on this branch")
    ap.add_argument("--threshold-hours", type=float, default=DEFAULT_THRESHOLD_HOURS)
    ap.add_argument("--report", default=None)
    ap.add_argument("--now", default=None, help="ISO-8601 UTC override (testing/determinism)")
    ap.add_argument("--fixture", default=None, help="JSON file of facts; skips the API")
    args = ap.parse_args()

    now = parse_ts(args.now) if args.now else datetime.now(timezone.utc)

    if args.fixture:
        with open(args.fixture) as fh:
            facts = json.load(fh)
        state = facts["state"]
        last_success = facts.get("last_success_completed_at")
    else:
        if not args.repo:
            print("ERROR: --repo is required (or set GITHUB_REPOSITORY)", file=sys.stderr)
            return 2
        state, last_success = fetch_facts(args.repo, args.workflow, args.branch)

    verdict = evaluate(
        workflow_state=state,
        last_success_completed_at=last_success,
        now=now,
        threshold_hours=args.threshold_hours,
    )

    print(f"  {'✓' if verdict.ok else '!'} heartbeat {verdict.code}: {verdict.headline}")

    if args.report:
        with open(args.report, "w") as fh:
            fh.write(
                render_report(
                    verdict, args.repo, args.workflow, args.branch, args.threshold_hours
                )
            )

    return 0 if verdict.ok else 1


if __name__ == "__main__":
    sys.exit(main())
