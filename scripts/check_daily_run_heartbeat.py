#!/usr/bin/env python3
"""
check_daily_run_heartbeat.py: is the daily run still running?

scripts/check_slos.py runs inside daily-run.yml, so it cannot report a run that
never starts (a workflow disabled by GitHub's 60-day inactivity rule or by
hand, or a cron that GitHub never delivers). This check runs on its own
schedule and asks the Actions API two questions about the watched workflow:

  1. Is it still active? A disabled workflow is a breach even if its last
     success is minutes old, because no future run will fire.
  2. How long since its last successful run on the given branch? At or over
     the threshold is a breach.

The default threshold is 30 hours: GitHub starts the daily cron 3-8h late,
so gaps between healthy runs already reach 27h.

Any successful run on the branch counts, scheduled or manual; both refresh
the cached database. Runs on other branches do not count, because they save
to their own branch's cache and never refresh main's data.

Exit code: 0 live, 1 breach or check error, 2 missing --repo. The workflow
files its issue with `if: failure()`. --report writes a markdown issue body.

Usage:
    python scripts/check_daily_run_heartbeat.py \
        --repo owner/name --workflow daily-run.yml \
        --threshold-hours 30 --report heartbeat_report.md

    # Offline: read the two facts from a file instead of the API.
    python scripts/check_daily_run_heartbeat.py --fixture facts.json --now 2026-08-27T12:00:00Z
"""

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, UTC

DEFAULT_WORKFLOW = "daily-run.yml"
DEFAULT_THRESHOLD_HOURS = 30.0


@dataclass(frozen=True)
class Verdict:
    """The decision, separated from how the facts were obtained."""

    ok: bool
    code: str  # live | workflow-disabled | never-succeeded | stale
    headline: str
    age_hours: float | None


def parse_ts(value: str) -> datetime:
    """Parse a GitHub API timestamp ('2026-08-26T10:30:55Z') as aware UTC."""
    return datetime.fromisoformat(value).astimezone(UTC)


def evaluate(
    *,
    workflow_state: str,
    last_success_completed_at: str | None,
    now: datetime,
    threshold_hours: float = DEFAULT_THRESHOLD_HOURS,
    workflow: str = DEFAULT_WORKFLOW,
) -> Verdict:
    """Decide the verdict from the two API facts. No I/O.

    A disabled workflow is checked first: it is a breach even when the last
    success is recent, because no future run will fire.
    """
    if workflow_state != "active":
        return Verdict(
            ok=False,
            code="workflow-disabled",
            headline=(
                f"`{workflow}` is **{workflow_state}**, not active — "
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

    Uses the run's `updated_at` (when it finished), not `created_at` (when it
    was queued), because freshness is about when the data landed.
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
        f"- Threshold: {threshold_hours:g}h since the last successful run\n\n"
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

    now = parse_ts(args.now) if args.now else datetime.now(UTC)

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
        workflow=args.workflow,
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
