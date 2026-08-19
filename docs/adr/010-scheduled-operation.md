# ADR 010: Scheduled Daily Operation Against the Live Source

**Status:** Accepted
**Date:** 2026-08-18
**Amends:** [ADR 008](008-prototype-scope.md) — the prototype boundary moves.

## Context

ADR 008 declared this repo a reference implementation: everything buildable,
nothing operating. That left one sentence unavailable to us: "this pipeline
has been running daily against live data." This ADR makes the repo operate —
at local-runner scale, on GitHub Actions, with written service commitments —
while leaving the cloud deployment exactly as deferred as ADR 008 says.

## What now operates

A GitHub Actions workflow (`.github/workflows/daily-run.yml`) runs the local
pipeline daily against the live Socrata API: fetch → DuckDB bronze/silver →
`dbt build` → SLO evaluation. The DuckDB file is retained as a 14-day
artifact. A pipeline failure or SLO breach files (or updates) a
`daily-run-breach` GitHub issue carrying the measured numbers and run URL.

**Schedule:** cron `0 10 * * *` = 06:00 America/New_York while DST is in
effect. GitHub cron cannot anchor to a timezone, so the same trigger fires at
05:00 local in winter. Accepted: the SLO windows are day-granular and
indifferent to a one-hour drift.

## Decision 1 — fetch window: `created_date`, not `:updated_at`

The first capped fetch on the `:updated_at` watermark failed by design, and
measurement explained why (live source, 2026-08-18):

| Predicate | Rows |
|---|---|
| `:updated_at` in trailing 1 day | 542,852 |
| `:updated_at` in trailing 7 days | 623,749 |
| `created_date` in trailing 7 days | 53,435 |

The source mass re-stamps `:updated_at` on roughly half a million rows
nightly — update volume is ~10× creation volume, and one day costs nearly as
much as seven. The cloud incremental spec (ADR-less; see PR #2's ingestion
contract) keeps `:updated_at` — at warehouse scale that volume is trivial and
catching every update is the point. A row-capped daily fetch on a public
runner cannot absorb it.

The daily run therefore uses an explicit `created_window` mode added to the
shared param builder (`databricks/notebooks/ingest_config.py`): fetch every
row *created* in the trailing 7 days, re-pulling the whole window each run.
Status updates to rows inside the window are captured by that re-pull; updates
to rows older than 7 days are outside this deployment's scope, by design and
documented. Nothing about the cloud spec changed.

## Decision 2 — row cap 150,000, and cap-hit is failure

Observed weekly creation volume is ~53–62k rows; the cap is ~2.4× that.
Hitting it means an upstream anomaly (volume spike, predicate regression),
and the run fails rather than proceeding with a silently truncated load — a
capped-but-green run would corrupt SLO-2's completeness math while looking
healthy. Zero rows and network failure (after exactly one retry) fail the
same way: red or fully green, never partial.

## Decision 3 — the SLO targets

- **SLO-1 freshness: newest `_loaded_at` < 26h at measurement.** One daily
  cycle plus 2h grace for run-time variance. This measures pipeline
  liveness (our own load stamp), NOT upstream staleness — `_loaded_at` is
  minutes old after any successful run, so source-side staleness detection
  rests on SLO-2 (see the 2026-08-18 postmortem). Measured in UTC
  explicitly — the first local evaluation returned `age_hours=-7` because the
  query compared a UTC stamp against session-local time.
- **SLO-2 completeness: yesterday's created-count ≥ 40% of the prior 7-day
  daily median.** Floor only — completeness guards against missing data, so
  spikes are not breaches. 40% sits below NYC 311's natural weekend/holiday
  troughs (~50–60% of median) while catching a half-empty ingest.

The executable queries live in `scripts/slo/`; `docs/SLO.md` reproduces them
and `scripts/check_claims.py` fails CI if the copies differ.

## What remains deferred (unchanged from ADR 008)

Azure/Databricks/Snowflake provisioning, the Airflow deployment, and the
Silver→Snowflake sync mechanism. The daily run operates the *local* pipeline;
it is evidence the design works against live data, not a substitute for the
cloud deployment.

## Consequences

- The README may claim "scheduled to run daily" with the workflow badge as
  live proof; "has been running daily since <date>" becomes claimable only
  by pointing at the run history.
- Breaches produce issues, and issues deserve postmortems —
  `docs/postmortems/TEMPLATE.md` exists from day one; it is filled in when
  reality provides material, never speculatively.
- No new dependencies: the workflow uses `local/requirements.txt` and the
  runner-provided `gh` with the default `github.token`.
