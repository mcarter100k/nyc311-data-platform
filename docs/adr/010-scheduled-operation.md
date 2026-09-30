# ADR 010: Scheduled Daily Operation Against the Live Source

**Status:** Accepted. Amended 2026-08-20 and 2026-09-29 (37-day window, 800k cap, 30-hour heartbeat; see the end).
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

The daily run therefore fetches by creation date (`local/ingest_config.py`):
every row *created* in the trailing 7 days, re-pulling the whole window each
run.
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
- *(Amended 2026-08-19: SLO-2 was redefined as a source reconciliation — we
  must load ≥98% of what the city actually published for yesterday, with the
  source's own count captured at fetch time into silver.source_counts. The
  original volume-cliff check below was demoted to a non-gating warning
  (scripts/check_upstream_stall.py, `upstream-stall` issue label): the
  2026-08-18 publish stall showed it reddens our reliability signal for the
  city's outages. Detection is preserved — the warning files the issue — but
  the run only fails when the loss is ours. See docs/SLO.md.)*
- **SLO-2 completeness: yesterday's created-count ≥ 40% of the prior 7-day
  daily median.** Floor only — completeness guards against missing data, so
  spikes are not breaches. 40% sits below NYC 311's natural weekend/holiday
  troughs (~50–60% of median) while catching a half-empty ingest.

The executable queries live in `scripts/slo/`; `docs/SLO.md` reproduces them
and `scripts/check_claims.py` fails CI if the copies differ.

## Amendment 2026-08-20 — Airflow runs locally, as a demonstration

`airflow/dags/nyc311_local.py` is a second DAG that actually executes: seven
tasks (source gate, fetch, bronze, silver, dbt build, SLO check, upstream-stall
warning) shelling out to `local_runner.py`. Verified end to end with
`airflow dags test` — DagRun state=success, all seven tasks green, the dbt
build inside the DAG reporting PASS=113 / ERROR=0.

`nyc311_pipeline.py`, the cloud DAG, was deleted with the Databricks path
([ADR 005](005-orchestration-strategy.md)).

**This does not change what operates the pipeline.** The Airflow scheduler only
fires while its process is alive, so a laptop misses any run scheduled while the
machine is asleep. `.github/workflows/daily-run.yml` remains the scheduled
runner. The local Airflow demonstrates that the orchestration design works; it
does not run it daily, and the README says so.

`catchup=False` is load-bearing rather than conventional: the fetcher pulls a
trailing 7-day window, so backfilling missed intervals would re-fetch the same
rows repeatedly. A missed run is covered by the next run's window.

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

## Amendment 2026-09-29 — a 37-day window, an 800k cap, a 30-hour heartbeat

**The fetch window is 37 days (was 7).** A request's status in Gold changes
only while the request is inside the fetch window, because the daily run
re-pulls that window and nothing else. With 7 days, Gold never saw a closure
after day 7, so its published 30-day closure rates (`fct_daily_volume`) were
really 7-day rates. Measured:

- On the same cohorts (requests created 2026-08-24 to 08-28), the 30-day
  closure rate read **68.4%** with the 7-day window and **89.5%** with 37 days.
- Three independent samples of rows Gold held as open found 22 of 40, 30 of 40
  and 25 of 30 already closed at the source.

37 is the 30-day closure window (`closure_window_days` in `dbt_project.yml`)
plus the 7-day settling horizon ([ADR 016](016-source-settling-horizon.md)), so
a request stays in the window until its 30-day outcome is final at the source.

Rows older than 37 days keep their last-seen status. That is enough for every
30-day metric Gold publishes, but a request still open at day 37 stays open in
Gold even after the city closes it. Rows loaded while the window was 7 days are
stale in the same way until one wide run re-fetches them. Both are in
[BACKLOG](../BACKLOG.md).

**The row cap is 800,000 (was 150,000).** A 37-day live run fetched 385,285
rows in 8.2 minutes, and all 148 dbt nodes and both SLOs passed. The cap is
about twice the measured volume. Hitting it still fails the run, for the reason
in Decision 2.

**The job timeout is 30 minutes (was 15)**, room for the larger fetch while a
hung run still stops long before the next day's trigger.

**SLO-2 no longer checks one day.** Decision 3's reconciliation now covers
every complete day in the window ([ADR 015](015-slo2-population-is-complete-days.md)).

**Retries.** "Exactly one retry" in Decision 2 was replaced by
[ADR 015](015-slo2-population-is-complete-days.md): three attempts with 1s and
2s backoff on connection errors, 429 and 5xx; any other error status fails at
once.

**The heartbeat threshold is 30 hours (was 26).** `heartbeat.yml` checks from
outside the daily run that `daily-run.yml` is still enabled and has succeeded
on `main` within the threshold, and files a `daily-run-breach` issue if not. It
is scheduled every 4 hours, and GitHub may delay or skip it. The threshold is
not SLO-1's 26 hours because GitHub starts the 10:00 UTC cron late (runs
typically start 13:00–18:00 UTC), and gaps between healthy successful runs
have reached 26.98 hours.

**One fetch mode.** `local/ingest_config.py` builds only the created-window
query. The `:updated_at` incremental mode and the full-load mode were removed
with the rest of the Databricks path, so no `:updated_at` watermark exists in
the repo.
