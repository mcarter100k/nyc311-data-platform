# Service Level Objectives

Two commitments for the scheduled daily run ([daily-run.yml](../.github/workflows/daily-run.yml)),
measured by [`scripts/check_slos.py`](../scripts/check_slos.py) against the DuckDB gold schema
immediately after each build. A breach files (or updates) a GitHub issue carrying the measured
numbers. The queries below are the contract — the executable copies live in
[`scripts/slo/`](../scripts/slo/) and `scripts/check_claims.py` fails CI if this page and those
files ever differ.

## SLO-1 — Freshness

**Target:** the newest `_loaded_at` in `gold.fct_service_requests` is less than **26 hours** old
at measurement time. **Window:** point-in-time, measured once per scheduled run. 26 = one daily
cycle + 2h grace for run-time variance (a late or long-running scheduled run).

**What this does and does not measure:** `_loaded_at` is stamped by our own pipeline when
Silver writes the row, so SLO-1 verifies that *a run recently succeeded in producing rows* —
pipeline liveness. It is structurally blind to upstream staleness: after any successful run
the newest `_loaded_at` is minutes old regardless of how stale the city's source data is
(see the 2026-08-18 postmortem).

**The denominator is sampled, not asked once.** Socrata is not read-consistent:
identical queries are answered by replicas at different indexing states. Measured
2026-08-26, six identical count calls for the same day returned 0, 0, 358, 358,
358, 0. A single capture was therefore a coin flip — and a losing flip is worse
than noisy, because the query below returns **pass** on a zero denominator, so a
capture landing on a lagging replica would certify completeness against nothing.
`fetch_source_count_yesterday` now probes five times and keeps the maximum: a row
visible on any replica exists, so the highest count is the most complete view
available. Disagreement is printed and the run records the value it took. No zero
capture has occurred in production to date (six for six non-zero); this closes the
exposure rather than repairs a failure.

**No SLO covers source staleness, deliberately.** SLO-2 does *not* — it reconciles our row
count against the city's own count for the same day, so a day the city barely published
reconciles at 100% and passes. Source staleness is surfaced by the non-gating
[upstream stall warning](#upstream-stall-warning-not-an-slo) below, and the reasoning for
keeping it a warning rather than promoting it to a third SLO is recorded in
[ADR 013](adr/013-no-source-freshness-slo.md).

<!--slo-sql:scripts/slo/slo1_freshness.sql-->
```sql
-- SLO-1: freshness. The newest row in the fact table must be under 26 hours
-- old at measurement time: one daily cycle plus a 2-hour grace for upstream
-- publish latency. Measured by scripts/check_slos.py immediately after the
-- scheduled build; the `pass` column is the verdict, everything else is the
-- evidence that goes into the breach issue.
-- _loaded_at is stamped in UTC, so "now" is taken AT TIME ZONE 'UTC' — a
-- session in any other timezone would otherwise skew the age by its offset.
SELECT
    'SLO-1 freshness'                                                       AS slo,
    max(_loaded_at)                                                         AS max_loaded_at,
    date_diff('hour', max(_loaded_at), current_timestamp AT TIME ZONE 'UTC')    AS age_hours,
    26                                                                      AS threshold_hours,
    date_diff('hour', max(_loaded_at), current_timestamp AT TIME ZONE 'UTC') < 26 AS pass
FROM gold.fct_service_requests;
```

## SLO-2 — Completeness (source reconciliation)

**Target:** we loaded at least **98%** of the rows the city actually **published** for yesterday.
**How:** the fetch stage asks the Socrata API for its own count of yesterday's created requests
(`local_runner.fetch_source_count_yesterday` → `silver.source_counts`); this query compares our
Gold row count against that number. **Why 98% and not 100%:** the quality filter deliberately
quarantines a tiny fraction (closed-before-created data-entry errors) and dedup can drop true
duplicates — documented removals, not loss.

**What changed (2026-08-19):** SLO-2 previously compared yesterday's volume against a trailing
7-day median, which reddened our run whenever *the city* stopped publishing (see the 2026-08-18
postmortem and issue stream). That conflated two failure classes. Now: if the city published 300
rows and we loaded 300, our pipeline did its job — green — even mid-outage; if they published
10,000 and we loaded 300, the loss is ours — red. A missing source-count capture fails closed;
a zero source count passes (nothing to load). The old volume-cliff signal is preserved as the
**upstream stall warning** below.

<!--slo-sql:scripts/slo/slo2_completeness.sql-->
```sql
-- SLO-2: completeness, measured as RECONCILIATION against the source.
-- We must have loaded at least 98% of what the city actually PUBLISHED for
-- yesterday — not 40% of a historical volume guess. If the city published
-- 300 rows and we loaded 300, our pipeline did its job (green) even during
-- an upstream outage; if they published 10,000 and we loaded 300, that loss
-- is ours (red). The source-side number is captured at fetch time by
-- local_runner.fetch_source_count_yesterday into silver.source_counts.
-- Why 0.98 and not 1.00: the quality filter legitimately quarantines a tiny
-- fraction (closed-before-created data-entry errors), and dedup can drop
-- true duplicates; both are deliberate, documented row removals — not loss.
-- NULL source count (capture missing) fails closed: a gate that cannot see
-- its reference must not pass. source_count = 0 passes: nothing published
-- means nothing to load — the upstream-stall WARNING path (not this gate)
-- reports that condition. Days are UTC calendar days on the runner.
WITH ours AS (
    SELECT count(*) AS n
    FROM gold.fct_service_requests
    WHERE cast(created_date AS date) = current_date - 1
),
source AS (
    SELECT source_count AS n
    FROM silver.source_counts
    WHERE target_date = current_date - 1
)
SELECT
    'SLO-2 completeness'                                                AS slo,
    (SELECT n FROM ours)                                                AS rows_loaded_yesterday,
    (SELECT n FROM source)                                              AS rows_published_by_source,
    0.98                                                                AS tolerance_floor,
    CASE
        WHEN (SELECT n FROM source) IS NULL THEN false
        WHEN (SELECT n FROM source) = 0     THEN true
        ELSE (SELECT n FROM ours) >= 0.98 * (SELECT n FROM source)
    END                                                                 AS pass;
```

## Upstream stall warning (not an SLO)

The volume-cliff check that used to be SLO-2 — yesterday below **40%** of the trailing 7-day
median (the floor sits under NYC 311's natural ~50–60% weekend/holiday troughs) — lives on in
[`scripts/check_upstream_stall.py`](../scripts/check_upstream_stall.py) as a **warning**: the
run stays green, and a stall verdict files or updates a GitHub issue labeled `upstream-stall`
so the outage stays visible to anyone reading the dashboards. Recovery is automatic while the
gap stays inside the trailing 7-day fetch window; a day the city never publishes within that
window is unrecoverable by the daily run.

**Why this is a warning and not a third SLO.** A source-freshness gate was proposed by the
2026-08-18 postmortem, measured, and rejected — see [ADR 013](adr/013-no-source-freshness-slo.md).
In short: the metric that would work (`max(created_date)`) duplicates the volume cliff this
check already detects, and the metric that would not duplicate it (the dataset's publish stamp)
would have read healthy throughout the very incident that motivated the proposal. The standing
rule is **gate on what we control, warn on what we don't** — a red build nobody can act on
trains the operator to ignore red builds.
