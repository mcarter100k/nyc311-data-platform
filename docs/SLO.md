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
cycle + 2h grace for upstream publish latency.

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

## SLO-2 — Completeness

**Target:** yesterday's created-request row count is at least **40%** of the median daily count
over the seven days before it. **Window:** trailing 8 calendar days (UTC on the runner).
**Why 40%:** NYC 311 weekend/holiday troughs run ~50–60% of the weekly median, so the floor sits
below natural variation while catching a half-empty ingest. Floor only — completeness guards
against missing data; a spike is not a breach.

<!--slo-sql:scripts/slo/slo2_completeness.sql-->
```sql
-- SLO-2: completeness. Yesterday's created-request count must be at least
-- 40% of the median daily count over the seven days before it. Floor only:
-- completeness guards against MISSING data, so a volume spike is not a
-- breach. Why 0.40: NYC 311 weekend and holiday troughs run ~50-60% of the
-- weekly median, so the floor sits below natural variation while still
-- catching a half-empty ingest. Days are calendar days in the measuring
-- session's timezone (UTC on the scheduled runner).
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
    'SLO-2 completeness'                                                AS slo,
    (SELECT n FROM yesterday)                                           AS rows_yesterday,
    (SELECT median(n) FROM daily)                                       AS median_prior_7d,
    0.40                                                                AS tolerance_floor,
    (SELECT n FROM yesterday) >= 0.40 * (SELECT median(n) FROM daily)   AS pass;
```
