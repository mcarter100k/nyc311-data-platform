# Service Level Objectives

An SLO (service level objective) is a written, measured promise. The daily run
([daily-run.yml](../.github/workflows/daily-run.yml)) makes two, and
[`scripts/check_slos.py`](../scripts/check_slos.py) measures both against the DuckDB Gold schema
right after each build. A breach fails the run and files (or comments on) a `daily-run-breach`
GitHub issue with the measured numbers. The queries on this page are copies of the files in
[`scripts/slo/`](../scripts/slo/); `scripts/check_claims.py` fails CI if they differ.

## SLO-1 — Freshness

**Target:** the newest `_loaded_at` in `gold.fct_service_requests` is less than **26 hours** old
when measured. 26 = one daily cycle plus 2 hours of grace for a late or slow run.

**What it measures:** `_loaded_at` is stamped by our own pipeline when Silver writes a row. So
SLO-1 says *a run recently succeeded in loading rows*: pipeline liveness. It cannot see whether
the city's data is stale; after any successful run the newest `_loaded_at` is minutes old (see the
[2026-08-18 postmortem](postmortems/2026-08-18-upstream-publish-stall.md)). Inside the daily run it is
measured minutes after a successful build, so it passes whenever the run gets that far; its
threshold matters when the check runs against a database that was not just rebuilt. It also
cannot see a run that never starts; the [heartbeat](../.github/workflows/heartbeat.yml) covers
that from outside.

<!--slo-sql:scripts/slo/slo1_freshness.sql-->
```sql
-- SLO-1: freshness. The newest row in the fact table must be under 26 hours
-- old at measurement time: one daily cycle plus a 2-hour grace for upstream
-- publish latency. Measured by scripts/check_slos.py immediately after the
-- scheduled build; the `pass` column is the verdict, everything else is the
-- evidence that goes into the breach issue.
-- _loaded_at is stamped in UTC, so "now" is taken AT TIME ZONE 'UTC'. Age is
-- elapsed time, not hour boundaries crossed.
SELECT
    'SLO-1 freshness'                                                       AS slo,
    max(_loaded_at)                                                         AS max_loaded_at,
    round(epoch(current_timestamp AT TIME ZONE 'UTC' - max(_loaded_at)) / 3600, 2) AS age_hours,
    26                                                                      AS threshold_hours,
    max(_loaded_at) > (current_timestamp AT TIME ZONE 'UTC') - INTERVAL 26 HOUR AS pass
FROM gold.fct_service_requests;
```

## SLO-2 — Completeness (source reconciliation)

**Target:** for **every day the load shows as complete**, we hold at least **98%** of the rows the
city itself says it **published** for that day.

**Why reconcile against the source.** This separates our losses from the city's. If the city
published 300 rows for a day because it was mid-outage and we loaded 300, our pipeline did its
job: green. If it published 10,000 and we loaded 300, the loss is ours: red. A day the city never
finished publishing is outside SLO-2 entirely; the [upstream stall
warning](#upstream-stall-warning-not-an-slo) covers that.

**How it works.** Three pieces:

1. The fetch stage asks the Socrata API for its own per-day counts across the whole fetch window
   (`local_runner.fetch_source_counts_window` → `silver.source_counts`).
2. [`int_load_completeness`](../dbt/models/intermediate/int_load_completeness.sql) decides which
   loaded days are complete: a day whose newest request lands within an hour of midnight.
3. The query below compares our Gold row count with the source's count for each of those days.

**Which days: chosen by the data, not the clock.** The source publishes on a lag that is not
constant: 23.3h and 23.5h in one week, then 49.0h at probe time (47.5h after the last publish).
So any fixed choice ("yesterday", "two days ago") is a whole day sometimes and a two-hour stub
other times, and a check on a stub proves nothing. Instead, every complete day in the window with a captured count is checked. Because the
fetch re-pulls and re-counts the whole window every run, a day first loaded as a stub is checked
again once the source fills it in. Full reasoning: [ADR 015](adr/015-slo2-population-is-complete-days.md).

**The source's count is sampled, not asked once.** Socrata answers identical queries from two
replicas, and one lags behind the other. Measured on 2026-08-27 over 98 grouped count requests,
each request was routed independently to one of exactly two states; the stale one answered 53% of
requests overall and 65% in the worst run. The stale replica is always *behind*, never ahead, and
the gap closes as a day ages ([ADR 016](adr/016-source-settling-horizon.md)). So the count query
runs eleven times (`local/local_runner.py#"SOURCE_COUNT_PROBES        = 11"`) and keeps each day's
highest answer: the best estimate of a number that only grows. Eleven is the smallest count that
keeps the chance of every probe hitting the stale replica under 1% at the worst measured split
(0.65¹¹ ≈ 0.009; five probes left it at 11.6%). The extra probes cost about 6 seconds (measured on
a 7-day window). A day missing from a probe's answer counts as that probe's zero. Each day records
`probe_count`, `source_count_min` and `probes_disagreed` in `silver.source_counts`, so the
denominator can be audited: the settling spread for a day is `source_count - source_count_min`.

Sampling makes SLO-2 stricter, deliberately: a higher denominator can only lower the ratio. It
helps only when *some* replica holds the day. If the source has not published a day at all, every
probe correctly returns 0. That case is handled by the population and the verdicts: only complete
days are checked, and a zero count on a complete day fails.

**Why 98% and not 100%.** Two things can legitimately lower the ratio, and the second is larger:

- **Deliberate removals, up to 0.24%.** Quarantined rows (closed before created) and true
  duplicates. The worst fully settled day of the 2026-08-27 load reconciled at
  10,521 / 10,546 = 0.9976.
- **Settling skew, up to 0.96%.** Our row count comes from whichever replica served the load; the
  source count is the highest of eleven probes. When they disagree, the gap lands in the ratio. It
  is largest at 3 days old, the youngest age at which a load from the stale replica can still reach
  midnight and count as complete: 112 / 11,627 = 0.963% measured.

Worst case is therefore
`scripts/slo/slo2_completeness.sql#"WORST CASE = 0.99037 * 0.99763 = 0.9880"`: a **1.20%** budget
against a 2.00% floor, leaving **0.80 points** of margin. The floor stays at 0.98; moving it would
mean fitting a threshold to one observation window. The margin is real, not theoretical: on the
2026-08-27 load the gate reported `worst_day=2026-08-24  worst_day_rows_loaded=11513
worst_day_rows_published=11627`, a ratio of **0.9902**. What would justify moving the floor is in
[ADR 016](adr/016-source-settling-horizon.md).

**How it fails.** The query's header lists the four failing cases. The last one, a window with no
complete day at all, means the fetch is wrong or the city has published nothing for about the
whole window: with 37 days loaded, dozens of days should be complete. Investigate before
re-running.

<!--slo-sql:scripts/slo/slo2_completeness.sql-->
```sql
-- SLO-2: completeness, measured as RECONCILIATION against the source.
-- We must have loaded at least 98% of what the city actually PUBLISHED, for
-- every day the load shows as complete. If the city published 10,000 rows for
-- a day and we hold 300, that loss is ours (red); if the city published 300
-- because it was mid-outage, that is not this gate's business (ADR 013).
--
-- Population: every day int_load_completeness marks complete (clock coverage).
-- The publish lag varies (23h-49h measured), so no fixed day offset works
-- (ADR 015). A day first loaded as a stub is re-checked on later runs while it
-- stays in the fetch window.
--
-- WHY 0.98 AND NOT 1.00 — the loss budget:
--   1. Deliberate row removal (quarantine, dedup): up to 0.24%.
--   2. Settling skew: up to 0.96%. The numerator is whichever replica served
--      the load; the denominator is the max over the capture probes. The gap
--      is largest at 3 days old, the youngest age a stale load can be complete
--      (112 / 11,627 measured), and zero from 7 days (ADR 016).
--
-- WORST CASE = 0.99037 * 0.99763 = 0.9880, i.e. a 1.20% budget against a 2.00%
-- floor: 0.80 points of margin. A settling gap above ~1.8% at 3 days would
-- justify moving the floor.
--
-- FOUR WAYS THIS FAILS, all deliberate:
--   * a complete day whose loaded count falls under the floor — real loss;
--   * a complete day with NO captured source count — a gate that cannot see
--     its reference must not pass;
--   * a complete day whose source count is ZERO — a contradiction: the load
--     says the source published that day through to midnight, so the capture
--     is wrong or the source retracted the day;
--   * NO complete day at all in the window — see the final CASE below.
with complete_days as (

    -- Days the load shows as fully published. Absent = outside the loaded
    -- window and not assessable from this build, which is not the same as
    -- incomplete.
    select load_day
    from gold.int_load_completeness
    where is_complete_day

),

ours as (

    select cast(created_date as date) as day, count(*) as n
    from gold.fct_service_requests
    group by 1

),

scored as (

    select
        c.load_day                                                          as day,
        coalesce(o.n, 0)                                                    as rows_loaded,
        s.source_count                                                      as rows_published,
        case
            when s.source_count is null then false
            when s.source_count = 0     then false
            else coalesce(o.n, 0) >= 0.98 * s.source_count
        end                                                                 as day_pass
    from complete_days c
    left join ours o           on o.day        = c.load_day
    left join silver.source_counts s on s.target_date = c.load_day

),

-- The single worst day, so the breach issue carries the day that failed rather
-- than an aggregate nobody can act on. Failing days first, then the lowest
-- ratio; a missing count sorts first of all, since it is the least explicable.
worst as (

    select *
    from scored
    order by day_pass asc,
             (rows_loaded * 1.0 / nullif(rows_published, 0)) asc nulls first,
             day desc
    limit 1

)

select
    'SLO-2 completeness'                                                    as slo,
    (select count(*) from scored)                                           as complete_days_assessed,
    (select max(day) from scored)                                           as newest_complete_day,
    (select day from worst)                                                 as worst_day,
    (select rows_loaded from worst)                                         as worst_day_rows_loaded,
    (select rows_published from worst)                                      as worst_day_rows_published,
    0.98                                                                    as tolerance_floor,
    -- Zero assessable days FAILS: the gate measured nothing. With 37 days
    -- loaded, dozens of days should be complete, so this means the fetch is
    -- wrong or the city has published nothing for about the whole window.
    -- Investigate before re-running.
    case
        when (select count(*) from scored) = 0 then false
        else (select bool_and(day_pass) from scored)
    end                                                                     as pass;
```

## Upstream stall warning (not an SLO)

[`scripts/check_upstream_stall.py`](../scripts/check_upstream_stall.py) answers the question SLO-2
does not: *is the city still publishing normally?* It is a **warning**: the run stays green, and a
stall files or updates a GitHub issue labelled `upstream-stall`, so the gap stays visible to anyone
reading the data. A day that fills in later, while still inside the fetch window, is re-checked by
SLO-2 on the next run. A day the city never publishes within the window cannot be recovered by the
daily run.

Either of two conditions warns. Both look at the newest day `int_load_completeness` marks
**complete**, and both use the source's own counts:

| Condition | Fires when | Basis |
|---|---|---|
| Staleness | the newest complete day is more than **2** days behind today (UTC) | at run time a healthy run sees yesterday as the partial day and the day before as complete, so 2 is normal and 3+ means a publish cycle was missed. Measured 3 on 2026-08-27. Few observations back this number, which is one reason it warns rather than gates |
| Volume | that day's **source** count is below **40%** of the median source count of the other complete days | the floor sits under NYC 311's natural ~50–60% weekend and holiday troughs; it catches a day the city publishes to midnight but only part-fills |

**Why a warning and not a third SLO.** A source-freshness gate was proposed after the 2026-08-18
incident, measured, and rejected ([ADR 013](adr/013-no-source-freshness-slo.md)). The metric that
would work (`max(created_date)`) duplicates what this check already detects, and the metric that
would not duplicate it (the dataset's publish stamp) read healthy throughout that very incident.
The rule is **gate on what we control, warn on what we don't**: a red build nobody can act on
teaches the operator to ignore red builds.
