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
2026-08-27 over 98 grouped count requests, Socrata answered from exactly **two**
states and routed each request independently — the stale share was 53% pooled and
65% in the worst single run. The disagreement is not noise: one replica is
**behind**, never ahead, and the gap closes as a day ages
([ADR 016](adr/016-source-settling-horizon.md)).
`fetch_source_counts_window` therefore probes **eleven** times
(`local/local_runner.py#"SOURCE_COUNT_PROBES        = 11"`) and keeps the per-day
maximum, which is the estimator for a quantity that only grows — what the city has
actually published for that day. Eleven is the smallest N holding
P(every probe lands on the stale replica) = 0.65¹¹ = 0.0087 under 1% at the worst
observed split; five left it at 11.6%. Measured cost of the change: **5.34 s → 11.69 s**
for a 7-day window, ~6 s on a run whose Gold build alone is minutes. A day absent from
a probe's response counts as that probe's zero. Each captured day records
`probe_count`, `source_count_min` and `probes_disagreed` in `silver.source_counts`, so
the denominator can be audited rather than trusted — the settling spread for a day is
`source_count - source_count_min`.

**Sampling makes SLO-2 stricter, and that is the intended direction.** The
denominator is the largest count any probe saw while the numerator is whatever
replica served the load, so raising the denominator can only lower the ratio. We
reconcile against the best estimate of what was published rather than against the
convenient number, and that choice spends margin against the 98% floor — the budget
is set out under SLO-2 below.

Be precise about what sampling buys, because an earlier version of this page was
not. Max-of-N helps only when *some* replica holds the day. When the source has
not published a day at all, every probe correctly returns 0, and no amount of
sampling changes that — the claim that this closed the zero-denominator exposure
was false. What closes it is SLO-2's population and its verdicts: the gate
assesses only days the load shows as **complete**, and a zero count on such a day
is a contradiction that **fails**, not a pass.

**No SLO covers source staleness, deliberately.** SLO-2 does *not* — it asks whether we
loaded what the city published for days the city published in full, so a day the city
never finished publishing is outside its population entirely. Source staleness is
surfaced by the non-gating [upstream stall warning](#upstream-stall-warning-not-an-slo)
below; the reasoning for keeping it a warning rather than promoting it to a third SLO is
recorded in [ADR 013](adr/013-no-source-freshness-slo.md), and the 2026-08-27 rebuild of
both signals in [ADR 015](adr/015-slo2-population-is-complete-days.md).

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

**Target:** for **every day the load shows as complete**, we hold at least **98%** of the rows
the city actually **published** for that day. **Window:** all complete days inside the trailing
fetch window, re-assessed on every run.

**How:** the fetch stage asks the Socrata API for its own per-day counts across the whole fetch
window in one grouped query (`local_runner.fetch_source_counts_window` → `silver.source_counts`);
[`int_load_completeness`](../dbt/models/intermediate/int_load_completeness.sql) says which loaded
days are whole; this query reconciles our Gold row count against the source's count for each of
those days.

**Why 98% and not 100% — the budget has two terms, and the second is the larger.** The first is
deliberate row removal: the quality filter quarantines closed-before-created data-entry errors and
dedup drops true duplicates — documented removals, not loss, worth up to **0.24%** (the worst
settled day of the 2026-08-27 live load reconciled at 10,521 / 10,546 = 0.9976). The second is
**settling skew**, worth up to **0.96%** — four times larger, and unnamed here until 2026-08-27.
The numerator comes from whichever replica served the load; the denominator is the maximum over
eleven capture probes. When those disagree the gap lands straight in the ratio, and at 3 days —
the youngest age at which a *stale* load of a day still reaches midnight and so still enters the
population — the measured gap was 112 / 11,627 = 0.963%.

Worst case is therefore
`scripts/slo/slo2_completeness.sql#"WORST CASE = 0.99037 * 0.99763 = 0.9880"`: a **1.20%** budget against a 2.00% floor,
leaving **0.80 points** of margin rather than the ~1.76 previously implied. The floor stays at
0.98 because 1.20 < 2.00, and moving it would mean fitting a threshold to one observation window —
but it is now roughly half consumed. This is not hypothetical: on the 2026-08-27 live load the
gate reported `worst_day=2026-08-24  worst_day_rows_loaded=11513  worst_day_rows_published=11627`,
a ratio of **0.9902**. The full measurement and what would move the floor are in
[ADR 016](adr/016-source-settling-horizon.md).

**What changed (2026-08-19):** SLO-2 previously compared yesterday's volume against a trailing
7-day median, which reddened our run whenever *the city* stopped publishing (see the 2026-08-18
postmortem and issue stream). That conflated two failure classes: if the city published 300 rows
and we loaded 300, our pipeline did its job — green — even mid-outage; if they published 10,000
and we loaded 300, the loss is ours — red. The old volume-cliff signal is preserved as the
**upstream stall warning** below.

**What changed (2026-08-27) — the population.** The reconciliation above was correct in question
and wrong in subject: it asked about `current_date - 1`. The source publishes on a lag, so
yesterday is never a whole day — it holds its first ~2 hours or nothing at all. Measured: the
2026-08-20 run reconciled 372/372 and reported "100%", certifying **3.5%** of that day's
eventual 10,701 rows; six identical probes for 2026-08-26 returned `[0, 0, 0, 0, 0, 0]`, and the
query's `WHEN source = 0 THEN true` branch turned that into a **pass on nothing**.

Moving the window to T-2 would only relocate the defect, because **the lag is not a constant**:
measured 23.3h and 23.5h twice in one week, then 49.0h on 2026-08-27 — with a publish 1.4h old
that carried nothing new. Any fixed offset is a whole day sometimes and a stub other times.

So the day is no longer chosen by arithmetic on the clock. It is chosen from the data, by the
primitive that already defines it: `int_load_completeness` marks a day complete when its newest
request lands within an hour of midnight — clock coverage, not a row-count threshold the source
is not read-consistent enough to support. Every complete day in the window with a captured
source count is assessed, which is also what makes a day **re-reconcilable**: the fetch re-pulls
and re-counts the whole window every run, so a day loaded as a stub is reconciled properly once
the source fills it in. The full reasoning, and what the design still does not solve, is in
[ADR 015](adr/015-slo2-population-is-complete-days.md).

**The three ways it fails, all deliberate:** a complete day under the floor (real loss); a
complete day with **no** captured count (a gate that cannot see its reference must not pass); and
a complete day whose captured count is **zero** (a contradiction — the load says the source
published that day through to midnight). Zero is no longer a pass anywhere. A window containing
**no** complete day fails too: the gate cannot measure, and the remedy — widen the fetch window
with `--live --days N` — is ours.

<!--slo-sql:scripts/slo/slo2_completeness.sql-->
```sql
-- SLO-2: completeness, measured as RECONCILIATION against the source.
-- We must have loaded at least 98% of what the city actually PUBLISHED, for
-- every day the load shows as complete. If the city published 10,000 rows for
-- a day and we hold 300, that loss is ours (red); if the city published 300
-- because it was mid-outage, that is not this gate's business (ADR 013).
--
-- THE POPULATION IS CHOSEN BY THE DATA, NOT BY THE CLOCK. This query used to
-- reconcile `current_date - 1` against a source count captured for
-- UTC-yesterday. That day is never a whole day: the source publishes on a lag,
-- so yesterday holds its first ~2 hours or nothing at all (358 rows against a
-- ~10,500 median on 2026-08-25; 0 rows on 2026-08-26). The gate therefore
-- certified a ~2-hour sliver on a good day and passed vacuously on a bad one.
--
-- Moving the window to T-2 does not fix it, because the lag is not a constant:
-- measured 23.3h and 23.5h twice in one week, then 49.0h on 2026-08-27 with a
-- publish 1.4h old. Any fixed offset is a stub on some days.
--
-- So the day is whatever int_load_completeness — the single definition of a
-- complete day, by clock coverage rather than by a row count the source is not
-- read-consistent about — says is complete. Every such day with a captured
-- source count is assessed, not just the newest, which is what makes a day
-- loaded as a stub get RE-RECONCILED once the source fills it in: the fetch
-- re-pulls and re-counts the whole window every run.
--
-- WHY 0.98 AND NOT 1.00 — the loss budget, in full. This comment used to name
-- only the first of the two terms below and read the headroom as ~1.76 points.
-- It is ~0.80.
--
--   1. DELIBERATE ROW REMOVAL — up to 0.24%. The quality filter quarantines
--      closed-before-created data-entry errors and dedup drops true duplicates.
--      Both are documented removals, not loss. Measured 2026-08-27 on the days
--      old enough to have stopped moving (7d+), the worst reconciled at
--      10,521 / 10,546 = 0.9976.
--
--   2. SETTLING SKEW — up to 0.96%, FOUR TIMES LARGER, and previously unnamed.
--      Socrata answers from two replicas, one of which is behind by an amount
--      that shrinks as a day ages and reaches zero at 7 days (ADR 016). The
--      numerator here is whatever replica served the LOAD; the denominator is
--      the maximum over SOURCE_COUNT_PROBES capture probes, which is
--      deliberately the freshest view available. When those disagree the gap
--      lands directly in this ratio. Measured 2026-08-27 over 20 probes/day the
--      gap at 3 days — the youngest age at which BOTH replicas hold a day whose
--      coverage reaches midnight, so the youngest age at which a stale load can
--      still be reconciled — was 112 / 11,627 = 0.963%.
--
--      Younger days do not widen this. At 2 days the stale replica holds only
--      the first ~2 hours (358 rows on 2026-08-25), so a load served by it is
--      not a complete day and never enters the population; a load served by the
--      FRESH replica is reconciled against a denominator from that same replica
--      and the gap is ~0. The exposure is a 3-day-old day, not a 1-day-old one.
--
-- WORST CASE = 0.99037 * 0.99763 = 0.9880, i.e. a 1.20% budget against a 2.00%
-- floor: 0.80 points of margin, not the 1.76 the old comment implied. The floor
-- STAYS at 0.98 — 1.20 < 2.00, so it is still adequate, and moving it would be
-- fitting a threshold to one observation window. But it is now roughly half
-- consumed, and the term that consumes it is the one nobody had measured. The
-- worst day actually observed is the arithmetic, not a hypothetical: on the
-- 2026-08-27 live load this gate reported worst_day = 2026-08-24 at
-- 11,513 / 11,627 = 0.9902.
--
-- What would move the floor: a settling gap at 3 days above ~1.8%, or a change
-- in what the city publishes late. ADR 016 records that the 7-day horizon comes
-- from ONE observation window and is not a guarantee.
--
-- THE THREE WAYS THIS FAILS, all deliberate:
--   * a complete day whose loaded count falls under the floor — real loss;
--   * a complete day with NO captured source count — a gate that cannot see
--     its reference must not pass;
--   * a complete day whose source count is ZERO — a contradiction, not a
--     "nothing to load" pass. The load says the source published that day
--     through to midnight; a zero denominator means the capture is wrong or
--     the source retracted the day, and the old `WHEN 0 THEN true` branch
--     turned exactly that into a green light.
--   * NO complete day at all in the window — see `assessable_days = 0` below.
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
    -- Zero assessable days FAILS. It means the loaded window contains no day
    -- the source has published in full, so this gate cannot measure the thing
    -- it exists to measure, and check_slos.py's own rule — zero checks
    -- evaluated is a breach of the gate, not a pass — applies inside the query
    -- too. It is also actionable by us rather than by the city: the remedy is
    -- to widen the fetch window (`--live --days N`), which is why gating on it
    -- does not violate ADR 013's "gate on what we control".
    case
        when (select count(*) from scored) = 0 then false
        else (select bool_and(day_pass) from scored)
    end                                                                     as pass;
```

## Upstream stall warning (not an SLO)

[`scripts/check_upstream_stall.py`](../scripts/check_upstream_stall.py) answers the question
SLO-2 does not: *is the city still publishing, and publishing normally?* It is a **warning** —
the run stays green, and a stall verdict files or updates a GitHub issue labeled `upstream-stall`
so the outage stays visible to anyone reading the dashboards. Recovery is automatic while the
gap stays inside the trailing fetch window (a day that fills in later is re-reconciled by SLO-2
on the next run); a day the city never publishes within that window is unrecoverable by the
daily run.

**Two conditions, either of which warns**, both anchored on the newest day
`int_load_completeness` marks **complete** — the same population fix SLO-2 got:

| Condition | Fires when | Basis |
|---|---|---|
| Staleness | the newest complete day is more than **2** days behind today (UTC) | at 10:00 UTC a healthy run sees yesterday as the publish-lag stub and the day before as complete, so 2 is normal and 3+ means a publish cycle was missed. Measured 3 on 2026-08-27 |
| Volume | that day's **source** count is below **40%** of the median source count of the other complete days | the floor sits under NYC 311's natural ~50–60% weekend/holiday troughs. Covers the *partial stall* ADR 013 recorded as a known limit |

**What it used to do, and why that was worthless (2026-08-27).** It compared *our own* row count
for `current_date - 1` against a trailing 7-day median of *our own* counts. Yesterday is the
publish-lag stub, so a healthy run scored ~358 against a ~10,500 median — 3.4%, against a 40%
floor. The check therefore fired on **100% of healthy runs**; issue #40 was commented every day
from 2026-08-20 onward. And comparing our counts to our counts meant it could not see the source
at all: a day we loaded thinly and a day the city published thinly were the same number to it.
A daily alert that cannot stay quiet discriminates nothing.

**Why this is a warning and not a third SLO.** A source-freshness gate was proposed by the
2026-08-18 postmortem, measured, and rejected — see [ADR 013](adr/013-no-source-freshness-slo.md).
In short: the metric that would work (`max(created_date)`) duplicates the volume cliff this
check already detects, and the metric that would not duplicate it (the dataset's publish stamp)
would have read healthy throughout the very incident that motivated the proposal. The standing
rule is **gate on what we control, warn on what we don't** — a red build nobody can act on
trains the operator to ignore red builds.

Note one honest wrinkle in that reasoning, resolved in
[ADR 015](adr/015-slo2-population-is-complete-days.md): ADR 013 rejected `max(created_date)` as
*redundant with this check*, and this check did not work. Staleness of the complete-day horizon
is now the thing this check measures directly, so the redundancy argument is restored rather
than contradicted — and it is still a warning, not a gate.
