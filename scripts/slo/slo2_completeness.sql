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
