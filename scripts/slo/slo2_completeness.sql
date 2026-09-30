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
