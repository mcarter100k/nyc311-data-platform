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
-- Why 0.98 and not 1.00: the quality filter legitimately quarantines a tiny
-- fraction (closed-before-created data-entry errors), and dedup can drop true
-- duplicates; both are deliberate, documented row removals — not loss. On a
-- 14-day live load measured 2026-08-27 the twelve complete days reconciled at
-- 0.9976 to 0.9998, so the floor sits well below observed behaviour.
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
