{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

-- fct_data_quality
--
-- Makes data quality a first-class observable metric rather than an invisible gate.
-- Reads every check ever written to SILVER.data_quality_log and computes:
--
--   rolling_7d_avg_failure_rate  — smooths day-to-day noise; a sustained drift
--                                  is more actionable than a single-day spike
--   first_seen / last_seen       — when a failure type first appeared and most
--                                  recently recurred; drives SLA conversations
--   is_rolling_threshold_breached — true when the 7-day average exceeds the same
--                                  threshold used by 03_silver.py to warn; this
--                                  column is the intended BI alert trigger
--
-- Materialized as a full table refresh (not incremental) because:
--   1. The source table has at most 5 checks × pipeline_days rows (~few thousand)
--   2. Window functions (rolling avg, first/last) require the full history
--   3. Re-running Silver for a past date can update historical DQ rows —
--      an incremental model would miss those updates

with dq_log as (

    select * from {{ ref('stg_data_quality_log') }}

),

-- 7-CALENDAR-DAY rolling average failure rate per check. A self-join on the
-- date window, not a `rows between 6 preceding` frame: a row-based frame
-- counts physical rows, so any gap in run_date (a skipped pipeline day)
-- silently stretches the "7-day" average across a longer calendar span —
-- mixing stale pre-outage rates into the window that drives the breach flag
-- exactly when it matters most. The join bounds the window in calendar days;
-- rolling_7d_day_count is the number of days WITH data inside it (< 7 in the
-- first week and after outages — interpret with fewer points in mind).
-- Safe to group by: (run_date, check_name) is unique-tested in
-- stg_data_quality_log.yml, so `a` rows never collapse.

with_rolling as (

    select
        a.run_date,
        a.check_name,
        a.pipeline_stage,
        a.records_checked,
        a.records_failed,
        a.failure_rate,

        avg(b.failure_rate)                                         as rolling_7d_avg_failure_rate,
        count(b.run_date)                                           as rolling_7d_day_count

    from dq_log a

    join dq_log b
      on b.check_name = a.check_name
     and b.run_date::date between a.run_date::date - 6
                              and a.run_date::date

    group by
        a.run_date,
        a.check_name,
        a.pipeline_stage,
        a.records_checked,
        a.records_failed,
        a.failure_rate

),

-- First and last dates each check ever had at least one failure.
-- A check with no failures in history will not appear in this CTE —
-- the left join below coalesces those to nulls.

failure_bounds as (

    select
        check_name,
        min(run_date)   as first_seen,
        max(run_date)   as last_seen,
        count(*)        as total_days_with_failures
    from dq_log
    where records_failed > 0
    group by check_name

),

final as (

    select
        r.run_date,
        r.check_name,
        r.pipeline_stage,
        r.records_checked,
        r.records_failed,
        round(r.failure_rate, 6)                                    as failure_rate,
        round(r.rolling_7d_avg_failure_rate, 6)                     as rolling_7d_avg_failure_rate,
        r.rolling_7d_day_count,
        f.first_seen,
        f.last_seen,
        coalesce(f.total_days_with_failures, 0)                     as total_days_with_failures,

        -- Mirrors the thresholds in 03_silver.py DQ_THRESHOLDS.
        -- When this flips to true in a BI dashboard, it means the 7-day
        -- rolling average has been elevated long enough to be a real trend,
        -- not a one-day anomaly.
        case
            when r.check_name = 'null_rate_unique_key'
             and r.rolling_7d_avg_failure_rate > 0.05  then true
            when r.check_name = 'null_rate_created_date'
             and r.rolling_7d_avg_failure_rate > 0.05  then true
            when r.check_name = 'duplicate_rate'
             and r.rolling_7d_avg_failure_rate > 0.10  then true
            when r.check_name = 'invalid_resolution_days'
             and r.rolling_7d_avg_failure_rate > 0.01  then true
            when r.check_name = 'unrecognized_borough'
             and r.rolling_7d_avg_failure_rate > 0.05  then true
            else false
        end                                                         as is_rolling_threshold_breached

    from with_rolling r
    left join failure_bounds f on r.check_name = f.check_name

)

select * from final
