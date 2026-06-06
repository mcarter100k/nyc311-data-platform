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

-- 7-day rolling average failure rate per check, ordered by run_date.
-- rolling_7d_day_count < 7 in the first week of pipeline history — the average
-- is still meaningful but should be interpreted with fewer data points in mind.

with_rolling as (

    select
        run_date,
        check_name,
        pipeline_stage,
        records_checked,
        records_failed,
        failure_rate,

        avg(failure_rate) over (
            partition by check_name
            order by run_date
            rows between 6 preceding and current row
        )                                                           as rolling_7d_avg_failure_rate,

        count(*) over (
            partition by check_name
            order by run_date
            rows between 6 preceding and current row
        )                                                           as rolling_7d_day_count

    from dq_log

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
