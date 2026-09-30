-- One row per (run_date, check_name) from SILVER.data_quality_log, with a
-- 7-day rolling failure rate, first/last failure dates, and a threshold-breach
-- flag for a dashboard to alert on. A full rebuild each run: the log is small,
-- the window functions need all history, and re-running Silver for a past date
-- can rewrite old rows.

with dq_log as (

    select * from {{ ref('stg_data_quality_log') }}

),

-- 7-calendar-day rolling average per check, as a self-join on dates rather
-- than a `rows between 6 preceding` frame, so a skipped run day shortens the
-- sample instead of stretching the window over older days.
-- rolling_7d_day_count says how many days had data. Grouping is safe because
-- (run_date, check_name) is unique-tested in staging.

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

-- First and last run_date with at least one failure; checks that never
-- failed are absent and come out NULL through the left join.

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

        -- Alert thresholds. These live only here: Silver records each rate
        -- but does not judge it.
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
