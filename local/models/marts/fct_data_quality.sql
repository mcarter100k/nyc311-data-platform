{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

with dq_log as (

    select * from {{ ref('stg_data_quality_log') }}

),

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
