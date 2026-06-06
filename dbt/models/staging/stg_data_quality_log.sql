{{
    config(materialized = 'view')
}}

select
    run_date,
    check_name,
    records_checked,
    records_failed,
    failure_rate,
    pipeline_stage
from {{ source('silver', 'data_quality_log') }}
