-- Passthrough over SILVER.data_quality_log, one row per (run_date, check_name).
-- Nothing to rename; it exists so fct_data_quality reads through staging like
-- every other model.

select
    run_date,
    check_name,
    records_checked,
    records_failed,
    failure_rate,
    pipeline_stage
from {{ source('silver', 'data_quality_log') }}
