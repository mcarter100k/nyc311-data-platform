{{
    config(materialized = 'view')
}}

-- Staging contract for SILVER.data_quality_log, the per-run check results
-- written by 03_silver.py. Grain: one row per (run_date, check_name).
-- A deliberately thin passthrough view: it exists so fct_data_quality (its
-- only consumer) never references the source directly, keeping the
-- source-isolation rule uniform across the project — one staging model per
-- source table, even when there is nothing to rename.

select
    run_date,
    check_name,
    records_checked,
    records_failed,
    failure_rate,
    pipeline_stage
from {{ source('silver', 'data_quality_log') }}
