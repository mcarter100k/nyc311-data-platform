-- Singular test: assert that the data_quality_log was updated in the last 24 hours.
--
-- Returns one row when max(run_date) is more than one day old, which causes
-- `dbt test` to fail. Zero rows means the DQ log reflects a pipeline run from
-- today or yesterday — the Silver job ran successfully and wrote quality metrics.
--
-- Why a singular test instead of source freshness:
--   The `run_date` column is a VARCHAR (YYYY-MM-DD string), not a timestamp, so
--   dbt source freshness cannot parse it correctly. This test casts run_date
--   explicitly and is more debuggable: the failing row shows the actual latest
--   run_date alongside the staleness threshold, making the failure self-explanatory.

select
    max(run_date)       as latest_run_date,
    current_date - 1    as staleness_threshold

from {{ source('silver', 'data_quality_log') }}

having max(run_date::date) < current_date - 1
