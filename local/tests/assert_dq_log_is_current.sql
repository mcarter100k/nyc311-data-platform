-- Fails when the newest data_quality_log run_date is more than a day old, or
-- the log is empty. A singular test rather than source freshness because
-- run_date is a YYYY-MM-DD string, which freshness cannot parse.

select
    max(run_date)       as latest_run_date,
    current_date - 1    as staleness_threshold

from {{ source('silver', 'data_quality_log') }}

-- IS NULL catches an empty log: max() is then NULL and a bare HAVING would pass.
having max(run_date::date) < current_date - 1
    or max(run_date::date) is null
