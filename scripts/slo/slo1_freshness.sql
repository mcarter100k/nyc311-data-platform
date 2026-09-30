-- SLO-1: freshness. The newest row in the fact table must be under 26 hours
-- old at measurement time: one daily cycle plus a 2-hour grace for upstream
-- publish latency. Measured by scripts/check_slos.py immediately after the
-- scheduled build; the `pass` column is the verdict, everything else is the
-- evidence that goes into the breach issue.
-- _loaded_at is stamped in UTC, so "now" is taken AT TIME ZONE 'UTC'. Age is
-- elapsed time, not hour boundaries crossed.
SELECT
    'SLO-1 freshness'                                                       AS slo,
    max(_loaded_at)                                                         AS max_loaded_at,
    round(epoch(current_timestamp AT TIME ZONE 'UTC' - max(_loaded_at)) / 3600, 2) AS age_hours,
    26                                                                      AS threshold_hours,
    max(_loaded_at) > (current_timestamp AT TIME ZONE 'UTC') - INTERVAL 26 HOUR AS pass
FROM gold.fct_service_requests;
