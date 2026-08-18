-- SLO-1: freshness. The newest row in the fact table must be under 26 hours
-- old at measurement time: one daily cycle plus a 2-hour grace for upstream
-- publish latency. Measured by scripts/check_slos.py immediately after the
-- scheduled build; the `pass` column is the verdict, everything else is the
-- evidence that goes into the breach issue.
SELECT
    'SLO-1 freshness'                                                   AS slo,
    max(_loaded_at)                                                     AS max_loaded_at,
    date_diff('hour', max(_loaded_at), current_timestamp::timestamp)    AS age_hours,
    26                                                                  AS threshold_hours,
    date_diff('hour', max(_loaded_at), current_timestamp::timestamp) < 26 AS pass
FROM gold.fct_service_requests;
