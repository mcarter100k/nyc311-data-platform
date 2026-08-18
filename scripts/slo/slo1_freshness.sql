-- SLO-1: freshness. The newest row in the fact table must be under 26 hours
-- old at measurement time: one daily cycle plus a 2-hour grace for upstream
-- publish latency. Measured by scripts/check_slos.py immediately after the
-- scheduled build; the `pass` column is the verdict, everything else is the
-- evidence that goes into the breach issue.
-- _loaded_at is stamped in UTC, so "now" is taken AT TIME ZONE 'UTC' — a
-- session in any other timezone would otherwise skew the age by its offset.
SELECT
    'SLO-1 freshness'                                                       AS slo,
    max(_loaded_at)                                                         AS max_loaded_at,
    date_diff('hour', max(_loaded_at), current_timestamp AT TIME ZONE 'UTC')    AS age_hours,
    26                                                                      AS threshold_hours,
    date_diff('hour', max(_loaded_at), current_timestamp AT TIME ZONE 'UTC') < 26 AS pass
FROM gold.fct_service_requests;
