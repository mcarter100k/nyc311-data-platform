-- SLO-2: completeness. Yesterday's created-request count must be at least
-- 40% of the median daily count over the seven days before it. Floor only:
-- completeness guards against MISSING data, so a volume spike is not a
-- breach. Why 0.40: NYC 311 weekend and holiday troughs run ~50-60% of the
-- weekly median, so the floor sits below natural variation while still
-- catching a half-empty ingest. Days are calendar days in the measuring
-- session's timezone (UTC on the scheduled runner).
WITH daily AS (
    SELECT cast(created_date AS date) AS day, count(*) AS n
    FROM gold.fct_service_requests
    WHERE cast(created_date AS date) BETWEEN current_date - 8 AND current_date - 2
    GROUP BY 1
),
yesterday AS (
    SELECT count(*) AS n
    FROM gold.fct_service_requests
    WHERE cast(created_date AS date) = current_date - 1
)
SELECT
    'SLO-2 completeness'                                                AS slo,
    (SELECT n FROM yesterday)                                           AS rows_yesterday,
    (SELECT median(n) FROM daily)                                       AS median_prior_7d,
    0.40                                                                AS tolerance_floor,
    (SELECT n FROM yesterday) >= 0.40 * (SELECT median(n) FROM daily)   AS pass;
