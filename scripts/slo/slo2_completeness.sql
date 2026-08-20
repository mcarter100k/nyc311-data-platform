-- SLO-2: completeness, measured as RECONCILIATION against the source.
-- We must have loaded at least 98% of what the city actually PUBLISHED for
-- yesterday — not 40% of a historical volume guess. If the city published
-- 300 rows and we loaded 300, our pipeline did its job (green) even during
-- an upstream outage; if they published 10,000 and we loaded 300, that loss
-- is ours (red). The source-side number is captured at fetch time by
-- local_runner.fetch_source_count_yesterday into silver.source_counts.
-- Why 0.98 and not 1.00: the quality filter legitimately quarantines a tiny
-- fraction (closed-before-created data-entry errors), and dedup can drop
-- true duplicates; both are deliberate, documented row removals — not loss.
-- NULL source count (capture missing) fails closed: a gate that cannot see
-- its reference must not pass. source_count = 0 passes: nothing published
-- means nothing to load — the upstream-stall WARNING path (not this gate)
-- reports that condition. Days are UTC calendar days on the runner.
WITH ours AS (
    SELECT count(*) AS n
    FROM gold.fct_service_requests
    WHERE cast(created_date AS date) = current_date - 1
),
source AS (
    SELECT source_count AS n
    FROM silver.source_counts
    WHERE target_date = current_date - 1
)
SELECT
    'SLO-2 completeness'                                                AS slo,
    (SELECT n FROM ours)                                                AS rows_loaded_yesterday,
    (SELECT n FROM source)                                              AS rows_published_by_source,
    0.98                                                                AS tolerance_floor,
    CASE
        WHEN (SELECT n FROM source) IS NULL THEN false
        WHEN (SELECT n FROM source) = 0     THEN true
        ELSE (SELECT n FROM ours) >= 0.98 * (SELECT n FROM source)
    END                                                                 AS pass;
