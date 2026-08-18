{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

-- Grain: one row per (calendar date, borough, complaint category).
-- Pre-aggregated to avoid repeated GROUP BY in BI queries; avoids full
-- fct_service_requests scan on every dashboard render.

with fct as (

    select * from {{ ref('fct_service_requests') }}

),

dim_date as (

    select * from {{ ref('dim_date') }}

),

dim_location as (

    select * from {{ ref('dim_location') }}

),

aggregated as (

    select
        -- ── Grain keys ────────────────────────────────────────────────────────
        d.full_date,
        d.year,
        d.month,
        d.quarter,
        d.is_weekend,
        d.is_federal_holiday,
        -- The dim_location join is LEFT, so l.borough is NULL whenever a fact
        -- row carries no location_id. Fold those into the UNSPECIFIED bucket so
        -- the grain key stays non-null and no volume is silently dropped.
        coalesce(l.borough, 'UNSPECIFIED')                                      as borough,
        f.complaint_category,

        -- ── Volume metrics ────────────────────────────────────────────────────
        count(*)                                                                as total_requests,

        -- ── Resolution metrics ────────────────────────────────────────────────
        -- avg_resolution_days excludes NULLs (open requests) automatically.
        round(avg(f.resolution_days), 2)                                        as avg_resolution_days,

        -- pct_resolved: fraction of requests at Closed status on the reference date.
        -- Expressed as a decimal (0.0 – 1.0) for BI tool compatibility.
        round(
            sum(case when f.is_resolved then 1.0 else 0.0 end) / count(*),
            4
        )                                                                       as pct_resolved,

        -- ── Overdue metric ────────────────────────────────────────────────────
        sum(case when f.is_overdue = true then 1 else 0 end)                   as overdue_requests

    from fct f

    left join dim_date d
        on f.created_date_id = d.date_id

    left join dim_location l
        on f.location_id = l.location_id

    -- Exclude rows where the date join failed (created_date outside spine range).
    where d.full_date is not null

    group by
        d.full_date,
        d.year,
        d.month,
        d.quarter,
        d.is_weekend,
        d.is_federal_holiday,
        coalesce(l.borough, 'UNSPECIFIED'),
        f.complaint_category

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'full_date',
            'borough',
            'complaint_category'
        ]) }}                                                                   as daily_volume_id,
        *

    from aggregated

)

select * from final
