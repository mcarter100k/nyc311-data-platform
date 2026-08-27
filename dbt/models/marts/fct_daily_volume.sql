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

-- Per-day completeness of the source publish, from the one model that defines
-- it (int_load_completeness). Joined, not re-derived: the recurrence horizon
-- needs the same concept and the two must not be allowed to drift apart.
--
-- This matters here because every figure on this table is a per-day figure, and
-- the newest loaded day is always a partial one — the source publishes on a
-- ~23.5h lag, so it holds the first couple of hours and stops (358 rows against
-- a ~10,500 median, measured 2026-08-26). Averaging days without filtering on
-- this flag pulls a ~2-hour day into the mean as though it were a day, which is
-- how the weekday-vs-weekend volume comparison got contaminated.
--
-- This is a metadata join on the date, one row per day; it does not re-read
-- facts from the intermediate layer and cannot change this model's grain.
load_completeness as (

    select * from {{ ref('int_load_completeness') }}

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
        -- Three-valued on purpose: TRUE / FALSE / NULL, where NULL means "this
        -- day is outside the currently loaded source window, so this build
        -- cannot assess it" — not "incomplete". The fact accumulates history
        -- past the window int_load_completeness is computed over, so days
        -- eventually age into NULL. Deliberately not coalesced: guessing TRUE
        -- would re-admit exactly the partial days the column exists to exclude,
        -- and guessing FALSE would retire good history.
        c.is_complete_day,
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

        -- pct_actioned: fraction where the city actually DID something, not
        -- merely closed the ticket. Read next to pct_resolved: the gap between
        -- them is the share of "resolutions" that found no violation, found
        -- nothing, were duplicates, or were handed off.
        round(
            sum(case when f.is_actioned then 1.0 else 0.0 end) / count(*),
            4
        )                                                                       as pct_actioned,

        -- ── Overdue metric ────────────────────────────────────────────────────
        sum(case when f.is_overdue = true then 1 else 0 end)                   as overdue_requests

    from fct f

    left join dim_date d
        on f.created_date_id = d.date_id

    left join dim_location l
        on f.location_id = l.location_id

    left join load_completeness c
        on d.full_date = c.load_day

    -- Exclude rows where the date join failed (created_date outside spine range).
    where d.full_date is not null

    group by
        d.full_date,
        d.year,
        d.month,
        d.quarter,
        d.is_weekend,
        d.is_federal_holiday,
        c.is_complete_day,
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
