-- One row per (created date, borough, complaint category), pre-aggregated so
-- dashboards do not scan fct_service_requests.
--
-- Every closure rate here is windowed and gated. A newly created cohort has
-- had little time to close, so "closed / created that day" reads low on the
-- newest days (right-censoring). So each rate counts "closed within
-- closure_window_days of creation", and a day publishes it only once that many
-- complete days of history follow it (is_denominator_closed); otherwise the
-- rate is NULL. assert_daily_volume_rates_have_closed_denominators checks the
-- gate against an independent recomputation.
--
-- The daily fetch re-pulls requests created in the last 37 days, so a row's
-- status keeps updating until day 37: closures up to day 30 plus 7 days for
-- the source to settle (ADR 016). A published rate can still rise slightly
-- until its day leaves that window; after that it is final.
--
-- total_requests is not censored: a count of requests created on a day is
-- whole once the source has published the day (see is_complete_day).
--
-- requests_open_past_window counts every request not closed within the
-- window, including ones still open. A sum of is_overdue would miss those,
-- because is_overdue is NULL while a request is open.

with fct as (

    select * from {{ ref('fct_service_requests') }}

),

dim_date as (

    select * from {{ ref('dim_date') }}

),

dim_location as (

    select * from {{ ref('dim_location') }}

),

-- One row per day from the single definition of a complete day. The newest
-- loaded day is always partial, so per-day averages must filter on it.
load_completeness as (

    select * from {{ ref('int_load_completeness') }}

),

-- End of trustworthy history: the newest complete day, the same horizon
-- fct_complaint_recurrence uses. NULL when no loaded day is complete.
horizon as (

    select max(load_day) as last_complete_date
    from load_completeness
    where is_complete_day

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
        -- NULL once the day leaves the loaded window: not assessable, which is
        -- not the same as incomplete, so it is not coalesced.
        c.is_complete_day,
        -- A fact row with no location_id folds into UNSPECIFIED, not dropped.
        coalesce(l.borough, 'UNSPECIFIED')                                      as borough,
        f.complaint_category,

        -- ── Volume (not censored) ─────────────────────────────────────────────
        count(*)                                                                as total_requests,

        -- ── Decode coverage (not censored) ────────────────────────────────────
        -- Rows whose resolution text the closure_type decoder could not read.
        -- They count as not actioned, so this sizes the floor under
        -- pct_actioned_within_window.
        sum(case when f.closure_type = 'Undecodable' then 1 else 0 end)         as undecodable_closure_requests,

        -- ── Window numerators (gated in `final`, never selected raw) ──────────
        sum(
            case
                when f.is_resolved
                 and f.resolution_days <= {{ var('closure_window_days') }}
                then 1 else 0
            end
        )                                                                       as closed_in_window,

        sum(
            case
                when f.is_resolved
                 and f.is_actioned
                 and f.resolution_days <= {{ var('closure_window_days') }}
                then 1 else 0
            end
        )                                                                       as actioned_in_window,

        -- Mean days to close among requests closed inside the window, so it is
        -- fixed per cohort instead of creeping up as late closures arrive.
        avg(
            case
                when f.is_resolved
                 and f.resolution_days <= {{ var('closure_window_days') }}
                then f.resolution_days
            end
        )                                                                       as avg_resolution_days_in_window

    from fct f

    left join dim_date d
        on f.created_date_id = d.date_id

    left join dim_location l
        on f.location_id = l.location_id

    left join load_completeness c
        on d.full_date = c.load_day

    -- Drop rows whose created_date falls outside the date spine.
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

observed as (

    select
        a.*,

        -- Complete days of history after this day, floored at zero. The NULL
        -- case is explicit because GREATEST(0, NULL) is NULL on Snowflake but
        -- 0 on DuckDB.
        case
            when h.last_complete_date is null then null
            else greatest(0, datediff('day', a.full_date, h.last_complete_date))
        end                                                                     as observation_days

    from aggregated a
    cross join horizon h

),

eligibility as (

    select
        o.*,

        -- The publication rule:
        --   observation_days >= closure_window_days: the cohort has had the
        --     full window to close.
        --   is_complete_day IS DISTINCT FROM FALSE: the day's own rows are
        --     whole; a half-published day is a biased sample however old it is.
        --     NULL (aged out of the load) is allowed, or no day in a long-lived
        --     warehouse would ever publish.
        -- FALSE, never NULL, when no complete day exists.
        (
            o.observation_days is not null
            and o.observation_days >= {{ var('closure_window_days') }}
            and o.is_complete_day is distinct from false
        )                                                                       as is_denominator_closed

    from observed o

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'full_date',
            'borough',
            'complaint_category'
        ]) }}                                                                   as daily_volume_id,

        full_date,
        year,
        month,
        quarter,
        is_weekend,
        is_federal_holiday,
        is_complete_day,
        borough,
        complaint_category,

        total_requests,
        undecodable_closure_requests,

        -- ── Eligibility, published so a reader can see why a rate is NULL ─────
        {{ var('closure_window_days') }}                                        as closure_window_days,
        observation_days,
        is_denominator_closed,

        -- ── Window measures, published only over a closed denominator ─────────
        case when is_denominator_closed then closed_in_window end                as requests_closed_within_window,

        case when is_denominator_closed then total_requests - closed_in_window end
                                                                                as requests_open_past_window,

        case
            when is_denominator_closed
            then round(1.0 * closed_in_window / total_requests, 4)
        end                                                                     as pct_closed_within_window,

        case
            when is_denominator_closed
            then round(1.0 * actioned_in_window / total_requests, 4)
        end                                                                     as pct_actioned_within_window,

        case
            when is_denominator_closed
            then round(avg_resolution_days_in_window, 2)
        end                                                                     as avg_resolution_days_within_window

    from eligibility

)

select * from final
