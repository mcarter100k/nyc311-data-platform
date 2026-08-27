{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

-- Grain: one row per (calendar date, borough, complaint category).
--
-- EVERY RATE HERE IS WINDOWED, because a rate whose numerator is "closed" over
-- a denominator of "created this day" is right-censored, worst on the newest
-- days. Measured on the local load (horizon 2026-08-24), the old pct_resolved
-- ran 0.7452 at 12 days observed down to 0.4003 at zero, and avg_resolution_days
-- 1.38 down to 0.07 — printed in the same column as though comparable. So every
-- rate is bounded to closure_window_days ("closed within N days of creation")
-- and published ONLY where N complete days follow the day; otherwise NULL.
-- total_requests is not censored — a created-count is whole once the source has
-- published the day, which is what is_complete_day says. See dbt/.
--
-- overdue_requests is gone: it summed is_overdue, which is NULL while a request
-- is open, so a request open for 200 days counted ZERO while one closed on day
-- 31 counted one. requests_open_past_window counts both. See dbt/.

with fct as (

    select * from {{ ref('fct_service_requests') }}

),

dim_date as (

    select * from {{ ref('dim_date') }}

),

dim_location as (

    select * from {{ ref('dim_location') }}

),

-- Per-day source completeness, from the one model that defines it. Every
-- figure here is a per-day figure and the newest loaded day is always partial
-- (~23.5h publish lag; 358 rows vs a ~10,500 median), which is how the
-- weekday-vs-weekend volume comparison got contaminated. Metadata join on the
-- date, one row per day — it cannot change this model's grain. See dbt/.
load_completeness as (

    select * from {{ ref('int_load_completeness') }}

),

-- End of trustworthy history — same definition fct_complaint_recurrence uses,
-- read from the same model. NOT max(created_date). NULL when no day is
-- complete, written out below rather than left to GREATEST. See dbt/.
horizon as (

    select max(load_day) as last_complete_date
    from load_completeness
    where is_complete_day

),

aggregated as (

    select
        d.full_date,
        d.year,
        d.month,
        d.quarter,
        d.is_weekend,
        d.is_federal_holiday,
        -- Three-valued: TRUE / FALSE / NULL, where NULL means the day is
        -- outside the currently loaded source window and this build cannot
        -- assess it — not "incomplete". Deliberately not coalesced. See dbt/.
        c.is_complete_day,
        coalesce(l.borough, 'UNSPECIFIED')                                      as borough,
        f.complaint_category,

        count(*)                                                                as total_requests,

        -- Rows whose resolution text the closure_type decoder could not read.
        -- is_actioned is FALSE for all of them, so this is the decode-shaped
        -- floor under pct_actioned_within_window at the same grain. Not
        -- censored — a property of the rows, not of elapsed time. See dbt/.
        sum(case when f.closure_type = 'Undecodable' then 1 else 0 end)         as undecodable_closure_requests,

        -- Censored numerators; gated in `final`, never selected raw.
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
        -- Complete days of published history following this created day.
        -- Floored at zero like fct_complaint_recurrence; NULL horizon written
        -- out so both engines agree. See dbt/.
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
        -- THE PUBLICATION RULE, in SQL not prose. Clause 1: the cohort has had
        -- the full window to close. Clause 2: the day's own rows are whole — a
        -- day the source published two hours of is a biased sample of itself at
        -- any age (the 2026-08-18 stall made that shape). IS DISTINCT FROM
        -- FALSE, not = TRUE: NULL means "aged out of the assessable window",
        -- not "incomplete", and = TRUE would NULL all of production. See dbt/.
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

        -- The eligibility contract, published as data: a consumer sees WHY a
        -- rate is NULL without rederiving the rule, and the singular test
        -- compares it against an independent recomputation.
        {{ var('closure_window_days') }}                                        as closure_window_days,
        observation_days,
        is_denominator_closed,

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
