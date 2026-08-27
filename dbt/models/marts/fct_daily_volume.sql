{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

-- Grain: one row per (calendar date, borough, complaint category).
-- Pre-aggregated to avoid repeated GROUP BY in BI queries; avoids full
-- fct_service_requests scan on every dashboard render.
--
-- ══ WHY EVERY RATE ON THIS TABLE IS WINDOWED ═════════════════════════════════
--
-- A service request created recently has had less time to be closed than one
-- created a month ago. Any rate whose numerator is "closed" and whose
-- denominator is "created on this day" is therefore RIGHT-CENSORED, and the
-- censoring is worst on the newest days — the days a reader looks at first.
--
-- This table published four such measures (pct_resolved, pct_actioned,
-- avg_resolution_days, overdue_requests) over an open-ended denominator. The
-- gradient, measured on the local load with the last complete day at
-- 2026-08-24:
--
--     created_day  days_observed  pct_resolved  avg_resolution_days
--     2026-08-12        12           0.7452            1.38
--     2026-08-14        10           0.7759            1.39
--     2026-08-17         7           0.7190            1.18
--     2026-08-20         4           0.6428            0.83
--     2026-08-22         2           0.7376            0.37
--     2026-08-23         1           0.6903            0.24
--     2026-08-24         0           0.4003            0.07
--
-- 0.4003 is not a bad day for the city. It is the closure rate of a cohort that
-- has existed for zero complete days, printed next to 0.7452 in the same column
-- as though the two were comparable. avg_resolution_days falls 1.38 → 0.07 for
-- the same reason: only the fastest closures have happened yet.
--
-- THE FIX: a rate is published only where its denominator is CLOSED — where
-- every row in it has had the same, full opportunity to enter the numerator.
-- Every rate below is bounded to a fixed window of `closure_window_days`
-- ("closed within N days of creation", not "closed by now"), and a day is
-- eligible only once N complete days of history follow it. Ineligible days
-- publish NULL. n/a is a true statement about a censored cohort; 0.4003 is not.
--
-- The rule is `is_denominator_closed`, computed in SQL below from
-- int_load_completeness, and asserted against an independent recomputation by
-- tests/assert_daily_volume_rates_have_closed_denominators.sql.
--
-- WHAT IS NOT WINDOWED. total_requests — a count of requests CREATED on a day
-- is complete as soon as the source has published that day, and nothing about
-- later closures can change it. Its own qualifier is is_complete_day, which
-- this table already carried and still does.
--
-- WHY N=30. It is the NYC administrative standard already encoded in
-- fct_service_requests.is_overdue (resolution_days > 30), so the window this
-- table publishes over and the deadline the city is held to are one number, in
-- one var, rather than two that can drift. On a 7–14 day local mirror NO day is
-- eligible and every rate column reads NULL — which is the correct answer for a
-- 14-day load asked a 30-day question, and is what the change is for.
--
-- ══ overdue_requests IS GONE, AND NOT ONLY BECAUSE OF CENSORING ══════════════
--
-- It was sum(case when is_overdue then 1 else 0 end), and is_overdue is
-- three-valued: NULL while a request is open (deliberately — see
-- assert_is_overdue_null_while_open). So a request created 200 days ago and
-- still open — the worst outcome the data can express — contributed ZERO to the
-- overdue count, while one closed on day 31 contributed one. The measure
-- counted only lateness that had already ended.
--
-- requests_open_past_window replaces it: requests created on this day that were
-- NOT closed within the window. Permanently-open requests are counted, which is
-- what "overdue" was always meant to mean, and it is knowable at exactly the
-- same moment the rest of the window measures are.

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

-- The end of trustworthy history: the newest day the source has published IN
-- FULL. Identical definition to the one fct_complaint_recurrence measures its
-- observation_days against, read from the same model for the same reason — NOT
-- max(created_date), which is always the first ~2 hours of a day and would
-- credit every cohort here with up to a day of closing time it never had.
-- NULL when no day in the load is complete; the expressions below write that
-- case out rather than leaving it to GREATEST, whose NULL handling differs
-- between Snowflake (NULL) and DuckDB (0).
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

        -- ── Volume metric (NOT censored) ──────────────────────────────────────
        count(*)                                                                as total_requests,

        -- ── Decode coverage for the rates below ───────────────────────────────
        -- How many rows in this group carried resolution text that the
        -- closure_type decoder could not read. Those rows are is_actioned =
        -- FALSE by default, so this is the size of the decode-shaped floor
        -- underneath pct_actioned_within_window at exactly the grain that
        -- percentage is published at. Published unconditionally — it is a
        -- property of the rows themselves, not of how long they have been
        -- observed, so nothing about it is censored.
        sum(case when f.closure_type = 'Undecodable' then 1 else 0 end)         as undecodable_closure_requests,

        -- ── Censored numerators (gated in `final`, never selected raw) ────────
        -- "Closed within the window", not "closed by now". Bounding the
        -- numerator is half the fix; the other half is refusing to divide by a
        -- denominator that is still filling up, which happens below.
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

        -- Mean time-to-close AMONG requests closed inside the window. Stated
        -- that way on purpose: a mean over "all closures so far" is a mean over
        -- a set that grows, and grows slowest at the top. Bounded, it is a
        -- fixed quantity per cohort, and the closure rate beside it says how
        -- much of the cohort it describes.
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

observed as (

    select
        a.*,

        -- Complete days of published history following this created day. The
        -- floor at zero mirrors fct_complaint_recurrence: a cohort created
        -- after the horizon has had no observed time, not negative time. NULL
        -- horizon is written out so both engines produce NULL rather than one
        -- producing a silent zero.
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

        -- THE PUBLICATION RULE, in SQL rather than prose. Both clauses are
        -- load-bearing and they guard different things:
        --
        --   observation_days >= closure_window_days
        --       the cohort has had the full window to close. Without it the
        --       numerator is still filling and the rate reads low.
        --
        --   is_complete_day IS DISTINCT FROM FALSE
        --       the day's own row population is whole. A day the source
        --       published only the first two hours of is a biased sample of
        --       itself, so a rate over it is biased however old it is — the
        --       2026-08-18 publish stall (docs/postmortems/) produced exactly
        --       that shape. IS DISTINCT FROM FALSE, not `= TRUE`: NULL here
        --       means the day has aged out of the assessable window, which is
        --       not evidence of incompleteness, and treating it as such would
        --       NULL every rate in a warehouse whose fact outlives the load
        --       window — i.e. all of production.
        --
        -- FALSE rather than NULL when no complete day exists: the AND
        -- short-circuits on the first clause, so "we cannot assess anything"
        -- publishes nothing instead of publishing an unknown.
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

        -- ── The eligibility contract, published as data ───────────────────────
        -- Carried on every row so a consumer can see WHY a rate is NULL without
        -- rederiving the rule, and so the singular test can compare the model's
        -- own verdict against an independent recomputation.
        {{ var('closure_window_days') }}                                        as closure_window_days,
        observation_days,
        is_denominator_closed,

        -- ── Window measures: published only over a closed denominator ─────────
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
