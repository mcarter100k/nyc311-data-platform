-- No rate on fct_daily_volume may be published over a cohort that has not
-- had the full closure window, and no eligible day may be suppressed.
--
-- The eligibility rule is recomputed here from int_load_completeness rather
-- than read off the model, so breaking the model's horizon cannot move both
-- sides together. Check (2) stops a model that publishes nothing from
-- passing.

with horizon as (

    select max(load_day) as last_complete_date
    from {{ ref('int_load_completeness') }}
    where is_complete_day

),

checked as (

    select
        v.full_date,
        v.borough,
        v.complaint_category,
        v.total_requests,
        v.is_complete_day,
        v.closure_window_days,
        v.observation_days                                                      as published_observation_days,
        v.is_denominator_closed                                                 as published_eligibility,

        -- NULL horizon written out: GREATEST(0, NULL) differs by engine.
        case
            when h.last_complete_date is null then null
            else greatest(0, datediff('day', v.full_date, h.last_complete_date))
        end                                                                     as recomputed_observation_days,

        -- FALSE, never NULL, when no complete day exists.
        (
            h.last_complete_date is not null
            and greatest(0, datediff('day', v.full_date, h.last_complete_date))
                >= v.closure_window_days
            and v.is_complete_day is distinct from false
        )                                                                       as recomputed_eligibility,

        -- Zero counts as published; only NULL is a refusal.
        (
            v.pct_closed_within_window          is not null
            or v.pct_actioned_within_window     is not null
            or v.requests_closed_within_window  is not null
            or v.requests_open_past_window      is not null
        )                                                                       as publishes_a_rate

    from {{ ref('fct_daily_volume') }} v
    cross join horizon h

)

select *
from checked
where
    -- (1) A rate published for a day without the full window of complete
    --     history, or for a day the source never finished publishing.
    (publishes_a_rate and not recomputed_eligibility)

    -- (2) An eligible, non-empty day that publishes no rate.
    or (recomputed_eligibility and total_requests > 0 and not publishes_a_rate)

    -- (3) The published eligibility flag disagrees with the recomputed rule.
    or (published_eligibility is distinct from recomputed_eligibility)

    -- (4) observation_days was measured to the wrong horizon.
    or (published_observation_days is distinct from recomputed_observation_days)
