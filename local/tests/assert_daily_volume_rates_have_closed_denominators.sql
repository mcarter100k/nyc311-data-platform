-- No rate on fct_daily_volume may be published over a denominator that is still
-- in flight. This is the test that would have caught the defect it exists for:
-- the table used to publish pct_resolved / pct_actioned / avg_resolution_days /
-- overdue_requests over "every request created that day", which on the newest
-- days is a cohort that has barely had time to close anything. The same column
-- read 0.7452 at twelve complete days of observation and 0.4003 at zero.
--
-- Structurally the sibling of assert_recurrence_horizon_is_last_complete_day:
-- the eligibility rule is RECOMPUTED here from int_load_completeness rather
-- than read off the model, so sabotaging the model's horizon cannot move both
-- sides together and hide the failure.
--
-- Four disjuncts, and the second one is why this test cannot be satisfied by
-- suppressing everything. A model that published NULL unconditionally would
-- pass a naive "no censored rate exists" check perfectly, which makes that
-- check worthless the moment someone breaks the gate open in the other
-- direction. Requiring eligible non-empty days to actually publish pins both
-- edges of the rule.

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

        -- Recomputed independently of the model. Same explicit NULL-horizon
        -- handling, for the same cross-engine reason: GREATEST(0, NULL) is NULL
        -- on Snowflake and 0 on DuckDB.
        case
            when h.last_complete_date is null then null
            else greatest(0, datediff('day', v.full_date, h.last_complete_date))
        end                                                                     as recomputed_observation_days,

        -- The AND short-circuits to FALSE (never NULL) when no complete day
        -- exists, so this is a two-valued verdict on every engine.
        (
            h.last_complete_date is not null
            and greatest(0, datediff('day', v.full_date, h.last_complete_date))
                >= v.closure_window_days
            and v.is_complete_day is distinct from false
        )                                                                       as recomputed_eligibility,

        -- Any window measure carrying a value. Zero counts as published — 0 is
        -- a claim about the cohort, NULL is a refusal to make one.
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
    -- (1) THE DEFECT ITSELF. A window measure published for a day that has not
    --     had the full window of complete history behind it, or whose own row
    --     population the source never finished publishing.
    (publishes_a_rate and not recomputed_eligibility)

    -- (2) THE OPPOSITE FAILURE, and what keeps (1) from being satisfiable by a
    --     model that publishes nothing at all. A fully observed, non-empty day
    --     must produce its rates.
    or (recomputed_eligibility and total_requests > 0 and not publishes_a_rate)

    -- (3) The eligibility flag the table publishes disagrees with the rule
    --     recomputed from int_load_completeness — the flag has become
    --     decoration rather than the gate.
    or (published_eligibility is distinct from recomputed_eligibility)

    -- (4) observation_days disagrees with the recomputation: the horizon this
    --     build measured against is not the last complete day.
    or (published_observation_days is distinct from recomputed_observation_days)
