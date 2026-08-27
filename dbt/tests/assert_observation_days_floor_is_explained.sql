-- Every floored observation_days must be explainable by the row's own closure
-- date. A row may read 0 only because it closed on or after the last complete
-- day, leaving no published history behind it — never because the horizon
-- collapsed underneath it.
--
-- This is the half of the old `>= 0` test that was worth keeping, written so it
-- can fail. GREATEST(0, ...) makes a negative value unrepresentable, so the
-- interesting failure was never "a negative appeared" — it was "the negatives
-- were absorbed". A horizon that stops advancing, or one that predates the
-- data, floors rows en masse; they then drop silently out of every
-- `observation_days >= N` filter, so the analysis loses sample rather than
-- erroring, and the old test reported PASS throughout (verified by sabotage:
-- horizon set to DATE '1999-01-01').
--
-- The comparison is against int_load_completeness rather than against anything
-- the model computed, so sabotaging the model's horizon cannot move both sides
-- together.
--
-- Companion to assert_recurrence_horizon_is_last_complete_day.sql, which
-- catches the opposite error — a horizon reaching too far forward, which floors
-- nothing and inflates everything.

with expected as (

    select max(load_day) as last_complete_date
    from {{ ref('int_load_completeness') }}
    where is_complete_day

)

select
    f.service_request_id,
    f.closed_date,
    f.observation_days,
    e.last_complete_date

from {{ ref('fct_complaint_recurrence') }} f
cross join expected e

where f.observation_days = 0
  and cast(f.closed_date as date) < e.last_complete_date
