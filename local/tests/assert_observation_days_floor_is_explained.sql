-- Every floored observation_days must be explainable by the row's own closure
-- date: a row may read 0 only because it closed on or after the last complete
-- day, never because the horizon collapsed underneath it.
--
-- This is the half of the old `>= 0` test that was worth keeping, written so
-- it can fail. GREATEST(0, ...) makes a negative unrepresentable, so the real
-- failure was never "a negative appeared" but "the negatives were absorbed" —
-- a frozen or backdated horizon floors rows en masse and they drop silently
-- out of every `observation_days >= N` filter. Compared against
-- int_load_completeness, so sabotaging the model's horizon cannot move both
-- sides together. See dbt/ for the full rationale.

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
