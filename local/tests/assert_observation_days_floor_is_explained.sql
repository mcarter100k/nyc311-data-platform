-- A row may have observation_days = 0 only if it closed on or after the last
-- complete day. Catches a horizon that is frozen or too far back: rows then
-- floor to 0 en masse and silently drop out of every `observation_days >= N`
-- filter, which a `>= 0` test cannot see.
--
-- Compared against int_load_completeness, not the model, so a broken model
-- horizon cannot move both sides. The companion test,
-- assert_recurrence_horizon_is_last_complete_day, catches a horizon too far
-- forward.

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
