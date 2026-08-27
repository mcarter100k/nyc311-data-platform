-- The horizon fct_complaint_recurrence was actually built against must be the
-- last COMPLETE load day — not the newest loaded day, not a frozen date, not
-- NULL. This is the test that would have caught the defect it exists for.
--
-- The horizon is not stored but IS recoverable: for any row that was not
-- floored, closed_date + observation_days is the horizon exactly. It is
-- compared against int_load_completeness, computed independently of whatever
-- the model did. The `recoverable` guard keeps a legitimately all-floored load
-- (less than a full day past the last complete day, e.g. a small --rows
-- sample) from reddening the build. See dbt/ for the full rationale.

with expected as (

    select max(load_day) as last_complete_date
    from {{ ref('int_load_completeness') }}
    where is_complete_day

),

recoverable as (

    select count(*) as n
    from {{ ref('fct_complaint_recurrence') }} f
    cross join expected e
    where cast(f.closed_date as date) < e.last_complete_date

),

recovered as (

    select distinct
        cast(cast(closed_date as date) + to_days(observation_days) as date)     as horizon_date
    from {{ ref('fct_complaint_recurrence') }}
    where observation_days > 0

),

verdict as (

    select
        (select last_complete_date from expected)                               as expected_horizon,
        (select min(horizon_date) from recovered)                               as recovered_horizon,
        (select count(*) from recovered)                                        as distinct_horizons,
        (select n from recoverable)                                             as rows_that_should_not_be_floored

)

select *
from verdict
where
    -- No complete day in the load: no honest horizon exists, and the model
    -- must not have invented one.
    expected_horizon is null

    -- Everything floored while rows existed that should not have been: the
    -- horizon has stopped advancing, or predates the data.
    or (rows_that_should_not_be_floored > 0 and distinct_horizons = 0)

    -- More than one horizon in a single build.
    or distinct_horizons > 1

    -- The original defect: recovered = newest loaded (partial) day, expected =
    -- last complete one.
    or (distinct_horizons = 1 and recovered_horizon is distinct from expected_horizon)
