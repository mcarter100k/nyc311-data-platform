-- The horizon fct_complaint_recurrence was built against must be the last
-- COMPLETE load day: not the newest (partial) loaded day, not a frozen date,
-- not NULL.
--
-- The horizon is not stored but can be recovered: for any row that was not
-- floored, closed_date + observation_days is the horizon. That is compared
-- with int_load_completeness, computed independently of the model.
--
-- A small `--rows N` sample can legitimately floor every row, so "no row
-- escaped the floor" only fails when some row closed before the last complete
-- day.

with expected as (

    select max(load_day) as last_complete_date
    from {{ ref('int_load_completeness') }}
    where is_complete_day

),

-- Rows that must carry a positive observation window if the horizon is sane.
recoverable as (

    select count(*) as n
    from {{ ref('fct_complaint_recurrence') }} f
    cross join expected e
    where cast(f.closed_date as date) < e.last_complete_date

),

-- The horizon read back out of the model; there should be exactly one.
recovered as (

    select distinct
        dateadd('day', observation_days, cast(closed_date as date))             as horizon_date
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
    -- No complete day in the load, so no horizon exists.
    expected_horizon is null

    -- Every row floored although some closed before the horizon.
    or (rows_that_should_not_be_floored > 0 and distinct_horizons = 0)

    -- More than one horizon in a single build.
    or distinct_horizons > 1

    -- One horizon, but the wrong day (e.g. the newest, partial loaded day).
    or (distinct_horizons = 1 and recovered_horizon is distinct from expected_horizon)
