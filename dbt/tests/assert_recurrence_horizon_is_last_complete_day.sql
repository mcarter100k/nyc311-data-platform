-- The horizon fct_complaint_recurrence was actually built against must be the
-- last COMPLETE load day — not the newest loaded day, not a frozen date, not
-- NULL.
--
-- This is the test that would have caught the defect it exists for. The model
-- measured observation_days against max(created_date), which the source's
-- ~23.5h publish lag guarantees is a two-hour-long day; every row was credited
-- with up to a day of observation it never had, and the bias was differential
-- across closure_type — the one dimension the table exists to compare.
--
-- HOW IT WORKS. The horizon is not stored, but it is recoverable: for any row
-- that was NOT floored, closed_date + observation_days IS the horizon, exactly.
-- Recovering it from the built artifact and comparing against
-- int_load_completeness — computed independently of whatever the model did —
-- catches a wrong horizon whichever side produced it.
--
-- The `recoverable` guard exists because "no row escaped the floor" is only
-- suspicious when some row SHOULD have. A load holding less than one full day
-- past its last complete day (local_runner.py --rows on a small sample) has
-- every closure at or after the horizon, floors all of them legitimately, and
-- must not redden the build. When rows that closed BEFORE the last complete day
-- exist and every one of them is still floored, the horizon is broken, and the
-- second clause below says so.

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

-- The horizon read back out of the artifact. One distinct value, or the model
-- is not applying a single horizon at all.
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
    -- No complete day in the load at all: there is no honest horizon, and the
    -- model must not have invented one.
    expected_horizon is null

    -- Every row floored while rows existed that should not have been: the
    -- horizon has stopped advancing, or predates the data.
    or (rows_that_should_not_be_floored > 0 and distinct_horizons = 0)

    -- More than one horizon in a single build: the cross join to the horizon
    -- CTE is no longer a single row.
    or distinct_horizons > 1

    -- The horizon exists and is the wrong day. This is the original defect:
    -- recovered = the newest loaded (partial) day, expected = the last complete
    -- one.
    or (distinct_horizons = 1 and recovered_horizon is distinct from expected_horizon)
