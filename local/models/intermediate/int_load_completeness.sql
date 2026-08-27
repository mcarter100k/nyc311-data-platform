-- int_load_completeness — grain: one row per calendar day present in the load.
--
-- THE PROBLEM THIS SOLVES. The source publishes with roughly a 23.5-hour lag,
-- so the newest created_date in any load is never a whole day: it holds the
-- first couple of hours and stops. Measured on this project's own loads, that
-- trailing day carried 358, 372, 382 and 832 rows against a ~10,500 median.
-- Anything that treats "the newest loaded day" as the end of observed history
-- therefore credits every row with up to a full day of observation it never
-- had. fct_complaint_recurrence did exactly that, and the resulting bias was
-- differential: it fell hardest on the closure types with the fewest closures
-- near the horizon, which is the comparison the analysis leads on.
--
-- THE DEFINITION. A day is COMPLETE when the source's coverage of it reaches
-- the final `complete_day_tail_minutes` of that day — i.e. the newest request
-- created on that day sits within that many minutes of midnight. Everything
-- downstream that needs "where does trustworthy history end" reads
-- `is_complete_day` from here rather than re-deriving it.
--
-- WHY NOT A ROW COUNT. The obvious alternative — "a day is complete when it
-- holds at least X rows, or at least X% of the trailing median" — is a
-- threshold on a quantity the source itself is not consistent about. Two
-- identical fetch queries issued minutes apart returned 65,936 and 54,446 rows
-- on 2026-08-26, a 17% spread, because Socrata serves identical queries from
-- replicas at different indexing states. A completeness rule built on raw
-- counts inherits that noise directly and would flip verdicts between runs.
-- Clock coverage does not: a thin replica of a finished day still contains
-- tickets filed at 23:5x, because those tickets exist in every replica of it.
-- The measured separation is not marginal either — on a 14-day live load every
-- complete day ended within ONE minute of midnight and the partial day ended
-- 1,314 minutes short, so the exact value of the threshold is not load-bearing.
--
-- WHY NOT A HARDCODED DATE. Obviously: the load moves every run. But also
-- because the trailing gap is not always one day. The 2026-08-18 upstream
-- publish stall (docs/postmortems/) left one day ~96% empty and the next day
-- entirely absent for 21+ hours. Completeness is evaluated per day and the
-- horizon is the MAXIMUM complete day, so an arbitrary number of trailing
-- partial or missing days is handled without special-casing.
--
-- KNOWN LIMIT, stated rather than hidden: this measures the TAIL of each day.
-- A day whose EARLY hours are missing still reads as complete. That shape is
-- not produced by the source's publish lag — it is produced by
-- `local_runner.py --rows N` sample mode, which walks created_date descending
-- and truncates the oldest day it reaches. In `--live` mode the fetch window
-- is day-aligned, so heads are intact and the tail is the only open end.
--
-- The population is the loaded source window (int_service_requests_cleaned),
-- not the accumulating fact. A day that has aged out of that window is absent
-- here, which is why consumers joining on date must treat a missing row as
-- "not assessable from this load" rather than as "incomplete".

with daily as (

    select
        cast(created_date as date)                                              as load_day,
        count(*)                                                                as requests_created,
        max(created_date)                                                       as last_created_at,

        -- Observable margin, so the verdict below can be audited rather than
        -- trusted. On the 14-day live load referenced above this ran 311–695
        -- for every complete day and 0 for the partial one.
        sum(
            case
                when datediff('minute', cast(created_date as date), created_date)
                     >= 1440 - {{ var('complete_day_tail_minutes') }}
                then 1
                else 0
            end
        )                                                                       as requests_in_tail_window

    from {{ ref('int_service_requests_cleaned') }}

    where created_date is not null

    group by 1

),

final as (

    select
        load_day,
        requests_created,
        last_created_at,
        requests_in_tail_window,

        -- Minutes between the newest request created on this day and the day's
        -- end. 1440 = minutes in a day; datediff truncates, so a request at
        -- 23:59:56 scores 1439 and leaves a 1-minute gap.
        1440 - datediff('minute', load_day, last_created_at)                    as minutes_short_of_midnight,

        case
            when 1440 - datediff('minute', load_day, last_created_at)
                 <= {{ var('complete_day_tail_minutes') }}
                then true
            else false
        end                                                                     as is_complete_day

    from daily

)

select * from final
