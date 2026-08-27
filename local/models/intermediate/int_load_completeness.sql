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
-- for the same 7-day window, a 17% spread, because Socrata serves identical
-- queries from replicas at different indexing states.
--
-- That spread was originally read here as NOISE. It is not, and the real
-- mechanism makes this argument stronger rather than weaker. Measured
-- 2026-08-27 with 20 probes per day (ADR 016), the disagreement is a RECENCY
-- LAG: one replica is behind, never ahead, and the gap closes monotonically
-- with a day's age — 10,427 rows at 2 days, 112 at 3, 50 at 4, 4 at 5, 2 at 6,
-- and exactly 0 from 7 days on. So a row-count threshold would not merely be
-- noisy; it would be systematically BIASED DOWNWARD on precisely the youngest
-- days, which are the days a completeness rule exists to judge. Half the
-- 65,936-vs-54,446 gap is one 2-day-old day that one replica had barely begun.
--
-- Clock coverage is immune to that bias, and for a reason the noise framing
-- did not capture: the source publishes a day as a TIME PREFIX. A replica
-- either holds the whole finished day — in which case it holds the tickets
-- filed at 23:5x, because those exist in every replica of a finished day — or
-- it holds a prefix that stops hours short and is nowhere near midnight. Both
-- states were observed for 2026-08-25 on 2026-08-27: one replica ended at
-- 02:06 with 358 rows, the other at 23:59:45 with 10,785. There is no
-- intermediate state in which a materially incomplete day passes a
-- coverage-to-midnight test, which is exactly what a count threshold cannot
-- promise. The measured separation is not marginal either — on a 14-day live
-- load every complete day ended within ONE minute of midnight and the partial
-- day ended 1,314 minutes short, so the threshold value is not load-bearing.
--
-- REPLICA-DEPENDENT, and honestly so: which of those two states answers the
-- fetch decides whether a 1-to-2-day-old day reads complete at all. The verdict
-- is correct either way — a day is marked complete only when the load actually
-- holds it to midnight — but the horizon can differ by a day between runs on
-- the same data. From 3 days on both replicas hold the day to midnight and the
-- verdict is stable.
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
