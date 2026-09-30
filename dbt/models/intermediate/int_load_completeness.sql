-- One row per calendar day in the loaded source window, with the verdict "has
-- the source published this day in full?". The single definition of a
-- complete day: fct_complaint_recurrence and fct_daily_volume read it rather
-- than re-deriving it.
--
-- The source publishes with a ~23.5-hour lag, so the newest loaded day only
-- holds its first couple of hours. A day is COMPLETE when its newest request
-- is within `complete_day_tail_minutes` of midnight. Row counts are not used:
-- a lagging source replica serves fewer rows for days younger than 7 (ADR
-- 016), so a count threshold would be biased low on exactly the young days
-- being judged. Coverage to midnight does not have that bias, because the
-- source publishes a day as a time prefix: a replica either holds the day to
-- ~23:59 or stops hours short.
--
-- Judged per day, so any number of trailing partial or missing days is
-- handled by taking the newest complete day. Only the tail of a day is
-- checked; a day missing its early hours (possible only in `--rows N` sample
-- mode) still reads complete.
--
-- A day outside the loaded window has no row here, which means "not
-- assessable from this load", not "incomplete".

with daily as (

    select
        cast(created_date as date)                                              as load_day,
        count(*)                                                                as requests_created,
        max(created_date)                                                       as last_created_at,

        -- Audit margin for the verdict below.
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

        -- 1440 minutes in a day; datediff truncates, so 23:59:56 leaves 1.
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
