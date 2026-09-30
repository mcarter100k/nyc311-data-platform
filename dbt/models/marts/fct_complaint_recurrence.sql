-- One row per closed service request that has a usable address.
--
-- 311 records what the city said happened (closure_type), not whether the
-- problem went away. The one test available from 311 alone: did the same
-- complaint come back at the same address soon after the closure?
--
-- No recurrence window is baked in. The model emits days_to_next_same_complaint
-- and observation_days, and a consumer computing a rate over N days must keep
-- only rows with observation_days >= N; otherwise tickets closed near the end
-- of the data, which have had no chance to recur, count as "did not recur".
--
-- Rebuilt each run from the current Silver window (the 37-day daily fetch),
-- so observation_days is at most ~36 and a 30-day rate covers only closures
-- from the first days of the window.
--
-- is_chronic_location marks addresses that recur by nature (one address filed
-- 236 noise complaints in a week). They dominate any unfiltered rate, so report
-- rates with and without them.

with source as (

    select
        service_request_id,
        unique_key,
        complaint_type,
        complaint_category,
        closure_type,
        created_date,
        closed_date,
        status,
        -- Address identity: upper, trim, collapse internal whitespace. That
        -- catches almost all duplicate spellings; suffix folding (STREET->ST)
        -- gained only a handful more and is not worth maintaining. Geocoding
        -- would be the real fix. The POSIX class avoids backslash escaping;
        -- DuckDB needs the 'g' flag to replace every match, Snowflake does not.
        regexp_replace(upper(trim(incident_address)), '[[:space:]]+', ' ')             as address_key

    from {{ ref('int_service_requests_cleaned') }}

    where incident_address is not null
      and trim(incident_address) <> ''

),

-- Any ticket, open or closed, can be the recurrence of an earlier one.
candidates as (

    select address_key, complaint_type, created_date
    from source

),

-- Only closed tickets are assessed: a later complaint says nothing about a
-- resolution that has not happened.
closed as (

    select *
    from source
    where closed_date is not null
      and status = 'Closed'

),

-- The horizon is the newest COMPLETE day from int_load_completeness, not
-- max(created_date): the newest loaded day is always partial, and measuring
-- against it would credit every closure with time it never had. NULL when no
-- loaded day is complete, and deliberately not defaulted.
horizon as (

    select max(load_day) as last_complete_date
    from {{ ref('int_load_completeness') }}
    where is_complete_day

),

with_next as (

    select
        c.service_request_id,
        c.unique_key,
        c.complaint_type,
        c.complaint_category,
        c.closure_type,
        c.address_key,
        c.created_date,
        c.closed_date,

        -- Days until the next same-address, same-type complaint after this
        -- closure. NULL means none was seen within the bounded window, which is
        -- not the same as "fixed"; read it with observation_days.
        min(
            datediff('day', cast(c.closed_date as date), cast(n.created_date as date))
        )                                                                       as days_to_next_same_complaint

    from closed c

    left join candidates n
        on  n.address_key     = c.address_key
        and n.complaint_type  = c.complaint_type
        and cast(n.created_date as date) >  cast(c.closed_date as date)
        and cast(n.created_date as date) <= dateadd(
                'day', {{ var('recurrence_max_window_days') }}, cast(c.closed_date as date)
            )

    group by
        c.service_request_id, c.unique_key, c.complaint_type, c.complaint_category,
        c.closure_type, c.address_key, c.created_date, c.closed_date

),

-- Tickets per (address, complaint type) in the loaded window.
location_volume as (

    select address_key, complaint_type, count(*) as location_ticket_count
    from source
    group by 1, 2

),

final as (

    select
        w.service_request_id,
        w.unique_key,
        w.address_key,
        w.complaint_type,
        w.complaint_category,
        w.closure_type,
        w.created_date,
        w.closed_date,
        w.days_to_next_same_complaint,

        -- Complete days of published history after this closure, floored at
        -- zero: rows closed after the horizon have had no observed time, and
        -- `observation_days >= N` then excludes them as it should.
        -- assert_observation_days_floor_is_explained and
        -- assert_recurrence_horizon_is_last_complete_day catch a horizon that
        -- is too far back or too far forward. The NULL case is explicit because
        -- GREATEST(0, NULL) is NULL on Snowflake but 0 on DuckDB; NULL fails
        -- the not_null test instead of filling the table with zeros.
        case
            when h.last_complete_date is null then null
            else greatest(
                0,
                datediff('day', cast(w.closed_date as date), h.last_complete_date)
            )
        end                                                                     as observation_days,

        v.location_ticket_count,

        -- A var because the right cut changes with how much history is loaded.
        case
            when v.location_ticket_count >= {{ var('chronic_location_min_tickets') }}
                then true
            else false
        end                                                                     as is_chronic_location

    from with_next w
    cross join horizon h
    left join location_volume v
        on  v.address_key    = w.address_key
        and v.complaint_type = w.complaint_type

)

select * from final
