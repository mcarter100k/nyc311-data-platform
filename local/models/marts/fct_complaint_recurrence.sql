{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

-- fct_complaint_recurrence — grain: one closed service request with an address.
--
-- Did the same complaint reappear at the same address after this closure? The
-- only test of resolution quality available from 311 alone.
--
-- NO window is baked in: consumers filter on observation_days >= N before
-- computing a rate over window N, or right-censoring biases it downward.
-- is_chronic_location matters — one address carried 236 Noise complaints in a
-- single week; such locations recur by nature, not by failed resolution.
-- See dbt/ for the full rationale.

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
        -- upper + trim + collapse internal whitespace. DuckDB needs the 'g'
        -- flag; Snowflake's regexp_replace is global by default. The POSIX class
        -- avoids backslash escaping, which silently produced a literal '\\s' and
        -- matched nothing. Suffix folding
        -- measured and rejected — it buys 6 strings. See dbt/.
        regexp_replace(upper(trim(incident_address)), '[[:space:]]+', ' ', 'g')        as address_key

    from {{ ref('int_service_requests_cleaned') }}

    where incident_address is not null
      and trim(incident_address) <> ''

),

-- Every ticket is a candidate *recurrence* of an earlier one, open or closed.
candidates as (

    select address_key, complaint_type, created_date
    from source

),

-- Only closed tickets can be assessed: an open ticket has not been resolved,
-- so a later complaint is not evidence about a resolution.
closed as (

    select *
    from source
    where closed_date is not null
      and status = 'Closed'

),

-- The horizon: the newest day the source has published IN FULL. NOT
-- max(created_date) — the source lags ~23.5h, so the newest loaded day is
-- always the first ~2 hours of one (358/372/382/832 rows vs a ~10,500 median),
-- and measuring against it credited every closure with up to a day it never
-- had. The completeness rule lives in int_load_completeness, once, because
-- fct_daily_volume needs it too. MAX over COMPLETE days also absorbs a
-- multi-day publish gap. NULL when no day is complete — see below. See dbt/.
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

        -- Days until the next same-address, same-type complaint was filed after
        -- this one closed. NULL means none was observed inside the bounded
        -- window — which is NOT the same as "the problem was fixed"; read it
        -- together with observation_days.
        min(
            datediff('day', cast(c.closed_date as date), cast(n.created_date as date))
        )                                                                       as days_to_next_same_complaint

    from closed c

    left join candidates n
        on  n.address_key     = c.address_key
        and n.complaint_type  = c.complaint_type
        and cast(n.created_date as date) >  cast(c.closed_date as date)
        and cast(n.created_date as date) <= cast(c.closed_date as date)
                + to_days({{ var('recurrence_max_window_days') }})

    group by
        c.service_request_id, c.unique_key, c.complaint_type, c.complaint_category,
        c.closure_type, c.address_key, c.created_date, c.closed_date

),

-- Ticket volume per (address, complaint type) across the loaded window.
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

        -- How many days of COMPLETELY PUBLISHED history follow this closure. A
        -- rate over window N is only honest where this is >= N — and under the
        -- old horizon a row reading 3 had really had ~2.04 days.
        -- Floored at zero: a request closed after the horizon has no observed
        -- time, not negative time. The floor is not a licence to floor
        -- everything, which is what a broken horizon does silently; the two
        -- singular tests in tests/ hold it to that.
        -- The NULL horizon is written out rather than left to GREATEST, whose
        -- NULL handling differs by engine (Snowflake NULL, DuckDB 0) — so "no
        -- complete day" reddens the build instead of yielding silent zeros.
        -- See dbt/ for the full rationale.
        case
            when h.last_complete_date is null then null
            else greatest(
                0,
                datediff('day', cast(w.closed_date as date), h.last_complete_date)
            )
        end                                                                     as observation_days,

        v.location_ticket_count,

        -- Chronic locations recur regardless of how any single ticket was
        -- closed. Threshold is deliberately low and deliberately a var: at one
        -- week of history 5 tickets at one address is already exceptional, and
        -- the right cut changes as history deepens.
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
