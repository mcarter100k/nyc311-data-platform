{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

-- fct_complaint_recurrence
--
-- Grain: one row per CLOSED service request that carries a usable address.
--
-- Why this exists: fct_service_requests records what the city SAID happened
-- (closure_type), and nothing in that data says whether the resident's problem
-- actually went away. This model supplies the only test available from 311
-- alone — did the same complaint reappear at the same address shortly after the
-- closure? A closure followed by an identical complaint three days later did
-- not resolve anything.
--
-- DELIBERATELY NO WINDOW IS BAKED IN. The model emits `days_to_next_same_complaint`
-- and `observation_days`; consumers choose their own threshold. Hardcoding
-- "recurred within N days" would hide right-censoring: a ticket closed on the
-- last day of loaded history has had no opportunity to recur, and counting it
-- as "did not recur" silently biases every rate downward. Filter on
-- `observation_days >= N` before computing a rate over window N.
--
-- The join is bounded by the recurrence_max_window_days var. That bound is
-- correctness-preserving for the intended question (nobody asks about
-- recurrence a year later) and it is what keeps the self-join tractable:
-- unbounded, a chronic address with hundreds of tickets fans out quadratically.
--
-- `is_chronic_location` is not decoration. Measured on one week of live data,
-- a single address carried 236 Noise - Residential complaints. Such locations
-- recur by nature rather than by failed resolution, and they dominate any
-- unfiltered recurrence rate — excluding them cut the spread between closure
-- types roughly in half. Any honest rate reports both figures.

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
        -- Address identity: upper, trim, and collapse internal whitespace.
        -- Measured before choosing: of 33,469 distinct address strings, runs of
        -- internal spaces account for 226 of the 232 collapsible duplicates —
        -- 97% of the available gain — and change the key for 4.71% of tickets
        -- ('WEST   86 STREET' vs 'WEST 86 STREET'). Suffix folding
        -- (STREET->ST, AVENUE->AVE, ...) was measured too and buys SIX more
        -- strings; it is deliberately not done, because an abbreviation table
        -- is a maintenance surface and 6 strings does not pay for one.
        -- Geocoding to a BBL/BIN remains the real fix and a separate concern.
        regexp_replace(upper(trim(incident_address)), '[[:space:]]+', ' ')             as address_key

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

-- The horizon against which observation time is measured: the newest day the
-- source has published IN FULL.
--
-- It used to be max(created_date) over the load, and that was wrong in a way
-- that survived review because it looks obviously right. The source publishes
-- on a ~23.5h lag, so the newest created_date is NEVER a whole day — it is the
-- first two hours of one (358, 372, 382, 832 rows measured, against a ~10,500
-- median). Measuring observation time against it credited every closure with
-- up to a full extra day it never had, and the error was differential across
-- closure_type, which is the dimension this table exists to compare.
--
-- The completeness rule lives in int_load_completeness, once, because
-- fct_daily_volume needs the same concept for its per-day figures. MAX over
-- complete days rather than MAX over days is also what makes a multi-day gap
-- safe: the 2026-08-18 upstream stall (docs/postmortems/) left two trailing
-- days unusable, and nothing here needs to know that.
--
-- NULL is possible — a load holding less than one complete day has no honest
-- horizon — and it is deliberately NOT defaulted to anything. See the
-- observation_days expression below for what happens then.
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
        and cast(n.created_date as date) <= dateadd(
                'day', {{ var('recurrence_max_window_days') }}, cast(c.closed_date as date)
            )

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
        -- rate computed over window N is only honest across rows where this is
        -- >= N, and that guarantee is only as good as the horizon: under the
        -- old max(created_date) horizon a row reading 3 had really had ~2.04
        -- days, because the newest loaded day contributed ~2 hours.
        --
        -- Floored at zero, and the floor is load-bearing rather than cosmetic.
        -- Any bounded load contains requests closed after the horizon — the
        -- city keeps closing tickets during the ~23.5h the newest day has not
        -- finished publishing — and the raw difference is then negative, which
        -- is not a meaning this column has: "days of published history
        -- following this closure" bottoms out at none. Zero is also the correct
        -- value for the consumer contract, since `observation_days >= N`
        -- excludes these rows from every window, exactly as right-censoring
        -- requires.
        --
        -- The floor is NOT a licence to floor everything, which is precisely
        -- what a broken horizon would do, silently and invisibly to a `>= 0`
        -- test. assert_observation_days_floor_is_explained.sql requires every
        -- floored row to be a row that closed on or after the last complete
        -- day, and assert_recurrence_horizon_is_last_complete_day.sql recovers
        -- the horizon back out of this column and compares it to
        -- int_load_completeness.
        --
        -- The NULL horizon case is explicit rather than left to GREATEST, whose
        -- NULL handling differs by warehouse: Snowflake returns NULL from
        -- GREATEST(0, NULL) while DuckDB returns 0. Relying on that would make
        -- "we have no complete day" a loud failure on one engine and a table
        -- full of silent zeros on the other. Written out, both engines produce
        -- NULL, the not_null test reddens the build, and nobody consumes a
        -- fabricated horizon.
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
