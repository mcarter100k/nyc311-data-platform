{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

with date_spine as (

    -- generate_date_spine wraps dbt_utils.date_spine to produce one row per calendar day.
    -- Range is driven by project variables min_date / max_date (default 2010-01-01 → 2030-12-31).
    {{ generate_date_spine(
        start_date = var('min_date'),
        end_date   = var('max_date')
    ) }}

),

dates as (

    select date_day as full_date
    from date_spine

),

-- Federal holiday rules (US). Non-fixed holidays use week-of-month arithmetic:
-- "3rd Monday" = dayofmonth BETWEEN 15 AND 21 AND dayofweek = 1.
-- Snowflake default: dayofweek 0=Sunday, 1=Monday … 6=Saturday.
-- Observed holidays (e.g. Jul 4 on Saturday → observed Friday) are not modelled
-- here; extend this CTE if reporting requires observed-holiday awareness.

with_attributes as (

    select
        -- ── Primary key ───────────────────────────────────────────────────────
        -- YYYYMMDD integer — compact, sortable, joins cleanly to fct tables.
        to_char(full_date, 'YYYYMMDD')::integer                                 as date_id,
        full_date,

        -- ── Calendar hierarchy ────────────────────────────────────────────────
        year(full_date)                                                         as year,
        quarter(full_date)                                                      as quarter,
        month(full_date)                                                        as month,
        to_char(full_date, 'MMMM')                                              as month_name,
        to_char(full_date, 'MON')                                               as month_abbr,
        dayofmonth(full_date)                                                   as day_of_month,
        dayofweek(full_date)                                                    as day_of_week,      -- 0=Sun … 6=Sat
        trim(to_char(full_date, 'DAY'))                                         as day_name,         -- 'MONDAY' … 'SUNDAY'
        to_char(full_date, 'DY')                                                as day_abbr,         -- 'Mon' … 'Sun'
        dayofyear(full_date)                                                    as day_of_year,
        weekofyear(full_date)                                                   as week_of_year,

        -- ── Weekend flag ──────────────────────────────────────────────────────
        case
            when dayofweek(full_date) in (0, 6) then true
            else false
        end                                                                     as is_weekend,

        -- ── US Federal holiday flag ───────────────────────────────────────────
        case
            -- New Year's Day — Jan 1
            when month(full_date) = 1  and dayofmonth(full_date) = 1
                then true
            -- Martin Luther King Jr. Day — 3rd Monday of January
            when month(full_date) = 1
             and dayofweek(full_date) = 1
             and dayofmonth(full_date) between 15 and 21
                then true
            -- Presidents' Day (Washington's Birthday) — 3rd Monday of February
            when month(full_date) = 2
             and dayofweek(full_date) = 1
             and dayofmonth(full_date) between 15 and 21
                then true
            -- Memorial Day — last Monday of May
            when month(full_date) = 5
             and dayofweek(full_date) = 1
             and dayofmonth(full_date) between 25 and 31
                then true
            -- Juneteenth — Jun 19 (federal holiday since 2021)
            when month(full_date) = 6  and dayofmonth(full_date) = 19
                then true
            -- Independence Day — Jul 4
            when month(full_date) = 7  and dayofmonth(full_date) = 4
                then true
            -- Labor Day — 1st Monday of September
            when month(full_date) = 9
             and dayofweek(full_date) = 1
             and dayofmonth(full_date) between 1 and 7
                then true
            -- Columbus Day — 2nd Monday of October
            when month(full_date) = 10
             and dayofweek(full_date) = 1
             and dayofmonth(full_date) between 8 and 14
                then true
            -- Veterans Day — Nov 11
            when month(full_date) = 11 and dayofmonth(full_date) = 11
                then true
            -- Thanksgiving — 4th Thursday of November
            when month(full_date) = 11
             and dayofweek(full_date) = 4
             and dayofmonth(full_date) between 22 and 28
                then true
            -- Christmas — Dec 25
            when month(full_date) = 12 and dayofmonth(full_date) = 25
                then true
            else false
        end                                                                     as is_federal_holiday

    from dates

)

select * from with_attributes
