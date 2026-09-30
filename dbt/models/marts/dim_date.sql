-- Calendar dimension: one row per day from min_date to max_date. Weekday,
-- weekend and US federal holiday flags are computed once here so every query
-- gets the same answer. Built from a spine, not the data, so a day with zero
-- requests still has a row.

with date_spine as (

    {{ generate_date_spine(
        start_date = var('min_date'),
        end_date   = var('max_date')
    ) }}

),

dates as (

    select date_day as full_date
    from date_spine

),

-- Floating holidays use week-of-month arithmetic: "3rd Monday" = day 15-21
-- and ISO weekday 1. ISO weekdays (1=Monday … 7=Sunday) are fixed by standard,
-- immune to the Snowflake WEEK_START session parameter, which would silently
-- shift plain DAYOFWEEK and break is_weekend. Observed holidays (Jul 4 on a
-- Saturday -> Friday off) are not modelled.

with_attributes as (

    select
        -- YYYYMMDD integer: compact, sortable, human-readable.
        to_char(full_date, 'YYYYMMDD')::integer                                 as date_id,
        full_date,

        -- ── Calendar hierarchy ────────────────────────────────────────────────
        year(full_date)                                                         as year,
        quarter(full_date)                                                      as quarter,
        month(full_date)                                                        as month,
        to_char(full_date, 'MMMM')                                              as month_name,
        to_char(full_date, 'MON')                                               as month_abbr,
        dayofmonth(full_date)                                                   as day_of_month,
        dayofweekiso(full_date)                                                 as day_of_week,      -- ISO: 1=Mon … 7=Sun
        decode(dayofweekiso(full_date),
            1, 'Monday',
            2, 'Tuesday',
            3, 'Wednesday',
            4, 'Thursday',
            5, 'Friday',
            6, 'Saturday',
            7, 'Sunday'
        )                                                                       as day_name,         -- 'Monday' … 'Sunday'
        to_char(full_date, 'DY')                                                as day_abbr,         -- 'Mon' … 'Sun'
        dayofyear(full_date)                                                    as day_of_year,
        weekiso(full_date)                                                      as week_of_year,     -- ISO week; immune to WEEK_OF_YEAR_POLICY

        -- ── Weekend flag ──────────────────────────────────────────────────────
        case
            when dayofweekiso(full_date) in (6, 7) then true
            else false
        end                                                                     as is_weekend,

        -- ── US Federal holiday flag ───────────────────────────────────────────
        case
            -- New Year's Day — Jan 1
            when month(full_date) = 1  and dayofmonth(full_date) = 1
                then true
            -- Martin Luther King Jr. Day — 3rd Monday of January
            when month(full_date) = 1
             and dayofweekiso(full_date) = 1
             and dayofmonth(full_date) between 15 and 21
                then true
            -- Presidents' Day (Washington's Birthday) — 3rd Monday of February
            when month(full_date) = 2
             and dayofweekiso(full_date) = 1
             and dayofmonth(full_date) between 15 and 21
                then true
            -- Memorial Day — last Monday of May
            when month(full_date) = 5
             and dayofweekiso(full_date) = 1
             and dayofmonth(full_date) between 25 and 31
                then true
            -- Juneteenth — Jun 19, federal holiday since 2021 only: the year
            -- guard keeps 2010–2020 spine dates correctly unflagged.
            when month(full_date) = 6  and dayofmonth(full_date) = 19
             and year(full_date) >= 2021
                then true
            -- Independence Day — Jul 4
            when month(full_date) = 7  and dayofmonth(full_date) = 4
                then true
            -- Labor Day — 1st Monday of September
            when month(full_date) = 9
             and dayofweekiso(full_date) = 1
             and dayofmonth(full_date) between 1 and 7
                then true
            -- Columbus Day — 2nd Monday of October
            when month(full_date) = 10
             and dayofweekiso(full_date) = 1
             and dayofmonth(full_date) between 8 and 14
                then true
            -- Veterans Day — Nov 11
            when month(full_date) = 11 and dayofmonth(full_date) = 11
                then true
            -- Thanksgiving — 4th Thursday of November
            when month(full_date) = 11
             and dayofweekiso(full_date) = 4
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
