{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

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

-- Federal holiday rules (US). Non-fixed holidays use week-of-month arithmetic.
-- DuckDB isodow: 1=Monday … 7=Sunday — mirrors dayofweekiso in the Snowflake
-- project, keeping day_of_week and is_weekend semantics identical across both.

with_attributes as (

    select
        -- ── Primary key ───────────────────────────────────────────────────────
        -- DuckDB strftime('%Y%m%d') instead of Snowflake to_char(date, 'YYYYMMDD').
        CAST(strftime(full_date, '%Y%m%d') AS INTEGER)                          as date_id,
        full_date,

        -- ── Calendar hierarchy ────────────────────────────────────────────────
        year(full_date)                                                         as year,
        quarter(full_date)                                                      as quarter,
        month(full_date)                                                        as month,
        strftime(full_date, '%B')                                               as month_name,
        strftime(full_date, '%b')                                               as month_abbr,
        dayofmonth(full_date)                                                   as day_of_month,
        isodow(full_date)                                                       as day_of_week,      -- ISO: 1=Mon … 7=Sun
        strftime(full_date, '%A')                                               as day_name,
        left(strftime(full_date, '%A'), 3)                                      as day_abbr,
        dayofyear(full_date)                                                    as day_of_year,
        weekofyear(full_date)                                                   as week_of_year,

        -- ── Weekend flag ──────────────────────────────────────────────────────
        case
            when isodow(full_date) in (6, 7) then true
            else false
        end                                                                     as is_weekend,

        -- ── US Federal holiday flag ───────────────────────────────────────────
        case
            when month(full_date) = 1  and dayofmonth(full_date) = 1
                then true
            when month(full_date) = 1
             and isodow(full_date) = 1
             and dayofmonth(full_date) between 15 and 21
                then true
            when month(full_date) = 2
             and isodow(full_date) = 1
             and dayofmonth(full_date) between 15 and 21
                then true
            when month(full_date) = 5
             and isodow(full_date) = 1
             and dayofmonth(full_date) between 25 and 31
                then true
            when month(full_date) = 6  and dayofmonth(full_date) = 19
             and year(full_date) >= 2021
                then true
            when month(full_date) = 7  and dayofmonth(full_date) = 4
                then true
            when month(full_date) = 9
             and isodow(full_date) = 1
             and dayofmonth(full_date) between 1 and 7
                then true
            when month(full_date) = 10
             and isodow(full_date) = 1
             and dayofmonth(full_date) between 8 and 14
                then true
            when month(full_date) = 11 and dayofmonth(full_date) = 11
                then true
            when month(full_date) = 11
             and isodow(full_date) = 4
             and dayofmonth(full_date) between 22 and 28
                then true
            when month(full_date) = 12 and dayofmonth(full_date) = 25
                then true
            else false
        end                                                                     as is_federal_holiday

    from dates

)

select * from with_attributes
