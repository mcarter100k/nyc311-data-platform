{% macro generate_date_spine(start_date=var('min_date'), end_date=var('max_date')) %}
{#
    Generates a continuous series of calendar dates, one row per day.

    Arguments:
        start_date  — inclusive start date string (YYYY-MM-DD). Defaults to var('min_date').
        end_date    — inclusive end date string (YYYY-MM-DD).  Defaults to var('max_date').

    Returns a result set with a single column: date_day (DATE).

    Usage in a model:
        with spine as (
            {{ generate_date_spine(
                start_date=var('min_date'),
                end_date=var('max_date')
            ) }}
        )
        select cast(date_day as date) as full_date from spine

    Implementation note: dbt_utils.date_spine is exclusive of end_date, so we add
    one day to make the range inclusive of the end_date argument.
#}

    select cast(date_day as date) as date_day
    from (
        {{ dbt_utils.date_spine(
            datepart = "day",
            start_date = "cast('" ~ start_date ~ "' as date)",
            end_date   = "dateadd('day', 1, cast('" ~ end_date ~ "' as date))"
        ) }}
    ) as spine

{% endmacro %}
