{% macro generate_date_spine(start_date=var('min_date'), end_date=var('max_date')) %}
{#
    DuckDB-compatible override of the production macro.
    Production uses dateadd('day', 1, ...) which is Snowflake-only syntax.
    DuckDB uses interval arithmetic: date + INTERVAL '1 day'.
#}
    select cast(date_day as date) as date_day
    from (
        {{ dbt_utils.date_spine(
            datepart = "day",
            start_date = "cast('" ~ start_date ~ "' as date)",
            end_date   = "(cast('" ~ end_date ~ "' as date) + INTERVAL '1 day')"
        ) }}
    ) as spine

{% endmacro %}
