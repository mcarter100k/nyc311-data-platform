{% macro generate_date_spine(start_date=var('min_date'), end_date=var('max_date')) %}
{#
    One row per calendar day, start_date to end_date inclusive (YYYY-MM-DD
    strings). Returns a single DATE column, date_day. dbt_utils.date_spine
    excludes its end date, so one day is added to it.
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
