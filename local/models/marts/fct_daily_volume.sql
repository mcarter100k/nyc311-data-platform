{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

with fct as (

    select * from {{ ref('fct_service_requests') }}

),

dim_date as (

    select * from {{ ref('dim_date') }}

),

dim_location as (

    select * from {{ ref('dim_location') }}

),

aggregated as (

    select
        d.full_date,
        d.year,
        d.month,
        d.quarter,
        d.is_weekend,
        d.is_federal_holiday,
        coalesce(l.borough, 'UNSPECIFIED')                                      as borough,
        f.complaint_category,

        count(*)                                                                as total_requests,
        round(avg(f.resolution_days), 2)                                        as avg_resolution_days,
        round(
            sum(case when f.is_resolved then 1.0 else 0.0 end) / count(*),
            4
        )                                                                       as pct_resolved,

        round(
            sum(case when f.is_actioned then 1.0 else 0.0 end) / count(*),
            4
        )                                                                       as pct_actioned,
        sum(case when f.is_overdue = true then 1 else 0 end)                   as overdue_requests

    from fct f

    left join dim_date d
        on f.created_date_id = d.date_id

    left join dim_location l
        on f.location_id = l.location_id

    where d.full_date is not null

    group by
        d.full_date,
        d.year,
        d.month,
        d.quarter,
        d.is_weekend,
        d.is_federal_holiday,
        coalesce(l.borough, 'UNSPECIFIED'),
        f.complaint_category

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'full_date',
            'borough',
            'complaint_category'
        ]) }}                                                                   as daily_volume_id,
        *

    from aggregated

)

select * from final
