with source as (

    select * from {{ ref('stg_service_requests') }}

),

borough_standardized as (

    select
        *,
        case
            when upper(trim(borough)) in ('BROOKLYN', 'BKLYN', 'BK', 'KINGS')
                then 'BROOKLYN'
            when upper(trim(borough)) in ('MANHATTAN', 'MN', 'NEW YORK', 'NEW YORK CITY', 'NYC')
                then 'MANHATTAN'
            when upper(trim(borough)) in ('QUEENS', 'QN', 'QNS')
                then 'QUEENS'
            when upper(trim(borough)) in ('BRONX', 'THE BRONX', 'BX')
                then 'BRONX'
            when upper(trim(borough)) in ('STATEN ISLAND', 'SI', 'S.I.', 'STATEN IS')
                then 'STATEN ISLAND'
            else 'UNSPECIFIED'
        end                                                                     as borough_clean

    from source

),

with_resolution_days as (

    select
        *,
        case
            when closed_date is not null and created_date is not null
                then datediff('day', created_date, closed_date)
            else null
        end                                                                     as resolution_days

    from borough_standardized

),

with_complaint_category as (

    select
        *,
        case
            when complaint_type ilike '%noise%'
                then 'Noise'
            when complaint_type ilike '%heat%'
              or complaint_type ilike '%hot water%'
                then 'Heat & Hot Water'
            when complaint_type ilike '%street%light%'
              or complaint_type ilike '%streetlight%'
                then 'Street Light'
            when complaint_type ilike '%rodent%'
              or complaint_type ilike '%mice%'
              or complaint_type ilike '%rat%'
                then 'Rodent'
            when complaint_type ilike '%illegal park%'
              or complaint_type ilike '%blocked driveway%'
                then 'Illegal Parking'
            when complaint_type ilike '%sanitation%'
              or complaint_type ilike '%dirty condition%'
              or complaint_type ilike '%missed collection%'
                then 'Sanitation'
            when complaint_type ilike '%water%'
              and complaint_type not ilike '%hot water%'
                then 'Water'
            when complaint_type ilike '%graffiti%'
                then 'Graffiti'
            when complaint_type ilike '%homeless%'
              or complaint_type ilike '%encampment%'
                then 'Homeless Services'
            when complaint_type ilike '%pothole%'
              or complaint_type ilike '%pavement%'
              or complaint_type ilike '%street condition%'
                then 'Street Condition'
            when complaint_type ilike '%tree%'
              or complaint_type ilike '%overgrown%'
                then 'Parks & Trees'
            else 'Other'
        end                                                                     as complaint_category

    from with_resolution_days

),

quality_filtered as (

    select *
    from with_complaint_category
    where resolution_days is null
       or resolution_days >= 0

)

select * from quality_filtered
