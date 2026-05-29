{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

with agencies as (

    select distinct
        agency_abbreviation,
        agency_name

    from {{ ref('int_service_requests_cleaned') }}

    where agency_abbreviation is not null
      and agency_abbreviation != ''

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['agency_abbreviation']) }}         as agency_id,
        agency_abbreviation,
        -- Trim and title-case inconsistencies in the raw agency name field.
        initcap(trim(agency_name))                                              as agency_name

    from agencies

)

select * from final
