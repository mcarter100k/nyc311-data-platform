{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

with locations as (

    select distinct
        -- Use the standardized borough from the intermediate layer, not the raw value.
        borough_clean                                                           as borough,
        coalesce(nullif(trim(community_board), ''), 'UNKNOWN')                 as community_board,
        coalesce(nullif(trim(incident_zip),    ''), 'UNKNOWN')                 as incident_zip

    from {{ ref('int_service_requests_cleaned') }}

    where borough_clean is not null

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'borough',
            'community_board',
            'incident_zip'
        ]) }}                                                                   as location_id,
        borough,
        community_board,
        incident_zip

    from locations

)

select * from final
