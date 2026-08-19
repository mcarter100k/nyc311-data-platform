{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

-- Location dimension derived from the data itself: one row per distinct
-- (borough, community_board, incident_zip) combination that has ever appeared,
-- giving analysts a three-level geographic drill path. There is no authoritative
-- upstream location list, so the domain grows as new combinations arrive.
-- CONTRACT: the UNKNOWN-coalescing on community_board and incident_zip below
-- must stay byte-identical to the join keys in fct_service_requests — if the
-- two drift, fact rows silently lose their location_id.

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
