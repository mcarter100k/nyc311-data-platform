{{
    config(
        materialized         = 'incremental',
        unique_key           = 'location_id',
        incremental_strategy = 'delete+insert',
        on_schema_change     = 'append_new_columns'
    )
}}

-- One row per (borough, community_board, incident_zip) ever seen in the data;
-- there is no upstream location list. The UNKNOWN coalescing below must match
-- the join keys in fct_service_requests exactly, or fact rows lose their
-- location_id.
--
-- Incremental so members are never dropped: the fact keeps history longer
-- than Silver's window, and its location_id must keep resolving. Members never
-- change (the three columns ARE the key), so insert-only is enough.
-- Full-refresh this together with fct_service_requests, never alone.

with locations as (

    select distinct
        borough_clean                                                           as borough,
        coalesce(nullif(trim(community_board), ''), 'UNKNOWN')                 as community_board,
        coalesce(nullif(trim(incident_zip),    ''), 'UNKNOWN')                 as incident_zip

    from {{ ref('int_service_requests_cleaned') }}

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

{% if is_incremental() %}

-- Append only new members. NOT IN is safe: location_id is never NULL.
where location_id not in (select location_id from {{ this }})

{% endif %}
