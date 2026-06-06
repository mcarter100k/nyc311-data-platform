with source as (

    select * from {{ source('silver', 'service_requests') }}

),

renamed as (

    select
        -- ── Identifiers ──────────────────────────────────────────────────────
        unique_key::varchar                                                     as unique_key,
        {{ dbt_utils.generate_surrogate_key(['unique_key']) }}                  as service_request_id,

        -- ── Timestamps (TIMESTAMP instead of TIMESTAMP_NTZ — DuckDB compat) ─
        created_date::timestamp                                                 as created_date,
        closed_date::timestamp                                                  as closed_date,
        resolution_action_updated_date::timestamp                               as resolution_action_updated_date,

        -- ── Agency ───────────────────────────────────────────────────────────
        agency::varchar                                                         as agency_abbreviation,
        agency_name::varchar                                                    as agency_name,

        -- ── Complaint ────────────────────────────────────────────────────────
        complaint_type::varchar                                                 as complaint_type,
        descriptor::varchar                                                     as descriptor,

        -- ── Location ─────────────────────────────────────────────────────────
        location_type::varchar                                                  as location_type,
        incident_zip::varchar                                                   as incident_zip,
        incident_address::varchar                                               as incident_address,
        street_name::varchar                                                    as street_name,
        city::varchar                                                           as city,
        borough::varchar                                                        as borough,
        community_board::varchar                                                as community_board,
        latitude::float                                                         as latitude,
        longitude::float                                                        as longitude,

        -- ── Status ───────────────────────────────────────────────────────────
        status::varchar                                                         as status,
        resolution_description::varchar                                         as resolution_description,

        -- ── Metadata ─────────────────────────────────────────────────────────
        open_data_channel_type::varchar                                         as channel_type,
        _silver_timestamp::timestamp                                            as _loaded_at,
        '{{ var("schema_version") }}'::varchar                                  as schema_version

    from source

)

select * from renamed
