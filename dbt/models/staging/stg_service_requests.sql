with source as (

    select * from {{ source('silver', 'service_requests') }}

),

renamed as (

    select
        -- ── Identifiers ──────────────────────────────────────────────────────
        unique_key::varchar                                                     as unique_key,
        {{ dbt_utils.generate_surrogate_key(['unique_key']) }}                  as service_request_id,

        -- ── Timestamps ───────────────────────────────────────────────────────
        created_date::timestamp_ntz                                             as created_date,
        closed_date::timestamp_ntz                                              as closed_date,
        resolution_action_updated_date::timestamp_ntz                           as resolution_action_updated_date,

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
        -- Use Silver's own load timestamp, not dbt's run timestamp.
        -- _silver_timestamp is written by 03_silver.py on every INSERT and UPDATE,
        -- so it reflects when Silver last touched this row — critical for the
        -- incremental filter in fct_service_requests.
        _silver_timestamp::timestamp_ntz                                         as _loaded_at,

        -- Schema version stamp — the dbt_project.yml var `schema_version` at
        -- the time this view was compiled. NOTE: because this model is a view,
        -- the value here is evaluated at query time and always reflects the
        -- CURRENT var. The stamp only becomes a durable per-row fact in
        -- fct_service_requests, where the incremental merge writes it into
        -- physical rows; query that column (not this one) to trace historical
        -- rows to the schema contract that produced them.
        -- Increment `schema_version` in dbt_project.yml whenever a breaking
        -- change is deployed. Additive changes (new columns) do not require a bump.
        -- See ADR 006 for the full schema evolution contract.
        '{{ var("schema_version") }}'::varchar                                  as schema_version

    from source

)

select * from renamed
