{{
    config(
        materialized        = 'incremental',
        unique_key          = 'service_request_id',
        incremental_strategy = 'delete+insert',
        on_schema_change    = 'append_new_columns',
        post_hook           = [
            "delete from {{ this }}
             where service_request_id in
               (select service_request_id from {{ ref('stg_service_requests') }}
                where service_request_id not in
                  (select service_request_id from {{ ref('int_service_requests_cleaned') }}))",
            "delete from {{ this }}
             where unique_key in
               (select unique_key from {{ ref('stg_quarantine') }})",
        ]
    )
}}

-- The center of the star schema: one row per service request, with foreign
-- keys to dim_agency (point-in-time SCD2), dim_date and dim_location. Every
-- dimension join is LEFT, so a request with an unknown agency or location is
-- still counted, with a NULL key. Incremental: each run merges what Silver
-- wrote (merge on Snowflake; the DuckDB copy uses delete+insert, since
-- dbt-duckdb has no merge).
--
-- Two post_hook deletes keep incremental runs equal to a full refresh:
-- (1) rows present in staging but dropped by the int quality filter;
-- (2) rows Silver quarantined this run, which never reach staging so (1)
--     cannot see them.
-- Both are scoped to what Silver currently holds, so history outside the
-- fetch window is never touched.

with requests as (

    select * from {{ ref('int_service_requests_cleaned') }}

),

dim_agency as (

    select * from {{ ref('dim_agency') }}

),

dim_date as (

    select * from {{ ref('dim_date') }}

),

dim_location as (

    select * from {{ ref('dim_location') }}

),

joined as (

    select
        r.service_request_id,
        r.unique_key,

        -- ── Foreign keys ──────────────────────────────────────────────────────
        a.agency_key                                                            as agency_id,
        d.date_id                                                               as created_date_id,
        l.location_id,

        -- ── Degenerate dimensions (high cardinality; not promoted to dims) ────
        r.complaint_type,
        r.complaint_category,
        r.descriptor,
        r.channel_type,

        -- ── Coordinates (NULL where the address was not geocodable) ───────────
        r.latitude,
        r.longitude,

        -- ── Dates ─────────────────────────────────────────────────────────────
        r.created_date,
        r.closed_date,
        r.resolution_action_updated_date,

        -- ── Status ────────────────────────────────────────────────────────────
        r.status,
        r.resolution_description,
        r.closure_type,

        -- ── Measures ──────────────────────────────────────────────────────────
        r.resolution_days,

        -- ── Derived flags ─────────────────────────────────────────────────────
        -- Only formally Closed counts; Pending and In Progress are open.
        case when r.status = 'Closed' then true else false end                  as is_resolved,

        -- Did the city do something, rather than close after finding no
        -- violation, nothing there, or a duplicate? Most closed requests are
        -- is_resolved and not is_actioned.
        case
            when r.closure_type in ('Resolved on Scene', 'Enforcement Action', 'Work Performed')
                then true
            else false
        end                                                                     as is_actioned,

        -- Took longer than closure_window_days (NYC's 30-day standard). NULL
        -- until status is Closed: the source sometimes sends a closed_date
        -- while the request is still open.
        case
            when r.status <> 'Closed'      then null
            when r.resolution_days is null then null
            when r.resolution_days > {{ var('closure_window_days') }} then true
            else false
        end                                                                     as is_overdue,

        -- ── Audit ─────────────────────────────────────────────────────────────
        r._loaded_at,
        r.schema_version

    from requests r

    -- Point-in-time SCD2 join: the version whose [valid_from, expiry_date)
    -- window contains created_date. Windows do not overlap, so there is no
    -- fan-out, and incremental and full-refresh builds assign the same key.
    left join dim_agency a
        on r.agency_abbreviation = a.agency_abbreviation
       and cast(r.created_date as date) >= a.valid_from
       and cast(r.created_date as date) <  coalesce(a.expiry_date, '9999-12-31'::date)

    left join dim_date d
        on cast(r.created_date as date) = d.full_date

    left join dim_location l
        on r.borough_clean                                                      = l.borough
        and coalesce(nullif(trim(r.community_board), ''), 'UNKNOWN')           = l.community_board
        and coalesce(nullif(trim(r.incident_zip),    ''), 'UNKNOWN')           = l.incident_zip

    -- Re-merge the current Silver window. Silver stamps one _loaded_at per
    -- run, so this admits every row it holds; the 1-hour margin is a buffer
    -- for a loader that lands rows slightly out of order.
    {% if is_incremental() %}
    where r._loaded_at > (
        select max(_loaded_at) - INTERVAL '1 hour'
        from {{ this }}
    )
    {% endif %}

)

select * from joined
