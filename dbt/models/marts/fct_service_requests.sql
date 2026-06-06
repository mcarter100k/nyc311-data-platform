{{
    config(
        materialized        = 'incremental',
        schema              = 'gold',
        unique_key          = 'service_request_id',
        incremental_strategy = 'merge',
        cluster_by          = ["cast(created_date as date)"]
    )
}}

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

-- Join each dimension to the fact using its natural key.
-- All dimension joins are LEFT to preserve service requests that lack a
-- geocodable location or have an unrecognized agency code — dropping them
-- would silently under-count complaint volume, which is worse than a NULL FK.

joined as (

    select
        -- ── Surrogate key (grain: one row per service request) ────────────────
        r.service_request_id,

        -- ── Natural key ───────────────────────────────────────────────────────
        r.unique_key,

        -- ── Foreign keys ──────────────────────────────────────────────────────
        -- agency_key is the SCD2 version-specific surrogate from dim_agency.
        -- The dim_agency join is restricted to is_current=true (see below) so
        -- this always carries the key of the version active at load time.
        a.agency_key                                                        as agency_id,
        d.date_id                                                               as created_date_id,
        l.location_id,

        -- ── Degenerate dimensions (high cardinality; not promoted to dims) ────
        r.complaint_type,
        r.complaint_category,
        r.descriptor,
        r.channel_type,

        -- ── Dates ─────────────────────────────────────────────────────────────
        r.created_date,
        r.closed_date,
        r.resolution_action_updated_date,

        -- ── Status ────────────────────────────────────────────────────────────
        r.status,
        r.resolution_description,

        -- ── Measures ──────────────────────────────────────────────────────────
        r.resolution_days,

        -- ── Derived flags ─────────────────────────────────────────────────────
        -- is_resolved: true only for formally Closed requests; Pending/In Progress are open.
        case when r.status = 'Closed' then true else false end                  as is_resolved,

        -- is_overdue: resolution took longer than the 30-day NYC administrative standard.
        -- NULL when resolution_days is NULL (request still open).
        case
            when r.resolution_days is null then null
            when r.resolution_days > 30    then true
            else false
        end                                                                     as is_overdue,

        -- ── Audit ─────────────────────────────────────────────────────────────
        r._loaded_at

    from requests r

    -- Restrict to the current version so the join returns exactly one dim_agency
    -- row per agency_abbreviation. Without is_current=true, the SCD2 table would
    -- fan out the fact: one row per historical name change per service request.
    left join dim_agency a
        on r.agency_abbreviation = a.agency_abbreviation
       and a.is_current = true

    left join dim_date d
        on cast(r.created_date as date) = d.full_date

    left join dim_location l
        on r.borough_clean                                                      = l.borough
        and coalesce(nullif(trim(r.community_board), ''), 'UNKNOWN')           = l.community_board
        and coalesce(nullif(trim(r.incident_zip),    ''), 'UNKNOWN')           = l.incident_zip

    -- Incremental filter: on non-first runs, only process rows Silver has
    -- touched since one hour before the last recorded _loaded_at in this table.
    -- The one-hour lookback prevents missing rows at the boundary between runs
    -- due to clock skew or in-flight records.
    -- On the first run (full build) this block is skipped entirely.
    {% if is_incremental() %}
    where r._loaded_at > (
        select dateadd('hour', -1, max(_loaded_at))
        from {{ this }}
    )
    {% endif %}

)

select * from joined
