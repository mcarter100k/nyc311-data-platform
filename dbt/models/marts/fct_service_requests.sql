{{
    config(
        materialized        = 'incremental',
        schema              = 'gold',
        unique_key          = 'service_request_id',
        incremental_strategy = 'merge',
        on_schema_change    = 'append_new_columns',
        cluster_by          = ["cast(created_date as date)"],
        post_hook           = "delete from {{ this }}
                               where service_request_id in
                                 (select service_request_id from {{ ref('stg_service_requests') }}
                                  where service_request_id not in
                                    (select service_request_id from {{ ref('int_service_requests_cleaned') }}))"
    )
}}

-- The post_hook closes the merge strategy's delete blindness: when Silver
-- CORRECTS a request so it now fails the quality filter in
-- int_service_requests_cleaned (closed_date moved before created_date), the
-- row disappears from the merge source and an ordinary incremental run would
-- keep its stale pre-correction values forever — diverging from what a
-- --full-refresh builds. The reconciliation delete removes fact rows that are
-- PRESENT in staging but ABSENT from the cleaned intermediate — i.e., rows
-- the quality filter currently quarantines — restoring
-- incremental ≡ full-refresh. Scoped through staging deliberately: rows
-- outside Silver's current coverage are untouched, so a Silver source that
-- carries a rolling window (the local mirror's live mode) never loses
-- accumulated history. NOT IN is NULL-safe here because service_request_id
-- is a not_null-tested surrogate on both sides; the anti-join costs one
-- staging scan per run.

-- The center of the star schema and the single source of truth at full detail.
-- Grain: one row per service request (~22M rows), carrying foreign keys to
-- dim_agency (point-in-time SCD2), dim_date, and dim_location, plus the core
-- measures (resolution_days, is_resolved, is_overdue). Built incrementally:
-- each run merges only the rows Silver has touched since the last watermark,
-- so daily runs stay minutes, not hours. All dimension joins are LEFT on
-- principle — a complaint with a bad agency code or un-geocodable address is
-- still counted (NULL key) rather than silently dropped. Rows are stamped with
-- schema_version at merge time; see ADR 006 for the evolution contract.

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
        -- The point-in-time join (see below) assigns the version in effect on
        -- the request's created_date, so incremental runs and --full-refresh
        -- produce identical keys for identical inputs.
        a.agency_key                                                        as agency_id,
        d.date_id                                                               as created_date_id,
        l.location_id,

        -- ── Degenerate dimensions (high cardinality; not promoted to dims) ────
        r.complaint_type,
        r.complaint_category,
        r.descriptor,
        r.channel_type,

        -- ── Coordinates ───────────────────────────────────────────────────────
        -- Carried through to Gold so geo analysis (heat maps, spatial joins)
        -- doesn't have to reach back into Silver. NULL where the address
        -- couldn't be geocoded (~15% at last manual measure; see docs/CLAIMS.md).
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
        -- is_resolved: true only for formally Closed requests; Pending/In Progress are open.
        case when r.status = 'Closed' then true else false end                  as is_resolved,

        -- is_actioned: did the city actually DO something, as opposed to
        -- closing the ticket after finding no violation / nothing there /
        -- a duplicate? Pairs with is_resolved: a request can be
        -- is_resolved = true and is_actioned = false, which is the majority
        -- case (63.3% of closed requests in a one-week live sample).
        case
            when r.closure_type in ('Resolved on Scene', 'Enforcement Action', 'Work Performed')
                then true
            else false
        end                                                                     as is_actioned,


        -- is_overdue: resolution took longer than the 30-day NYC administrative standard.
        -- NULL when resolution_days is NULL (request still open).
        case
            when r.resolution_days is null then null
            when r.resolution_days > 30    then true
            else false
        end                                                                     as is_overdue,

        -- ── Audit ─────────────────────────────────────────────────────────────
        r._loaded_at,
        -- schema_version is stamped in the staging view, where it is evaluated
        -- at query time — it only becomes durable here, when the incremental
        -- merge writes it into a physical row. After the var is bumped, rows not
        -- touched by later merges keep the version they were built under.
        -- (A --full-refresh restamps all history with the current version.)
        r.schema_version

    from requests r

    -- Point-in-time SCD2 join: pick the agency version whose validity window
    -- [valid_from, expiry_date) contains the request's created_date. Windows are
    -- half-open and non-overlapping, so each request matches at most one version
    -- (no fan-out), and the assignment is a pure function of the inputs — an
    -- incremental run and a --full-refresh produce the same agency_id for the
    -- same row. valid_from backdates each agency's first version, so requests
    -- created before the first snapshot run still join to a version.
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
