{# dbt-duckdb does not implement the 'merge' incremental strategy;
   delete+insert on the unique key has the same upsert semantics
   (the Snowflake project uses merge). #}
{{
    config(
        materialized        = 'incremental',
        schema              = 'gold',
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

{# Two reconciliation deletes, because rows leave the pipeline at two
   different points and only one of them is visible to dbt.

   The first removes rows the dbt quality filter drops: present in staging,
   absent from int. That keeps incremental ≡ full-refresh without touching
   history outside Silver's rolling window.

   The second removes rows SILVER rejected. Those never reach staging at all —
   quarantine runs in pandas before dbt sees anything — so the first delete is
   structurally blind to them, and a row loaded by an earlier run and rejected
   by a later one stayed in the fact table indefinitely. Found 2026-08-22 by an
   end-to-end run after the fetch window moved: two rows served as "In Progress"
   in Gold while the source reported them closed seconds before they were
   created. (mirrors dbt/) #}

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

        a.agency_key                                                            as agency_id,
        d.date_id                                                               as created_date_id,
        l.location_id,

        r.complaint_type,
        r.complaint_category,
        r.descriptor,
        r.channel_type,

        r.latitude,
        r.longitude,

        r.created_date,
        r.closed_date,
        r.resolution_action_updated_date,

        r.status,
        r.resolution_description,
        r.closure_type,

        r.resolution_days,

        case when r.status = 'Closed' then true else false end                  as is_resolved,

        case
            when r.closure_type in ('Resolved on Scene', 'Enforcement Action', 'Work Performed')
                then true
            else false
        end                                                                     as is_actioned,


        case
            -- Still open: the outcome is NOT YET KNOWN, so the honest answer is
            -- NULL. Keying on `resolution_days is null` alone did not achieve
            -- that: the source emits rows carrying a closed_date while status is
            -- still Open / In Progress / Assigned, so resolution_days was
            -- populated and is_overdue came out FALSE. 4,139 such rows were
            -- being counted as "on time" by
            -- `COUNT(*) FILTER (WHERE NOT is_overdue)` — precisely the failure
            -- the three-valued design exists to prevent, and which the README
            -- claimed it did prevent.
            when r.status <> 'Closed'      then null
            when r.resolution_days is null then null
            when r.resolution_days > 30    then true
            else false
        end                                                                     as is_overdue,

        r._loaded_at,
        r.schema_version

    from requests r

    -- Point-in-time SCD2 join on the half-open validity window; idempotent
    -- across incremental and full-refresh builds (mirrors dbt/models/marts).
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

    {% if is_incremental() %}
    where r._loaded_at > (
        select max(_loaded_at) - INTERVAL '1 hour'
        from {{ this }}
    )
    {% endif %}

)

select * from joined
