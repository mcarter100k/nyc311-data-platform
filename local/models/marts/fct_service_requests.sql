{{
    config(
        materialized        = 'incremental',
        schema              = 'gold',
        unique_key          = 'service_request_id',
        incremental_strategy = 'merge'
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

        r.created_date,
        r.closed_date,
        r.resolution_action_updated_date,

        r.status,
        r.resolution_description,

        r.resolution_days,

        case when r.status = 'Closed' then true else false end                  as is_resolved,

        case
            when r.resolution_days is null then null
            when r.resolution_days > 30    then true
            else false
        end                                                                     as is_overdue,

        r._loaded_at

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
