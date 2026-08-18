{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

-- SCD Type 2 agency dimension. Each row is one version of an agency record.
-- New versions are created by the agency_snapshot whenever agency_name changes.
-- agency_abbreviation changes are not versioned here — a new abbreviation is a
-- new agency. See ADR 007 for the full versioning contract.

with snapshot as (

    select * from {{ ref('agency_snapshot') }}

),

versioned as (

    select
        agency_abbreviation,
        agency_name,
        -- dbt_valid_from / dbt_valid_to are TIMESTAMP_TZ from the snapshot;
        -- cast to DATE because analysts query by calendar date, not time-of-day.
        dbt_valid_from::date                as effective_date,
        dbt_valid_to::date                  as expiry_date,
        -- is_current is true for the one active version per agency.
        -- NULL dbt_valid_to means "no end date yet" — i.e., current.
        (dbt_valid_to is null)              as is_current

    from snapshot

),

final as (

    select
        -- Surrogate key encodes both the agency and the version.
        -- Stable as long as (abbreviation, effective_date) is unchanged.
        -- fct_service_requests joins point-in-time on [valid_from, expiry_date),
        -- so a request keeps the key of the version in effect on its created_date
        -- regardless of when (or how often) the fact row is rebuilt. Old versions
        -- are never deleted, so those keys always resolve.
        {{ dbt_utils.generate_surrogate_key(['agency_abbreviation', 'effective_date']) }}
                                            as agency_key,
        agency_abbreviation,
        agency_name,
        effective_date,
        expiry_date,
        -- valid_from backdates each agency's FIRST version to the beginning of
        -- time: the snapshot only starts observing an agency on its first run,
        -- but requests created before that date still belong to the earliest
        -- known version. Later versions keep their true effective_date.
        case
            when row_number() over (
                partition by agency_abbreviation
                order by effective_date
            ) = 1
            then '1900-01-01'::date
            else effective_date
        end                                 as valid_from,
        is_current

    from versioned

)

select * from final
