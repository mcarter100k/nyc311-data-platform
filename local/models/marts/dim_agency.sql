-- SCD Type 2 agency dimension: one row per version of an agency, opened by
-- agency_snapshot when agency_name changes (ADR 007).
--
-- Accepted limit: validity windows are whole days, so a rename detected
-- mid-day also assigns that day's earlier requests to the new version. Windows
-- stay half-open and non-overlapping, so there is no fan-out.

with snapshot as (

    select * from {{ ref('agency_snapshot') }}

),

versioned as (

    select
        agency_abbreviation,
        agency_name,
        -- Full timestamp: keys and orders two versions opened the same day.
        dbt_valid_from                      as version_opened_at,
        dbt_valid_from::date                as effective_date,
        dbt_valid_to::date                  as expiry_date,
        -- NULL dbt_valid_to means the version is still open.
        (dbt_valid_to is null)              as is_current

    from snapshot

),

final as (

    select
        -- One key per version. Built from the full timestamp, not
        -- effective_date, so two versions opened the same day do not collide.
        {{ dbt_utils.generate_surrogate_key(['agency_abbreviation', 'version_opened_at']) }}
                                            as agency_key,
        agency_abbreviation,
        agency_name,
        effective_date,
        expiry_date,
        -- The first version is backdated so requests created before the
        -- snapshot first saw the agency still join to a version.
        case
            when row_number() over (
                partition by agency_abbreviation
                order by version_opened_at
            ) = 1
            then '1900-01-01'::date
            else effective_date
        end                                 as valid_from,
        is_current

    from versioned

)

select * from final
