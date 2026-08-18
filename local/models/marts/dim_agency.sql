{{
    config(
        materialized = 'table',
        schema       = 'gold'
    )
}}

with snapshot as (

    select * from {{ ref('agency_snapshot') }}

),

versioned as (

    select
        agency_abbreviation,
        agency_name,
        dbt_valid_from::date                as effective_date,
        dbt_valid_to::date                  as expiry_date,
        (dbt_valid_to is null)              as is_current

    from snapshot

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['agency_abbreviation', 'effective_date']) }}
                                            as agency_key,
        agency_abbreviation,
        agency_name,
        effective_date,
        expiry_date,
        -- Join-window start for the point-in-time join in fct_service_requests;
        -- first version per agency backdated so pre-snapshot history resolves.
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
