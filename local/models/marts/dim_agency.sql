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
        is_current

    from versioned

)

select * from final
