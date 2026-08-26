{# dbt-duckdb does not implement the 'merge' incremental strategy;
   delete+insert on the unique key has the same upsert semantics
   (the Snowflake project uses merge). #}
{{
    config(
        materialized         = 'incremental',
        schema               = 'gold',
        unique_key           = 'location_id',
        incremental_strategy = 'delete+insert',
        on_schema_change     = 'append_new_columns'
    )
}}

{# Incremental for RETENTION, not performance. Rebuilt as a table this
   dimension was reconstructed each run from Silver's rolling window, while
   fct_service_requests accumulates past it — so a combination that left the
   window vanished from the dimension and the fact's FK silently dangled. A
   Kimball dimension grows and never loses members. The grain IS the key
   (location_id hashes exactly borough + community_board + incident_zip, and
   the table holds nothing else), so members are immutable and append-only is
   the entire lifecycle. See dbt/ for the full rationale. #}

with locations as (

    select distinct
        borough_clean                                                           as borough,
        coalesce(nullif(trim(community_board), ''), 'UNKNOWN')                 as community_board,
        coalesce(nullif(trim(incident_zip),    ''), 'UNKNOWN')                 as incident_zip

    from {{ ref('int_service_requests_cleaned') }}

    where borough_clean is not null

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'borough',
            'community_board',
            'incident_zip'
        ]) }}                                                                   as location_id,
        borough,
        community_board,
        incident_zip

    from locations

)

select * from final

{% if is_incremental() %}

where location_id not in (select location_id from {{ this }})

{% endif %}
