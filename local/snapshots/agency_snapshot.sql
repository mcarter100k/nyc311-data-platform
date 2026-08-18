{% snapshot agency_snapshot %}

{{
    config(
        target_schema = 'snapshots',
        unique_key    = 'agency_abbreviation',
        strategy      = 'check',
        check_cols    = ['agency_name'],
    )
}}

select
    agency_abbreviation,
    initcap(trim(agency_name)) as agency_name

from {{ ref('int_service_requests_cleaned') }}

where agency_abbreviation is not null
  and trim(agency_abbreviation) != ''

-- Most recent name wins the dedup so the check strategy can detect renames
-- (mirrors dbt/snapshots/agency_snapshot.sql).
qualify row_number() over (
    partition by agency_abbreviation
    order by created_date desc, agency_name
) = 1

{% endsnapshot %}
