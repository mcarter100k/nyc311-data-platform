{% snapshot agency_snapshot %}

{{
    config(
        target_schema = 'snapshots',
        unique_key    = 'agency_abbreviation',
        strategy      = 'check',
        check_cols    = ['agency_name'],
    )
}}

-- One row per agency_abbreviation with its title-cased name. The check
-- strategy opens a new version when agency_name changes; a new abbreviation
-- is a new agency (ADR 007).
--
-- The dedup keeps the name on the most recent request, so a rename wins as
-- soon as it appears and the snapshot can detect it. agency_name breaks ties.

select
    agency_abbreviation,
    initcap(trim(agency_name)) as agency_name

from {{ ref('int_service_requests_cleaned') }}

where agency_abbreviation is not null
  and trim(agency_abbreviation) != ''

qualify row_number() over (
    partition by agency_abbreviation
    order by created_date desc, agency_name
) = 1

{% endsnapshot %}
