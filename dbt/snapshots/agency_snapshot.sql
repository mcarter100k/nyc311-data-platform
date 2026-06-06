{% snapshot agency_snapshot %}

{{
    config(
        target_schema = 'snapshots',
        unique_key    = 'agency_abbreviation',
        strategy      = 'check',
        check_cols    = ['agency_name'],
    )
}}

-- One row per agency_abbreviation, carrying the initcap-normalized name.
-- The check strategy opens a new version whenever agency_name changes for a
-- given abbreviation. Abbreviation changes are not tracked here — a changed
-- abbreviation is treated as a new agency (see ADR 007).
--
-- QUALIFY deduplicates within the source in case multiple service requests
-- for the same agency_abbreviation carry slightly different agency_name strings
-- in the same snapshot run (e.g., casing variants that survived Silver).
-- We pick the alphabetically first name; real name changes produce a new
-- version on the next run once the Silver data stabilises.

select
    agency_abbreviation,
    initcap(trim(agency_name)) as agency_name

from {{ ref('int_service_requests_cleaned') }}

where agency_abbreviation is not null
  and trim(agency_abbreviation) != ''

qualify row_number() over (
    partition by agency_abbreviation
    order by agency_name
) = 1

{% endsnapshot %}
