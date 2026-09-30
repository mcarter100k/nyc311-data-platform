-- Exactly one is_current row per agency_abbreviation. Two would fan out the
-- fct_service_requests join; zero leaves the agency with no current version.
-- Counted without filtering on is_current first, so the zero case can surface.

with version_counts as (

    select
        agency_abbreviation,
        sum(case when is_current then 1 else 0 end) as current_version_count

    from {{ ref('dim_agency') }}

    group by agency_abbreviation

)

select *
from version_counts
where current_version_count != 1
