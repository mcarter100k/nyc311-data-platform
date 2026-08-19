-- Asserts that exactly one is_current=true row exists per agency_abbreviation.
--
-- The SCD Type 2 contract requires one active version per agency at any point
-- in time. More than one current row would fan out the fct_service_requests join
-- and silently double-count service requests. Zero current rows for a known agency
-- means the agency has no valid FK target in the fact table.
--
-- This test returns rows that violate the constraint — dbt expects 0 rows.

-- Count current versions per agency WITHOUT pre-filtering on is_current:
-- filtering first would drop zero-current agencies before grouping, so the
-- zero case (an agency whose only current row was wrongly expired) could
-- never surface — the test would only ever catch duplicates.

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
