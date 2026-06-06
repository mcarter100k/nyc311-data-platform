-- Asserts that exactly one is_current=true row exists per agency_abbreviation.
--
-- The SCD Type 2 contract requires one active version per agency at any point
-- in time. More than one current row would fan out the fct_service_requests join
-- and silently double-count service requests. Zero current rows for a known agency
-- means the agency has no valid FK target in the fact table.
--
-- This test returns rows that violate the constraint — dbt expects 0 rows.

select
    agency_abbreviation,
    count(*) as current_version_count

from {{ ref('dim_agency') }}

where is_current = true

group by agency_abbreviation

having count(*) != 1
