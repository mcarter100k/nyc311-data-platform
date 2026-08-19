-- Singular test: assert that no service request in the fact table has a negative
-- resolution_days value. A negative value indicates closed_date < created_date,
-- which is a data entry error that should have been caught by the quality filter
-- in int_service_requests_cleaned.
--
-- A passing test returns zero rows. Any rows returned cause `dbt test` to fail.
-- This test is the last line of defence before mart data reaches reporting consumers.

select
    service_request_id,
    unique_key,
    created_date,
    closed_date,
    resolution_days

from {{ ref('fct_service_requests') }}

where resolution_days < 0
