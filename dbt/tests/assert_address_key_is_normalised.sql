-- address_key must have no run of 2+ spaces and no leading/trailing space.
-- Asserts on the output, not the code: a regex that matches nothing (e.g. a
-- mis-escaped '\\s+') raises no error and silently normalises nothing.

select
    service_request_id,
    address_key

from {{ ref('fct_complaint_recurrence') }}

where address_key like '%  %'          -- collapsed whitespace means no run of 2+
   or address_key <> trim(address_key) -- and no leading/trailing space
