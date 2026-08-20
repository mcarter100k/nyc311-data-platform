-- Guards address_key normalisation against a SILENT no-op.
--
-- The first attempt at this used '\\s+' as the pattern. In SQL that is a
-- literal backslash followed by 's+', so it matched nothing — and a regex that
-- matches nothing raises no error. The build passed, the column looked right,
-- and 475 rows kept their double spaces. Nothing failed; the fix simply had no
-- effect.
--
-- That is the failure mode this test exists for. A normalisation step that
-- can silently do nothing needs an assertion on its OUTPUT, not on its code.

select
    service_request_id,
    address_key

from {{ ref('fct_complaint_recurrence') }}

where address_key like '%  %'          -- collapsed whitespace means no run of 2+
   or address_key <> trim(address_key) -- and no leading/trailing space
