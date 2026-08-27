-- is_overdue must be NULL for anything not closed.
--
-- The three-valued design exists so that `COUNT(*) FILTER (WHERE NOT is_overdue)`
-- cannot count an open request as "on time" — a boolean FALSE would silently
-- inflate every resolution-rate measure built on it.
--
-- The README asserted that property; the implementation did not deliver it.
-- is_overdue keyed on `resolution_days is null`, and the source emits rows that
-- carry a closed_date while status is still Open / In Progress / Assigned. Those
-- rows got a resolution_days, so is_overdue came out FALSE: 4,139 open requests
-- were being counted as on time by the exact expression the design was written
-- to protect.
--
-- Worth asserting rather than trusting, because the failure is invisible in
-- aggregate — the rate simply reads better than it is.

select
    service_request_id,
    status,
    closed_date,
    resolution_days,
    is_overdue
from {{ ref('fct_service_requests') }}
where status <> 'Closed'
  and is_overdue is not null
