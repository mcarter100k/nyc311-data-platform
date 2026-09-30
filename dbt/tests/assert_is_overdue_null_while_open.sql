-- is_overdue must be NULL for anything not Closed, so
-- `COUNT(*) FILTER (WHERE NOT is_overdue)` cannot count an open request as on
-- time. The source sends some open rows with a closed_date, so keying on
-- resolution_days alone is not enough.

select
    service_request_id,
    status,
    closed_date,
    resolution_days,
    is_overdue
from {{ ref('fct_service_requests') }}
where status <> 'Closed'
  and is_overdue is not null
