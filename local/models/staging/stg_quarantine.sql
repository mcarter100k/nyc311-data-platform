-- Passthrough over SILVER.quarantine, so fct_service_requests can delete
-- rejected rows without reading a source directly.

select
    unique_key,
    created_date,
    closed_date,
    resolution_days,
    quarantine_reason,
    _silver_timestamp
from {{ source('silver', 'quarantine') }}
