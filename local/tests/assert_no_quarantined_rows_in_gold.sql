-- No row Silver rejected may remain in fct_service_requests. Guards the
-- second post_hook: quarantined rows never reach staging, so without it a row
-- loaded before it became invalid stays in Gold with nothing going red.

select
    f.unique_key,
    q.quarantine_reason
from {{ ref('fct_service_requests') }} f
join {{ ref('stg_quarantine') }} q
  on f.unique_key = q.unique_key
