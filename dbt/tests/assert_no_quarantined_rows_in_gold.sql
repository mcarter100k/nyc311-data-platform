-- No row Silver rejected may survive in the serving layer.
--
-- Guards the second post_hook on fct_service_requests. Quarantine happens in
-- pandas before dbt sees anything, so a rejected row never reaches staging and
-- the ORIGINAL reconciliation delete (present in staging, absent from int) is
-- structurally blind to it. A row loaded before it became invalid therefore sat
-- in Gold indefinitely.
--
-- Found 2026-08-22 by an end-to-end run after the fetch window moved: two
-- requests served as "In Progress" in Gold while the source reported them
-- closed seconds before they were created. Row counts alone never showed it —
-- the fact table simply had two rows too many and undercounted closures by two.
--
-- This is worth asserting rather than trusting because deleting the post_hook
-- breaks nothing visible: the build stays green, every count stays plausible,
-- and the error only compounds run over run.

select
    f.unique_key,
    q.quarantine_reason
from {{ ref('fct_service_requests') }} f
join {{ ref('stg_quarantine') }} q
  on f.unique_key = q.unique_key
