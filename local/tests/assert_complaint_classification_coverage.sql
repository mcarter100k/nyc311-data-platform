-- Classification coverage guard.
--
-- 'Other' must remain a genuine long tail, not a silent dumping ground. Before
-- the taxonomy pass it held 36% of all rows, which made every category-level
-- conclusion in the marts unreliable: the largest slice of every chart was
-- "we did not classify this".
--
-- Fails the build when 'Other' exceeds 5% of rows. The threshold is loose
-- relative to the measured 0.13% on purpose — it is a regression alarm (a
-- broken rule, a reordered CASE, a large new upstream complaint type), not a
-- precision target. Raise it only with evidence, never to silence a red build.

with totals as (

    select
        count(*)                                                        as total_rows,
        sum(case when complaint_category = 'Other' then 1 else 0 end)   as other_rows

    from {{ ref('int_service_requests_cleaned') }}

)

select
    total_rows,
    other_rows,
    round(100.0 * other_rows / total_rows, 2)                           as other_pct,
    5.0                                                                 as threshold_pct

from totals

where total_rows > 0
  and 100.0 * other_rows / total_rows > 5.0
