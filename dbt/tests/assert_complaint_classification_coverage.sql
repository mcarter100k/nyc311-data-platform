-- Classification coverage guard.
--
-- The catch-all must remain a genuine long tail, not a silent dumping ground.
-- Before the taxonomy pass it held 36% of all rows, which made every
-- category-level conclusion in the marts unreliable: the largest slice of every
-- chart was "we did not classify this".
--
-- The bucket is now called 'Undecodable' rather than 'Other', and the rename is
-- the point rather than cosmetics. The CASE in int_service_requests_cleaned has
-- no rule that ASSIGNS 'Other' — every row in it arrived by falling off the end
-- — so the old label described a verdict the model never reached. Measured on
-- the local load, 189 of 189 rows carried a real complaint_type with no rule
-- for it (Green Infrastructure, E-Scooter, LinkNYC, Ferry Inquiry): the bucket
-- was 100% decoder miss and 0% genuine other. 'Unspecified' — the source
-- supplied no complaint_type at all — is a separate value and is NOT counted
-- here, because a missing input is not a decode failure.
--
-- Fails the build when 'Undecodable' exceeds 5% of rows. The threshold is loose
-- relative to the measured 0.15% on purpose — it is a regression alarm (a
-- broken rule, a reordered CASE, a large new upstream complaint type), not a
-- precision target. Raise it only with evidence, never to silence a red build.

with totals as (

    select
        count(*)                                                             as total_rows,
        sum(case when complaint_category = 'Undecodable' then 1 else 0 end)  as undecodable_rows

    from {{ ref('int_service_requests_cleaned') }}

)

select
    total_rows,
    undecodable_rows,
    round(100.0 * undecodable_rows / total_rows, 2)                          as undecodable_pct,
    5.0                                                                      as threshold_pct

from totals

where total_rows > 0
  and 100.0 * undecodable_rows / total_rows > 5.0
