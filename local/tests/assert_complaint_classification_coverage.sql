-- Fails when more than 5% of rows are complaint_category 'Undecodable' (a
-- complaint_type no rule matched). 'Unspecified' (no complaint_type at all) is
-- not counted: missing input is not a decode failure.
--
-- 5% is a loose regression alarm for a broken or reordered rule or a large new
-- upstream complaint type (well under 1% at the time it was set). Raise it
-- only with evidence, never to silence a failure.

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
