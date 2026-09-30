-- Fails when more than 12% of rows with resolution text are 'Undecodable'.
--
-- closure_type is read from free text, so a template the source rewrites
-- overnight stops matching silently: its rows move to 'Undecodable', turn
-- is_actioned FALSE, and every action rate drops with nothing going red.
--
-- Denominator: rows that carry resolution text, the rows the rules actually
-- read. Rows with no text would inflate it and hide a rules regression.
--
-- 12% is a loose regression alarm (about 7% at the time it was set), not a
-- precision target. Tighten it as the rules improve; never raise it to
-- silence a failure.

with population as (

    select closure_type

    from {{ ref('int_service_requests_cleaned') }}

    -- Exactly the rows the leading branch of the CASE does NOT capture, i.e.
    -- the rows the pattern rules were given something to read.
    where resolution_description is not null
      and trim(resolution_description) <> ''
      and upper(trim(resolution_description)) <> 'N/A'

),

totals as (

    select
        count(*)                                                             as rows_with_text,
        sum(case when closure_type = 'Undecodable' then 1 else 0 end)        as undecodable_rows

    from population

)

select
    rows_with_text,
    undecodable_rows,
    rows_with_text - undecodable_rows                                        as decoded_rows,
    round(100.0 * undecodable_rows / rows_with_text, 2)                      as undecodable_pct,
    12.0                                                                     as threshold_pct

from totals

where rows_with_text > 0
  and 100.0 * undecodable_rows / rows_with_text > 12.0
