-- Closure-decode coverage guard.
--
-- closure_type is read out of free text, so it has a failure mode the complaint
-- taxonomy does not: the source can rewrite a template overnight and every rule
-- that matched it stops matching, silently. The rows do not disappear — they
-- move into the catch-all, is_actioned turns FALSE for all of them, and every
-- action rate steps down without anything going red.
--
-- That is not hypothetical. Until this pass the catch-all was named
-- 'Unspecified' and ALSO held the rows with no resolution text at all, so a
-- decoder miss and a silent source were one number and neither could be
-- measured. Splitting them made the miss visible: 8,330 rows on the local load,
-- 7.34% of every row that carried resolution text.
--
-- DENOMINATOR: rows that carry resolution text. Not all rows — a request with
-- no resolution_description gave the decoder nothing to fail at, and including
-- it would let a flood of empty text mask a rules regression by inflating the
-- denominator. This is the population the rules were actually applied to.
--
-- THRESHOLD 12%, against 7.34% measured. Deliberately loose: this is a
-- regression alarm for a template change or a deleted rule, not a precision
-- target, and the honest reading of the current 7.34% is that the rules have
-- real ground to make up. Tightening it as the rules improve is the intended
-- direction; raising it needs evidence, and never to silence a red build.

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
