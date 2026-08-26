{{
    config(
        materialized         = 'incremental',
        schema               = 'gold',
        unique_key           = 'location_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'append_new_columns'
    )
}}

-- Location dimension derived from the data itself: one row per distinct
-- (borough, community_board, incident_zip) combination that has ever appeared,
-- giving analysts a three-level geographic drill path. There is no authoritative
-- upstream location list, so the domain grows as new combinations arrive.
-- CONTRACT: the UNKNOWN-coalescing on community_board and incident_zip below
-- must stay byte-identical to the join keys in fct_service_requests — if the
-- two drift, fact rows silently lose their location_id.

-- WHY INCREMENTAL, when a 540-row dimension rebuilds in milliseconds: this is
-- a retention requirement, not a performance one.
--
-- This model was `materialized: table`, rebuilt every run from
-- int_service_requests_cleaned — which carries only Silver's rolling window.
-- fct_service_requests is incremental and accumulates history far beyond that
-- window. So a combination that stopped appearing in the window was DROPPED
-- from the dimension on the next run while fact rows kept pointing at its
-- location_id, and the FK silently dangled. The decay was invisible: nothing
-- errored, and fct_daily_volume simply coalesced the failed join to borough
-- 'UNSPECIFIED', quietly moving volume out of the real boroughs.
--
-- Kimball's rule is the fix: a conformed dimension GROWS and never loses
-- members, because a fact row's foreign key must resolve for as long as that
-- fact row exists. Once the fact outlives the source window, the dimension
-- must too.
--
-- The dimension is IMMUTABLE, which is what makes plain accumulation correct
-- rather than an SCD problem. The three attributes are not attributes OF the
-- key — they ARE the key: location_id is the surrogate hash over exactly
-- (borough, community_board, incident_zip) and the table holds no other
-- column. A location therefore has no attribute that can change; any change to
-- any of the three produces a different hash, i.e. a NEW member, and the old
-- one stays valid for the fact rows already pointing at it. Type 0/insert-only
-- is the whole lifecycle — no SCD2 validity windows, no Type 1 restatement.
--
-- The incremental filter below makes that explicit: an already-recorded member
-- is never rewritten, only new combinations are appended. unique_key is
-- retained as the guard that keeps the merge an upsert rather than an append
-- if the filter is ever removed.
--
-- A --full-refresh still rebuilds from the current window only, and would
-- re-orphan accumulated fact rows. That is the same trade fct_service_requests
-- already makes (a --full-refresh of the fact rebuilds it from the window too),
-- so refreshing BOTH together stays internally consistent; refreshing this
-- model alone does not. Recovering a dimension that has already decayed means
-- replaying a Silver window wide enough to cover the fact's date range — the
-- accumulation below then absorbs those members permanently.

with locations as (

    select distinct
        -- Use the standardized borough from the intermediate layer, not the raw value.
        borough_clean                                                           as borough,
        coalesce(nullif(trim(community_board), ''), 'UNKNOWN')                 as community_board,
        coalesce(nullif(trim(incident_zip),    ''), 'UNKNOWN')                 as incident_zip

    from {{ ref('int_service_requests_cleaned') }}

    where borough_clean is not null

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'borough',
            'community_board',
            'incident_zip'
        ]) }}                                                                   as location_id,
        borough,
        community_board,
        incident_zip

    from locations

)

select * from final

{% if is_incremental() %}

-- Append-only: keep every member this dimension has ever recorded and add the
-- combinations this run saw for the first time. NOT IN is NULL-safe here —
-- location_id is a not_null-tested hash of three non-null expressions on both
-- sides of the comparison.
where location_id not in (select location_id from {{ this }})

{% endif %}
