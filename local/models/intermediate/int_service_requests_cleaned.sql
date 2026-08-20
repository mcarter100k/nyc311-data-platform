with source as (

    select * from {{ ref('stg_service_requests') }}

),

-- Step 1: standardize borough from the shared seed (config/borough_variants.csv),
-- the same file the Python Silver transforms read. LEFT JOIN + COALESCE
-- reproduces the previous CASE ... ELSE 'UNSPECIFIED' exactly; `variant` is
-- unique-tested so no fan-out is possible.

borough_standardized as (

    select
        source.*,
        coalesce(bv.canonical, 'UNSPECIFIED')                                   as borough_clean

    from source

    left join {{ ref('borough_variants') }} bv
        on upper(trim(source.borough)) = bv.variant

),

with_resolution_days as (

    select
        *,
        case
            when closed_date is not null and created_date is not null
                then datediff('day', created_date, closed_date)
            else null
        end                                                                     as resolution_days

    from borough_standardized

),

-- Step 3: classify complaint_type into 21 operational categories + 'Other'.
-- Order matters (first match wins); the '%tree%' guard and the Animals /
-- Construction / Environmental orderings are load-bearing — see dbt/.

with_complaint_category as (

    select
        *,
        case
            -- ── Noise ────────────────────────────────────────────────────
            when complaint_type ilike '%noise%'
                then 'Noise'

            -- ── Heat & Hot Water ─────────────────────────────────────────
            when complaint_type ilike '%heat%'
              or complaint_type ilike '%hot water%'
                then 'Heat & Hot Water'

            -- ── Animals ──────────────────────────────────────────────────
            -- MUST precede Housing ('%unsanitary%') and Parks ('%in a park%'):
            -- "Unsanitary Animal Pvt Property" and "Animal in a Park" would
            -- otherwise be captured by those broader rules.
            when complaint_type ilike '%animal%'
              or complaint_type ilike '%pigeon%'
              or complaint_type ilike '%unleashed dog%'
              or complaint_type ilike '%bees/wasps%'
              or complaint_type ilike '%mosquito%'
                then 'Animals'

            -- ── Rodent ───────────────────────────────────────────────────
            -- 'rat' is anchored to a word start ('rat%' / '% rat%') rather than
            -- used as a bare substring: '%rat%' also matches "grating" and
            -- "administrative".
            when complaint_type ilike '%rodent%'
              or complaint_type ilike '%mice%'
              or complaint_type ilike 'rat%'
              or complaint_type ilike '% rat%'
                then 'Rodent'

            -- ── Vehicles ─────────────────────────────────────────────────
            when complaint_type ilike '%abandoned vehicle%'
              or complaint_type ilike '%derelict vehicle%'
              or complaint_type ilike '%abandoned bike%'
                then 'Abandoned Vehicle'

            when complaint_type ilike '%taxi%'
              or complaint_type ilike '%for hire vehicle%'
              or complaint_type ilike '%lost property%'
                then 'Taxi & For-Hire Vehicle'

            when complaint_type ilike '%illegal park%'
              or complaint_type ilike '%blocked driveway%'
                then 'Illegal Parking'

            -- ── Street infrastructure ────────────────────────────────────
            when complaint_type ilike '%street%light%'
              or complaint_type ilike '%streetlight%'
                then 'Street Light'

            when complaint_type ilike '%traffic signal%'
              or complaint_type ilike '%parking meter%'
              or complaint_type ilike '%bus stop%'
              or complaint_type ilike '%bike rack%'
              or complaint_type ilike '%street sign%'
              or complaint_type ilike '%highway sign%'
              or upper(trim(complaint_type)) = 'TRAFFIC'
                then 'Traffic & Signals'

            -- ── Waste ────────────────────────────────────────────────────
            when complaint_type ilike '%sanitation%'
              or complaint_type ilike '%dirty condition%'
              or complaint_type ilike '%missed collection%'
              or complaint_type ilike '%street sweeping%'
              or complaint_type ilike '%litter basket%'
              or complaint_type ilike '%disposal complaint%'
              or complaint_type ilike '%dumpster%'
                then 'Sanitation'

            when complaint_type ilike '%illegal dumping%'
              or complaint_type ilike '%industrial waste%'
                then 'Illegal Dumping'

            -- ── Parks & Trees ────────────────────────────────────────────
            -- The '%tree%' wildcard is guarded with NOT ILIKE '%street%'
            -- because "S-tree-t" contains "tree": before this guard,
            -- "Street Sweeping Complaint" and all three "Street Sign - ..."
            -- types (309 rows in a one-week sample) were classified as
            -- Parks & Trees. The guard is order-independent, so a future rule
            -- reshuffle cannot resurrect the bug. No NYC tree complaint type
            -- contains the word "street".
            when (complaint_type ilike '%tree%' and complaint_type not ilike '%street%')
              or complaint_type ilike '%overgrown%'
              or complaint_type ilike '%root/sewer/sidewalk%'
              or complaint_type ilike '%park rules%'
              or complaint_type ilike '%in a park%'
              or complaint_type ilike '%maintenance or facility%'
              or complaint_type ilike '%bench%'
              or complaint_type ilike '%beach/pool/sauna%'
              or complaint_type ilike '%stump%'
                then 'Parks & Trees'

            when complaint_type ilike '%pothole%'
              or complaint_type ilike '%pavement%'
              or complaint_type ilike '%street condition%'
              or complaint_type ilike '%sidewalk condition%'
              or complaint_type ilike '%curb condition%'
              or complaint_type ilike '%highway condition%'
              or complaint_type ilike '%bridge condition%'
              or complaint_type ilike '%obstruction%'
              -- Snow/ice clearance is unobservable in a summer window; the
              -- rule is declared now so winter volume cannot land in 'Other'.
              or complaint_type ilike '%snow%'
                then 'Street Condition'

            when complaint_type ilike '%graffiti%'
                then 'Graffiti'

            when complaint_type ilike '%homeless%'
              or complaint_type ilike '%encampment%'
                then 'Homeless Services'

            -- ── Environmental ────────────────────────────────────────────
            -- Before Construction, so "Construction Lead Dust" classifies as
            -- a lead hazard rather than as construction work.
            when complaint_type ilike '%air quality%'
              or complaint_type ilike '%asbestos%'
              or complaint_type ilike '%hazardous material%'
              or complaint_type ilike '%lead%'
                then 'Environmental Hazard'

            -- ── Construction ─────────────────────────────────────────────
            -- Before Housing, so "General Construction/Plumbing" (DOB) does
            -- not fall into the '%general%'/'%plumbing%' housing rules.
            when complaint_type ilike '%construction%'
              or complaint_type ilike '%building/use%'
              or complaint_type ilike '%real time enforcement%'
              or complaint_type ilike '%special projects%'
              or complaint_type ilike '%emergency response team%'
              or complaint_type ilike '%lot condition%'
              or complaint_type ilike '%scaffold%'
              or complaint_type ilike '%wood pile%'
                then 'Construction & Building Code'

            -- ── Water & Sewer ────────────────────────────────────────────
            when complaint_type ilike '%water%'
              or complaint_type ilike '%sewer%'
              or complaint_type ilike '%sewage%'
                then 'Water & Sewer'

            -- ── Housing & Building Maintenance ───────────────────────────
            -- The HPD interior-condition codes (uppercase in the source).
            -- HEAT/HOT WATER and WATER LEAK are deliberately NOT here: they
            -- are caught by the more specific categories above, which are
            -- the ones consumers ask for by name.
            when complaint_type ilike '%unsanitary condition%'
              or complaint_type ilike '%paint/plaster%'
              or complaint_type ilike '%door/window%'
              or complaint_type ilike '%flooring/stairs%'
              or complaint_type ilike '%plumbing%'
              or complaint_type ilike '%electric%'
              or complaint_type ilike '%appliance%'
              or complaint_type ilike '%elevator%'
              or complaint_type ilike '%boiler%'
              or complaint_type ilike '%outside building%'
              or complaint_type ilike '%indoor air quality%'
              or complaint_type ilike '%mold%'
              or upper(trim(complaint_type)) in ('GENERAL', 'SAFETY')
                then 'Housing & Building Maintenance'

            -- ── Quality of life ──────────────────────────────────────────
            when complaint_type ilike '%police matter%'
              or complaint_type ilike '%panhandling%'
              or complaint_type ilike '%drug activity%'
              or complaint_type ilike '%drinking%'
              or complaint_type ilike '%smoking%'
              or complaint_type ilike '%fireworks%'
              or complaint_type ilike '%urinating%'
              or complaint_type ilike '%bike/roller/skate%'
              or complaint_type ilike '%posting%'
              or complaint_type ilike '%investigations and discipline%'
                then 'Public Safety & Quality of Life'

            when complaint_type ilike '%vendor%'
              or complaint_type ilike '%consumer complaint%'
              or complaint_type ilike '%food%'
              or complaint_type ilike '%cannabis%'
              or complaint_type ilike '%outdoor dining%'
              or complaint_type ilike '%day care%'
              or complaint_type ilike '%tattooing%'
                then 'Consumer & Business'

            else 'Other'
        end                                                                     as complaint_category

    from with_resolution_days

),

-- Step 4: classify how the request was closed, from the templated
-- resolution_description text. 'Closed' != 'fixed' — see dbt/ for the full
-- rationale and the ordering constraints.

with_closure_type as (

    select
        *,
    case
        when resolution_description is null
          or trim(resolution_description) = ''
          or upper(trim(resolution_description)) = 'N/A'                    then 'Unspecified'

        -- Duplicate first: unambiguous, and the phrasing appears inside otherwise
        -- action-shaped sentences.
        when resolution_description ilike '%duplicate%'
          or resolution_description ilike '%already exists%'
          or resolution_description ilike '%received an earlier complaint%'
          or resolution_description ilike '%previously reported by another%'
          or resolution_description ilike '%cannot open multiple service requests%' then 'Duplicate'

        -- Access failed: the agency tried and could not perform the work.
        -- '%not able to gain access%' is HPD's phrasing, '%unable to gain entry%'
        -- is NYPD's — both mean the same outcome.
        when resolution_description ilike '%unable to gain entry%'
          or resolution_description ilike '%not able to gain access%'
          or resolution_description ilike '%unable to complete the inspection%'
          or resolution_description ilike '%could not gain access%'
          or resolution_description ilike '%did not accept assistance%'     then 'Access Failed'

        -- Pending: before every action rule, so "scheduled to be removed" and
        -- "will inspect" are not counted as work already done.
        when resolution_description ilike '%still open%'
          or resolution_description ilike '%check back later%'
          or resolution_description ilike '%will inspect%'
          or resolution_description ilike '%will visit%'
          or resolution_description ilike '%will determine%'
          or resolution_description ilike '%will address%'
          or resolution_description ilike '%will notify%'
          or resolution_description ilike '%will be removed%'
          or resolution_description ilike '%scheduled to be%'
          or resolution_description ilike '%created a work order%'
          or resolution_description ilike '%within 30 days%'
          or resolution_description ilike '%within 120 days%'
          or resolution_description ilike '%further investigation is required%'
          or resolution_description ilike '%is reviewing your complaint%'
          or resolution_description ilike '%inspection is currently in progress%' then 'Pending'

        -- Enforcement: a legal instrument was issued.
        when resolution_description ilike '%issued a summons%'
          or resolution_description ilike '%made an arrest%'
          or resolution_description ilike '%notice of violation%'
          or resolution_description ilike '%violations were issued%'
          or resolution_description ilike '%violation was issued%'
          or resolution_description ilike '%office of administrative trials%' then 'Enforcement Action'

        -- Resolved on scene. NYPD's highest-volume template states BOTH "no
        -- criminal violation existed" AND "the condition was corrected without
        -- the need to issue a summons" — officers settled it informally. Placed
        -- ahead of No Violation Found deliberately: the operative outcome is that
        -- the condition stopped, not that no law was broken. Documented as a
        -- judgment call because the same sentence supports either reading.
        when resolution_description ilike '%condition was corrected%'
          or resolution_description ilike '%was corrected%'                 then 'Resolved on Scene'

        -- Work performed: physical or administrative work completed.
        when resolution_description ilike '%repaired%'
          or resolution_description ilike '%cleaned the location%'
          or resolution_description ilike '%collected%'
          or resolution_description ilike '%removed the%'
          or resolution_description ilike '%shut the running hydrant%'
          or resolution_description ilike '%sent official written notification%'
          or resolution_description ilike '%a report was prepared%'
          or resolution_description ilike '%mailed you%'
          or resolution_description ilike '%corrected the problem%'
          or resolution_description ilike '%completed the requested%'
          or resolution_description ilike '%accepted assistance%'
          or resolution_description ilike '%letter was sent%'
          or resolution_description ilike '%sent an advisory%'
          or resolution_description ilike '%addressed the issue%'
          or resolution_description ilike '%had been restored%'             then 'Work Performed'

        -- No violation: a condition may exist, but it breaks no rule.
        when resolution_description ilike '%no criminal violation%'
          or resolution_description ilike '%no evidence of a criminal violation%'
          or resolution_description ilike '%did not violate%'
          or resolution_description ilike '%no violation%'
          or resolution_description ilike '%action was not necessary%'
          or resolution_description ilike '%no further action%'
          or resolution_description ilike '%observe a violation%'
          or resolution_description ilike '%no dsny related%'
          or resolution_description ilike '%no work is necessary%'
          or resolution_description ilike '%meets its standards%'           then 'No Violation Found'

        -- Nothing there: the agency looked and found no condition at all.
        when resolution_description ilike '%found no condition%'
          or resolution_description ilike '%could not find%'
          or resolution_description ilike '%couldn%t find%'
          or resolution_description ilike '%did not find%'
          or resolution_description ilike '%didn%t find%'
          or resolution_description ilike '%observed no%'
          or resolution_description ilike '%no condition%'
          or resolution_description ilike '%found there was no%'            then 'No Condition Found'

        -- Handed to someone else. Deliberately NARROW patterns: bare '%referred%'
        -- and '%please contact%' appear in routine closing boilerplate ("If the
        -- problem persists, please contact 311") and would swallow half the table.
        when resolution_description ilike '%does not fall under the jurisdiction%'
          or resolution_description ilike '%does not have jurisdiction%'
          or resolution_description ilike '%referred this complaint to%'
          or resolution_description ilike '%referred to the appropriate%'
          or resolution_description ilike '%has forwarded the request%'
          or resolution_description ilike '%will be referred%'
          or resolution_description ilike '%has requested the department of%'
          or resolution_description ilike '%out of jurisdiction%'           then 'Referred Elsewhere'

        else 'Unspecified'
    end                                                                                as closure_type

    from with_complaint_category

),

quality_filtered as (

    select *
    from with_closure_type
    where resolution_days is null
       or resolution_days >= 0

)

select * from quality_filtered
