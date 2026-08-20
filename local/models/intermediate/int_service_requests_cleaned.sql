with source as (

    select * from {{ ref('stg_service_requests') }}

),

borough_standardized as (

    select
        *,
        case
            when upper(trim(borough)) in ('BROOKLYN', 'BKLYN', 'BK', 'KINGS')
                then 'BROOKLYN'
            when upper(trim(borough)) in ('MANHATTAN', 'MN', 'NEW YORK', 'NEW YORK CITY', 'NYC')
                then 'MANHATTAN'
            when upper(trim(borough)) in ('QUEENS', 'QN', 'QNS')
                then 'QUEENS'
            when upper(trim(borough)) in ('BRONX', 'THE BRONX', 'BX')
                then 'BRONX'
            when upper(trim(borough)) in ('STATEN ISLAND', 'SI', 'S.I.', 'STATEN IS')
                then 'STATEN ISLAND'
            else 'UNSPECIFIED'
        end                                                                     as borough_clean

    from source

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

quality_filtered as (

    select *
    from with_complaint_category
    where resolution_days is null
       or resolution_days >= 0

)

select * from quality_filtered
