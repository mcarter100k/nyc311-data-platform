# ADR 007: SCD Type 2 for the Agency Dimension

**Status:** Accepted
**Date:** 2026-06-05

## Context

The NYC 311 pipeline receives `agency` and `agency_name` fields from the Socrata API.
Agency names occasionally change — city agencies are reorganized, renamed, or merged.
The original `dim_agency` model was a plain `SELECT DISTINCT` with SCD Type 1 semantics:
every refresh overwrote the current name, silently erasing the fact that a given
`agency_abbreviation` had a different name in prior years.

This matters for two reporting scenarios:

1. **Historical accuracy** — a service request from 2015 involving "Department of
   Homeless Services" should still be queryable by that name, even if the agency was
   renamed "Department of Social Services" in 2018.

2. **Breaking-change detection** — if an analyst's dashboard filters on `agency_name =
   'Department of Homeless Services'` and the SCD1 overwrite silently renames it, the
   filter returns zero rows with no error. SCD Type 2 makes the old name permanently
   queryable as an expired version.

A secondary driver: the pipeline already uses dbt snapshots for nothing else. Adding
one snapshot for agency versioning establishes the pattern for any future slowly-changing
attribute that needs history tracking without requiring custom Silver MERGE logic.

## Options Considered

### Option A: Keep SCD Type 1 (latest name wins)

Continue with the `SELECT DISTINCT` approach. Historical service requests show the
current agency name, not the name that was active when the request was filed.

**Pros:** Zero implementation complexity. No new snapshot table. Historical queries
always return current names — useful for "who is responsible now" dashboards.

**Cons:** Silent data loss. An analyst querying requests from 2015 sees the 2025 agency
name with no indication that the name changed. Year-over-year reports that filter by
agency name become incorrect when the agency is renamed. There is no audit trail of
what the agency was called at any point in time.

### Option B: Manual effective/expiry date management in Silver

Track agency name changes in `03_silver.py` using a custom Delta MERGE with
effective/expiry date logic — similar to the SCD Type 2 reference pattern already
commented in the Silver notebook.

**Pros:** No dependency on dbt snapshots. History is tracked at the Silver layer, closer
to the raw data.

**Cons:** Custom MERGE logic for a dimension with ~60 rows is high-effort for low
payoff. The Silver notebook's primary responsibility is service request fact data, not
agency reference data. Mixing reference data management with fact data processing
makes the notebook harder to reason about. Change detection requires comparing the
incoming agency_name against the current Silver record on every run.

### Option C: dbt snapshot with check strategy (chosen)

Create `snapshots/agency_snapshot.sql` using the `check` strategy on `agency_name`.
`dim_agency.sql` reads from the snapshot and exposes `effective_date`, `expiry_date`,
and `is_current` columns. The snapshot table lives in a dedicated `SNAPSHOTS` schema.

**Pros:**
- Change detection is handled declaratively by dbt — no custom MERGE logic.
- The snapshot audit columns (`dbt_valid_from`, `dbt_valid_to`) are managed by dbt
  and are reliable.
- `dim_agency` remains a simple transformation model (snapshot → cast dates → generate
  surrogate key). No imperative code.
- Adding a second slowly-changing dimension in the future follows the same pattern.

**Cons:**
- The pipeline has two steps where it previously had one: `dbt snapshot` must run before
  `dbt run` for the agency dimension to reflect the latest source data.
- `dim_agency` no longer depends directly on `int_service_requests_cleaned` in the dbt
  DAG — it depends on `agency_snapshot`. The lineage test was updated to assert the
  snapshot dependency rather than a direct intermediate dependency.
- The `check` strategy compares the entire `agency_name` column on every snapshot run.
  For ~60 agencies this is negligible; it would be reconsidered if the dimension grows
  to thousands of rows.

## Decision

**Option C — dbt snapshot with check strategy.**

The snapshot adds one processing step but removes all custom change-detection code.
For a slowly-changing reference table of ~60 rows, the operational overhead is minimal
and the correctness guarantee is worth more than the simplicity of SCD Type 1.

## Implementation

### `snapshots/agency_snapshot.sql`

Source query: `SELECT DISTINCT agency_abbreviation, initcap(trim(agency_name))` from
`int_service_requests_cleaned`, with a `QUALIFY row_number() = 1` to produce exactly
one row per `agency_abbreviation` per snapshot run (required by the `unique_key`
constraint).

Configuration:
```sql
unique_key  = 'agency_abbreviation'
strategy    = 'check'
check_cols  = ['agency_name']
target_schema = 'snapshots'
```

The `check` strategy is chosen over `timestamp` because there is no `updated_at`
timestamp on the agency reference data — the source is derived from service requests,
not from an authoritative agency registry with modification timestamps.

### `dim_agency.sql`

Reads from `{{ ref('agency_snapshot') }}`. Exposes:

| Column              | Type    | Description                                         |
|---------------------|---------|-----------------------------------------------------|
| `agency_key`        | VARCHAR | Surrogate key on (abbreviation, effective_date). PK |
| `agency_abbreviation` | VARCHAR | Natural key. Not unique — multiple versions share it |
| `agency_name`       | VARCHAR | Full name, initcap-normalized. Tracked attribute    |
| `effective_date`    | DATE    | When this version became active                     |
| `expiry_date`       | DATE    | When superseded (NULL for the current version)      |
| `is_current`        | BOOLEAN | TRUE for exactly one row per abbreviation           |

### `fct_service_requests.sql`

The agency join is restricted to `AND a.is_current = true`:

```sql
left join dim_agency a
    on r.agency_abbreviation = a.agency_abbreviation
   and a.is_current = true
```

Without this filter, the SCD2 table fans out the fact: a service request for an agency
with two historical name versions would produce two rows in the fact table — one per
version. The `is_current = true` restriction pins the join to the current version,
ensuring a 1:1 relationship between service requests and agency rows.

The `agency_id` column in `fct_service_requests` carries `agency_key` (the versioned
surrogate). Historical fact rows retain the `agency_key` from the version that was
current when they were last merged by Silver. Old `agency_key` values continue to
resolve the FK because SCD2 never deletes expired dimension rows.

### Singular test

`dbt/tests/assert_one_current_agency_per_abbreviation.sql` verifies that exactly one
`is_current = true` row exists per `agency_abbreviation`. This constraint is what makes
the `fct_service_requests` join safe. A violation (zero or two current rows) would be
caught before any BI consumer sees corrupted data.

## What triggers a new version

**A new version is opened when `agency_name` changes** for an existing
`agency_abbreviation`. Examples:
- "Dept of Sanitation" → "Department of Sanitation" (typo correction)
- "Human Resources Administration" → "Department of Social Services" (reorganization)

**A changed abbreviation is a new agency**, not a new version of the old one. The
`unique_key = 'agency_abbreviation'` means that if DSNY became NYDS, the snapshot
would insert a new record for NYDS and expire the DSNY record — they are treated as
distinct agencies. This is consistent with how foreign keys work: `fct_service_requests`
stores the abbreviation that appeared in the API response at ingestion time.

## Consequences

**Positive:**
- Historical service requests resolve to the agency name that was active at load time.
- Agency name changes are auditable: query `dim_agency WHERE agency_abbreviation = 'X'`
  to see the full version history.
- The singular test catches any snapshot bug that produces two active rows, before it
  fans out the fact table join.

**Negative / accepted risks:**
- `dbt snapshot` must run before `dbt run` in the Airflow DAG. If a dbt run is
  executed without a preceding snapshot, `dim_agency` reflects the previous snapshot
  state — agency names updated in Silver since the last snapshot won't appear yet.
  Mitigation: Airflow task dependency `snapshot_agency >> dbt_run` enforces ordering.
- New service requests load with `agency_id = agency_key` of the currently-active
  version. If the Airflow snapshot step is skipped and an agency name changed, those
  fact rows will carry the old `agency_key` until the snapshot runs. This is a
  short-term inconsistency, not data loss — the snapshot run corrects it and the
  next incremental dbt run picks up the new key.
