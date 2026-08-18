# ADR 006: Schema Evolution Strategy

**Status:** Accepted
**Date:** 2026-05-31

## Context

The NYC 311 pipeline ingests data from the NYC Open Data Socrata API, which is maintained
by a third party. The API schema changes without notice: new fields are added when the city
updates its 311 system, column names are occasionally renamed in documentation (though the
API wire format is usually stable), and data quality of existing fields can shift across
calendar years as data entry practices change.

The pipeline has three layers, each with a different relationship to schema change:

- **Bronze**: receives raw data directly from the API. Must accept anything the API sends.
- **Silver**: cleans and conforms Bronze data. Should propagate new columns without blocking
  the pipeline — a new nullable API field is almost always safe to pass through.
- **Gold (dbt)**: serves BI consumers. Schema stability is a contract with analysts and
  dashboards. Breaking changes here have real downstream consequences.

The core tension: the further downstream a schema change propagates automatically, the less
work is required from engineers — but the higher the risk that BI consumers see unexpected
changes in the data they are querying. The strategy must balance pipeline resilience (not
failing on expected API evolution) with consumer protection (not silently changing what
analysts depend on).

A secondary driver: this pipeline must be operable by a small team. A strategy that requires
manual DDL for every API field addition would create a constant maintenance burden and make
the pipeline fragile on the common case (additive changes) in order to protect against the
rare case (breaking changes).

## Options Considered

### Option A: Fully explicit schemas at every layer

All column lists are hardcoded at Bronze, Silver, and Gold. New API columns are silently
ignored until an engineer adds them. Breaking changes at the API level have no impact until
explicitly adopted.

**Pros:** Maximum predictability. Every column that exists anywhere in the pipeline is
deliberately chosen. No surprises.

**Cons:** Every Socrata API addition requires a manual code change to Bronze, a Silver schema
migration, and potentially a Gold model update — three PRs for what is typically a backwards-
compatible change. Historical data from NYC 311 shows the API adds 2-5 new fields per year.
At that rate, the overhead is manageable but the opportunity cost is real: the team is
writing boilerplate DDL instead of building analytics.

### Option B: Fully dynamic schemas at every layer

Auto Loader propagates any new column directly through Bronze → Silver → Gold without any
code change. dbt staging models use `select *` and mart models inherit new columns
automatically via star expansion.

**Pros:** Zero maintenance overhead for additive schema changes. The pipeline is resilient
to any API evolution the city introduces.

**Cons:** BI dashboards can silently gain or lose columns. If the API renames a field, the
old column disappears from Gold with no alert. Analysts building reports on top of Gold
marts cannot trust that the columns they depend on will be there tomorrow. This is a
correctness problem, not just an operational inconvenience.

### Option C: Graduated trust — permissive at Bronze and Silver, explicit at Gold

Bronze accepts all changes via Auto Loader schema inference and `mergeSchema=true`. Silver
propagates all Bronze columns dynamically via a runtime-resolved MERGE with `autoMerge`
enabled. Gold (dbt) uses explicit column lists: new columns only appear in Gold marts when
an engineer adds them to a `.sql` file, which requires a PR and review.

**Pros:** The common case (additive API changes) requires zero code changes to keep flowing
through Bronze and Silver. Gold consumers are protected — the columns they query are stable
and deliberate. A schema registry in Bronze provides observability: the team can see exactly
when each API field was first observed without having to grep source files.

**Cons:** New columns are "stranded" in Silver until someone explicitly promotes them to Gold.
This is an acceptable delay: a column that no analyst is querying yet does not need to be in
a Gold mart. When a new column becomes analytically interesting, promoting it to Gold is a
single PR that adds the column to a `.sql` file.

## Decision

**Option C — graduated trust.** Bronze and Silver are permissive; Gold is explicit.

The specific implementation at each layer:

### Bronze (`02_bronze.py`)

Auto Loader handles schema inference via `cloudFiles.schemaLocation`. New API columns are
written to Bronze automatically via `mergeSchema=true` on the Delta write. After each
successful write, the notebook compares the current Bronze table schema against
`BRONZE.schema_registry` and appends any newly observed columns with `column_name`,
`data_type`, `first_seen_date`, and `source_layer`.

The schema registry serves two purposes:
1. **Observability**: the team can query `BRONZE.schema_registry` to answer "when did column
   X first appear?" without reading git blame or Databricks job history.
2. **Downstream impact assessment**: when Silver or Gold needs to adopt a new column, the
   registry provides the authoritative first-seen date and data type.

New columns do not fail the Bronze job. Failing on a new nullable column would block
ingestion of all valid records in that batch — a disproportionate response to what is almost
always a safe change.

### Silver (`03_silver.py`)

The MERGE SET and VALUES expressions are built at runtime from the actual columns present in
the quality-filtered DataFrame (`df_quality.columns`), rather than being hardcoded. This
means a new Bronze column flows into Silver on the next pipeline run without any code change.

`spark.databricks.delta.schema.autoMerge.enabled` is set to `true` before the MERGE so
that Delta Lake extends the Silver table's schema when the source DataFrame contains columns
not yet in the target. Without this, a new column in the source would cause the MERGE to
fail with a schema mismatch error.

The transformation chain (`standardize_borough`, `compute_resolution_days`, null handling)
uses `withColumn` rather than `select`, so it never drops columns from Bronze. A new Bronze
column passes through the entire Silver transformation chain transparently.

### Gold (dbt)

dbt mart models (`fct_service_requests`, `dim_agency`, `dim_location`, `fct_daily_volume`,
`fct_data_quality`) use explicit column lists in their `.sql` files. New Silver columns are
invisible to Gold consumers until an engineer adds them to a mart model.

The staging layer (`stg_service_requests`) is the boundary between Silver and the dbt DAG.
It uses an explicit `SELECT` with named columns, providing a stable alias contract. New
Silver columns that appear between staging and mart models are discarded at this boundary.

The `schema_version` variable in `dbt_project.yml` is a string integer that is incremented
when a **breaking** schema change is deployed to Gold. Breaking changes include:

- A column is removed from a mart model
- A column is renamed in a mart model
- A column's data type changes incompatibly (e.g., INTEGER → VARCHAR)

Breaking changes require a PR that:
1. Updates the affected mart `.sql` files
2. Updates the corresponding `.yml` documentation
3. Increments `schema_version` in `dbt_project.yml`
4. Includes a migration note explaining what changed and why

Additive changes (a new column appearing in a mart) do NOT require a version bump because
existing downstream consumers are unaffected by the addition of a new column.

The `schema_version` value is selected in `stg_service_requests` as a literal column.
Because staging is a view, the value there always reflects the current var; it becomes a
durable per-row stamp in `fct_service_requests`, whose incremental merge writes it into
physical rows. Rows untouched by later merges keep the version they were built under,
which is what allows pre- and post-breaking-change rows to be distinguished when
backfilling or comparing historical data. (A `--full-refresh` restamps all history with
the current version.)

## Consequences

**Positive:**

- The Bronze and Silver pipelines are resilient to the expected rate of NYC Open Data API
  evolution (2-5 new fields per year). No engineer needs to be paged at midnight because
  Socrata added a field.
- The schema registry gives the team a queryable, dated record of every API field — better
  observability than inspecting the Delta table schema directly.
- Gold mart consumers have a stable schema contract. The columns they query are deliberate
  and reviewed.
- The `schema_version` column gives analysts a way to filter data by schema era when a
  breaking change occurs.

**Negative / accepted risks:**

- The `service_requests_quarantine` table written by Silver is intentionally excluded from
  the dbt lineage graph. It is an operational surface for DQ investigation, not an
  analyst-facing dataset. Engineers query it directly in Snowflake; it has no dbt source
  definition and no Gold consumer.
- New API columns are stranded in Silver until explicitly promoted to Gold. A column that
  becomes analytically interesting requires a PR. This is a deliberate speed bump — it is
  the mechanism by which Gold schema stability is enforced.
- The dynamic MERGE in Silver makes the pipeline harder to reason about statically. You
  cannot look at `03_silver.py` and know exactly which columns are being merged — you need
  to inspect the Bronze table schema at runtime. The tradeoff comment in `03_silver.py`
  documents this explicitly.
- `autoMerge=true` scoped to the Silver session means that if a malformed column arrives
  from Bronze (e.g., a column whose name is a reserved SQL keyword), it will be added to
  Silver before anyone can intervene. Mitigation: the Bronze schema registry will log the
  column on its first appearance, giving the team visibility before it propagates to Gold.

## The Contract (summary)

| Layer  | Accepts new columns | Requires code change | Requires PR review |
|--------|--------------------|-----------------------|--------------------|
| Bronze | Automatically      | No                   | No                 |
| Silver | Automatically      | No                   | No                 |
| Gold   | No                 | Yes                  | Yes                |

Breaking changes at Gold also require incrementing `schema_version` in `dbt_project.yml`.
Additive changes at Gold (adding a new column to a mart model) require a PR but do not
require a version bump.
