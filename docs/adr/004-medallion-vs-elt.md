# ADR 004: Medallion Architecture vs. Alternative Ingestion Patterns

**Status:** Accepted
**Date:** 2026-05-27

## Context

Three dominant patterns exist for moving data from an operational source (NYC Open Data
Socrata API) to an analytical serving layer (Snowflake Gold dimensional model):

1. **Medallion (Bronze/Silver/Gold)** — staged transformation across a lakehouse,
   with each layer having a single defined responsibility.
2. **Pure ELT** — land raw data directly in the warehouse (via Fivetran, Airbyte, or
   a lightweight loader); transform entirely within the warehouse using SQL.
3. **Two-layer (Raw → Gold)** — ingest raw data, apply all cleaning and business logic
   in one transformation step.

The choice determines the tool surface area, the number of failure isolation boundaries,
the ability to replay from intermediate checkpoints, and what skills the project demonstrates
to technical reviewers.

## Options Considered

### Full Medallion Architecture (Bronze / Silver / Gold)

Three layers with explicit, non-overlapping responsibilities:

- **Bronze**: raw data written exactly as received from the source — JSON structure
  preserved, no type coercion, no deduplication. Append-only. Partitioned by ingestion
  date in ADLS Gen2. Immutable once written.
- **Silver**: cleaning and conformance applied by PySpark. Deduplication on `unique_key`,
  type casting, borough name standardization, null handling. Output is a typed, deduplicated
  Delta table synced to Snowflake.
- **Gold**: business logic applied by dbt SQL. Dimensional model (star schema), surrogate
  key generation, complaint categorization, resolution metrics. Output is tested and
  documented Snowflake tables consumed directly by BI tooling.

Each layer is a checkpoint: if the Silver job fails, Bronze is intact. If a dbt test
fails, Silver is intact. Any layer can be replayed independently without re-ingesting
from the source. The architecture has been proven at scale at companies like Databricks,
Airbnb (Minerva), and Stripe (Stripe Sigma) — it is not experimental.

### Pure ELT (Fivetran → Snowflake → dbt)

Use a managed connector (Fivetran, Airbyte) to load raw data directly into Snowflake's
raw schema, then apply all cleaning and modeling in dbt. This pattern eliminates the
Databricks compute plane entirely and has a dramatically smaller operational footprint —
a single dbt project handles everything from type casting to dimensional modeling.

This approach is architecturally sound and widely used in modern data teams. It is the
correct choice when:
- PySpark complexity is unnecessary (the source data is clean and well-typed).
- The team's core skill set is SQL, not distributed compute.
- Operational simplicity outweighs intermediate checkpoint value.

Rejected for this project for two reasons: (1) it eliminates the Databricks/PySpark
showcase, which is a core portfolio objective; (2) it removes the intermediate debugging
checkpoint — when a data quality issue surfaces in the Gold layer, there is no way to
distinguish a source data problem from a transformation logic problem without the Silver
layer as an intermediate reference.

### Two-Layer Architecture (Raw → Gold)

Ingest raw data to ADLS (or directly to Snowflake), then apply both cleaning logic and
business logic in a single transformation step. Simpler than full medallion; retains
Databricks in the stack.

The problem is that cleaning logic and business logic are conceptually distinct concerns
that generate different categories of bugs. A borough name variant that slips through
cleaning causes wrong GROUP BY results in Gold — but without a Silver layer to inspect,
you cannot determine whether the failure originated in ingestion (wrong raw value) or in
the Gold transformation (wrong CASE WHEN branch). Merging the two layers destroys that
diagnostic capability.

Rejected because it trades debuggability for simplicity at a layer boundary that carries
high business impact.

## Decision

**Full medallion architecture** with explicit layer contracts enforced by tool and
schema isolation:

| Layer  | Tool       | Schema     | Responsibility (single, non-overlapping)              |
|--------|------------|------------|-------------------------------------------------------|
| Bronze | Databricks | ADLS/Delta | Store raw source data exactly as received             |
| Silver | Databricks | Snowflake  | Clean, type, deduplicate; no business logic           |
| Gold   | dbt        | Snowflake  | Apply business logic; never modify source semantics   |

The layer contract is enforced at the Snowflake access layer: the `NYC311_TRANSFORMER`
role has write access to SILVER and GOLD but `SELECT`-only on BRONZE. The `NYC311_LOADER`
role can write only to BRONZE. The `NYC311_REPORTER` role can read only GOLD. No role can
blur these boundaries through direct SQL.

### The diagnostic principle that drives this decision

> "When a data quality issue surfaces, you must be able to distinguish bad source data
> from bad transformation logic. Without Bronze and Silver as separate, checkpointable
> layers, that distinction disappears. You are left debugging a black box."

This principle applies at scale where a single pipeline processes hundreds of millions
of rows from dozens of sources. The mediation cost of an extra layer is paid once; the
debuggability benefit is recouped on every incident.

## Consequences

**Three failure points instead of one.** The pipeline can fail at ingest (Bronze),
at Silver cleaning (Databricks PySpark), or at dbt model execution (Gold). Each failure
point is isolated by design: the Airflow DAG (`nyc311_pipeline`) has separate tasks for
each layer, and a failure in `load_silver` does not invalidate the `ingest_raw` output
that successfully wrote to Bronze. Replay from any checkpoint is safe because:
- Bronze writes are idempotent (existing partitions are skipped unless `force_reload=true`).
- Silver jobs are idempotent (overwrite-safe Delta merge on `unique_key`).
- dbt runs are idempotent (DROP-AND-RECREATE for table materializations).

**Two compute environments.** Running both Databricks and Snowflake adds cost and
operational surface area compared to a pure-ELT Snowflake-only architecture. At this
project's data scale (~35M rows from NYC 311 historical dataset), the Databricks cluster
cost for Bronze→Silver processing is a few dollars per run. The incremental operational
complexity is accepted as a deliberate portfolio showcase of distributed compute skills.

**Silver-to-Gold handoff is the architectural seam.** The Databricks Silver job writes
to the Snowflake SILVER schema (via the Databricks Snowflake connector or Snowpipe). dbt
reads from this schema under the TRANSFORMER role. If the handoff mechanism changes
(e.g. replacing the connector with Snowpipe ingest from ADLS directly), only the Silver
job changes — dbt models are unaffected because they read from a stable schema contract,
not from a file path. This decoupling is intentional.
