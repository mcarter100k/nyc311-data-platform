# ADR 002: Transformation Tool Selection

**Status:** Accepted
**Date:** 2026-05-27

## Context

After raw NYC 311 data is ingested and cleaned by the Databricks Bronze→Silver pipeline, a
second transformation tool is needed to produce the Gold dimensional model (staging →
intermediate → marts). The tool must operate at the SQL layer against data already resident
in Snowflake, enforce a testable schema contract, generate lineage documentation, and integrate
cleanly with the GitHub Actions CI/CD pipeline.

This decision defines where the Bronze→Silver boundary ends and the Silver→Gold boundary
begins. That boundary is explicit: Databricks owns all PySpark computation (deduplication,
type casting, partitioned writes); the SQL transformation tool owns everything from Silver
onward. The two tools must never overlap at the same layer — doing so creates an undebuggable
dependency chain.

## Options Considered

### dbt Core (with dbt-snowflake adapter)

Open-source SQL transformation framework. Models are pure SELECT statements compiled and
executed against Snowflake by dbt's CLI. Each model is a versioned file, making the full
transformation DAG inspectable via git blame. The built-in test framework (`unique`,
`not_null`, `accepted_values`, `relationships`) runs as generated SQL against live data.
Schema tests are declared in YAML alongside models, enabling peer review of data contracts
without running the pipeline. The `dbt docs generate` command produces a static HTML site
containing a full DAG lineage graph, column-level documentation, and test coverage — directly
publishable to GitHub Pages. The Snowflake adapter is maintained by dbt Labs and is the
most mature adapter in the dbt ecosystem, with first-class support for Snowflake-specific
features: warehouse sizing per model, query tags, transient table materialization, and
`CLUSTER BY` configuration. dbt Core runs headlessly from CLI, CI runner, or Airflow
BashOperator with no proprietary scheduler required.

### dbt Cloud

Adds a managed scheduler, web IDE, and Slim CI (only run models affected by a PR diff).
The cost is a per-seat or per-usage subscription on top of Snowflake credits. For a
portfolio project with a single developer and a pre-existing CI infrastructure (GitHub
Actions), the additional cost is unjustified. Slim CI is achievable in GitHub Actions via
`dbt build --select state:modified+` with a persisted state artifact. Rejected.

### Databricks Notebooks (extended Silver→Gold in PySpark)

The Silver→Gold transformation could remain inside Databricks, writing Gold tables directly
to Snowflake via the Snowflake Spark connector. This eliminates the two-tool story but
sacrifices every advantage of the SQL-first approach: no static DAG graph, no test
framework with schema contracts, no auto-generated documentation, and no diff-reviewable
SQL. PySpark notebooks are significantly harder to peer-review for SQL correctness than
SELECT statements. The Databricks approach also tightly couples compute cost to transformation
complexity — running PySpark to evaluate a simple GROUP BY is wasteful when Snowflake's
cost-per-credit model handles aggregations efficiently without cluster spin-up time.
Rejected for the Gold layer.

### Snowpark (Python/Scala DataFrame API in Snowflake)

Snowpark executes Python or Scala code inside the Snowflake engine. It is a valid choice
for teams that want Python expressiveness without leaving Snowflake. However, Snowpark
programs are stored procedures or UDFs — they have no native DAG graph, no built-in test
framework, and no documentation generation. The learning curve is higher than dbt SQL for
analysts, and the tooling ecosystem is substantially smaller. Snowpark is best suited for
ML feature engineering inside Snowflake, not for dimensional modeling. Rejected.

## Decision

**dbt Core** with the `dbt-snowflake` adapter for all Silver→Gold transformation logic.

The project implements a three-layer dbt model structure:
- **Staging** (`models/staging/`) — thin views over Silver source tables; rename to snake_case,
  cast types, add surrogate keys. No business logic.
- **Intermediate** (`models/intermediate/`) — business rules applied: borough standardization,
  resolution day calculation, complaint categorization, data quality filtering.
- **Marts** (`models/marts/`) — dimensional model: `dim_agency`, `dim_date`, `dim_location`,
  `fct_service_requests`, `fct_daily_volume`. Materialized as Snowflake tables.

The layering enforces the principle that each concern lives in exactly one place: source
aliasing in staging, business logic in intermediate, physical table structure in marts.

### Deliberate portfolio decision: publishable lineage graph

`dbt docs generate && dbt docs serve` produces a self-contained HTML application containing
the full DAG of all models, their source dependencies, column-level descriptions, and test
results. This artifact is checked into CI and deployed to GitHub Pages on every merge to
main. The purpose is not documentation for its own sake — it is to make architectural
thinking visible to any hiring reviewer or collaborator without requiring them to clone the
repository and understand the folder structure. A reviewer can open the lineage graph and see
immediately that Gold tables never reference Silver tables directly, that intermediate models
have no circular dependencies, and that every mart column has a declared test.

## Consequences

**dbt operates at the SQL layer only.** Complex transformations that require non-SQL
computation — window functions over unordered streams, ML inference, fuzzy string matching
at scale — must remain in Databricks and be resolved before data reaches the Silver layer.
This creates a hard layer contract: PySpark for Bronze→Silver, dbt SQL for Silver→Gold.
Violating this contract by pushing business logic into Databricks notebooks that write
directly to Gold schema bypasses the dbt test framework and breaks the lineage graph.

**Orchestration of `dbt run` is delegated to Airflow.** dbt Core has no built-in scheduler.
The Airflow DAG (`airflow/dags/nyc311_pipeline.py`) invokes `dbt run --target prod` via
BashOperator after the Databricks Silver job completes successfully. This dependency is
enforced at the DAG level, not by dbt (see ADR 005).

**The `dbt test` step is a blocking gate in CI.** Schema tests (`unique`, `not_null`,
`relationships`) and the singular test (`assert_resolution_days_nonnegative`) run as part
of every CI pipeline. A failing test blocks the merge. This is the data quality contract
between the transformation layer and any downstream consumer.

**Provider package pinning.** `dbt_utils` is pinned to `>=1.1.0, <2.0.0` in `packages.yml`.
The `generate_surrogate_key` macro changed behavior between dbt_utils 0.x and 1.x (MD5
hash inputs are now consistently lowercased and cast to string). Any upgrade must be
accompanied by a reconciliation check on surrogate keys already loaded into production
Gold tables, since key values will differ for previously computed rows.


---

## Amendment 2026-08-20 — the PySpark side was removed

This ADR chose dbt over continued PySpark for Gold, with Spark retained for
Bronze/Silver. The Databricks path was later removed entirely: it was specified
but never provisioned, and its transform module was imported only by a notebook
that never ran and by its own tests — while the pandas transform that runs
daily had no unit tests at all. The decision recorded here still holds; the
alternative it was weighed against no longer exists in the repo. Silver logic
now lives in `local/silver_transformations.py`, and the tests moved onto it.
