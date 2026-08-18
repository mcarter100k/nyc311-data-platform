# ADR 001: Warehouse Selection

**Status:** Accepted
**Date:** 2026-05-27

## Context

The platform requires an analytical warehouse to serve the Gold layer — the dimensional model
produced by dbt from cleaned Silver data. The warehouse must meet several criteria: SQL-first
query interface for BI tooling, strong dbt adapter maturity, separation of storage and compute
so that idle periods incur near-zero cost, and native connectivity to Azure Data Lake Storage
Gen2 for the Silver-to-Gold handoff from Databricks.

A secondary criterion that shaped this decision: the warehouse is the durable serving layer
for BI dashboards queried by analysts concurrently with ongoing ETL runs. Any tool that
conflates ETL compute with serving compute creates priority starvation between the two
workloads — a risk that must be eliminated by design, not by operational convention.

## Options Considered

### Snowflake

Fully managed columnar warehouse with strict separation of storage and compute. Virtual
warehouses are independent compute clusters that auto-suspend after configurable inactivity
and auto-resume on first query — the cost model is genuinely pay-per-second of active compute,
not a reserved-instance model that charges during idle hours. The `dbt-snowflake` adapter is
maintained by dbt Labs and is the most mature adapter in the dbt ecosystem: it supports
Snowflake-native materializations (`transient`, `incremental` with `MERGE`), `CLUSTER BY`
configuration, query tagging, and virtual warehouse selection per dbt target. Snowflake's
BI connector ecosystem (Tableau, Looker, Power BI, Mode) is mature and well-documented.
The Terraform provider (`Snowflake-Labs/snowflake`) supports full declarative provisioning
of databases, schemas, warehouses, roles, and grants — enabling the least-privilege model
defined in ADR 003.

### Databricks SQL

Databricks SQL Warehouses run on the same Databricks compute plane used by the Bronze and
Silver transformation jobs. This creates two architectural problems:

1. **Priority starvation.** When the Silver PySpark job and an analyst BI query compete
   for the same Databricks cluster pool or SQL warehouse slots, there is no hard isolation
   boundary. ETL jobs running during business hours will cause latency spikes for dashboards,
   and vice versa. Snowflake's virtual warehouse model makes this separation a first-class
   architectural primitive, not an operational concern.

2. **Adapter maturity.** The `dbt-databricks` adapter is well-maintained but Databricks SQL
   is optimized for Delta Lake materializations and Lakehouse SQL patterns. The dimensional
   model produced by this project (star schema with foreign key relationships and
   `CLUSTER BY` semantics) is a better fit for Snowflake's column-store optimizations.

Databricks SQL is the correct choice for projects that do not leave the Databricks
ecosystem and do not need strict compute isolation between ETL and BI.

### Azure Synapse Analytics

Azure Synapse Analytics offers native integration with ADLS Gen2 (external tables read
directly from the storage layer without data movement) and tight Azure AD identity
federation. However, the `dbt-synapse` adapter has historically lagged behind the
Snowflake and BigQuery adapters in feature coverage and stability. Synapse's dedicated
SQL pool (MPP architecture) requires pre-provisioning a fixed compute tier — there is no
auto-suspend equivalent, meaning idle compute costs accrue continuously. The serverless
SQL pool avoids this but does not support DDL operations that dbt requires (`CREATE TABLE`,
`MERGE`). Synapse Analytics is the correct choice for organizations already heavily invested
in the Azure data platform stack that require zero data movement from ADLS to the warehouse.

## Decision

**Snowflake** is the Gold layer warehouse. Its auto-suspend virtual warehouse eliminates
idle compute cost, its dbt adapter is the industry reference implementation, and its
strict compute isolation between the ETL warehouse and the BI warehouse prevents the
priority starvation failure mode that Databricks SQL cannot eliminate by design.

The interview-defensible framing of this architecture:
> "Databricks is the compute engine for transformation. Snowflake is the serving layer
> for queries. Conflating the two forces ETL jobs and analyst queries to compete for the
> same resources. Separating them means a long-running dbt run has zero impact on
> dashboard latency."

## Consequences

**Data movement cost.** Raw and Silver data live in Azure Data Lake Storage Gen2 (East US 2).
Snowflake on AWS (the default free-trial deployment) incurs Azure egress charges for the
Silver-to-Snowflake sync performed by the Databricks Silver job. At the scale of the NYC 311
dataset (~35M rows, ~15 GB), this cost is negligible in development.
*(Correction, 2026-08-18: figures describe the pre-split dataset; erm2-nwe9 has covered 2020–present, ~22M rows, since the city's Dec 2025 split. Decision unaffected.)*

**At 10× production volume**, the correct mitigation is to deploy Snowflake on Azure in the
same region as ADLS (East US 2). Snowflake supports Azure as a cloud provider for all tiers.
Co-locating eliminates egress entirely and reduces sync latency from minutes to seconds via
Private Link. This migration path is a natural evolution — no dbt SQL changes are required,
only the Terraform Snowflake provider configuration and the `account` identifier in profiles.yml.

**Snowflake SQL dialect.** Certain Snowflake-specific syntax is used in the dbt models:
`TIMESTAMP_NTZ`, `DATEDIFF`, `DAYOFWEEK`, `to_char` with Oracle format strings, and
`CLUSTER BY` on table materializations. Migrating to a different warehouse would require
SQL dialect translation in the mart models. This trade-off is accepted: dialect lock-in
is confined to the Gold layer (dbt models), not to the ingestion or cleaning logic, which
lives entirely in PySpark and is warehouse-agnostic.

**Role-based access.** Snowflake's RBAC model (provisioned by Terraform in ADR 003) allows
the BI warehouse to run under the `NYC311_REPORTER` role, which has `SELECT` on GOLD only.
ETL runs under `NYC311_TRANSFORMER`. This isolation is enforced at the Snowflake account
level and cannot be bypassed by misconfigured BI tool credentials.
