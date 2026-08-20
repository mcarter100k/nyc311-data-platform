# ADR 008: Prototype Scope — Cloud Services Specified, Not Provisioned

**Status:** Accepted
**Date:** 2026-08-17

## Context

This repository is a reference implementation built on a personal budget. The Azure,
Databricks, and Snowflake services it targets bill by provisioned capacity and usage;
running them continuously to demonstrate a portfolio pipeline would cost real money for
zero additional design signal. Earlier revisions of the README described the platform as
if it were deployed ("runs every morning"). A claim-verification audit flagged every such
statement: asserting live wiring that does not exist is a worse defect in a reference
repo than the absence of the wiring itself.

## Decision

The repo's identity is a **reference implementation**: design reasoning, infrastructure
as code, a dimensional model, and test scaffolding — with one fully executable path.

**Real (runs, and CI or a command proves it):**

- The end-to-end pipeline logic, executed locally against DuckDB:
  `local/local_runner.py` runs ingest → bronze → silver → dbt gold → sample queries
  against the live Socrata API with no cloud accounts.
- The dbt project: parses in CI (`.github/workflows/dbt.yml`), architecture verified by
  the pytest suite against the compiled manifest.
- Terraform: passes `terraform validate` in CI (`.github/workflows/terraform.yml`).
  It has never been applied; no state file exists.
- Silver transformation functions: unit-tested against a local SparkSession
  (`tests/unit/test_silver_transformations.py`).

**Deferred (specified, not provisioned):**

- Azure resource group, ADLS Gen2, Databricks workspace — `terraform/modules/azure-infra`
  is an explicit stub; the module call in `terraform/main.tf` is commented out.
- Snowflake account objects — fully specified in
  `terraform/modules/snowflake-foundation`, never applied.
- Scheduled execution — `airflow/dags/nyc311_pipeline.py` is the orchestration spec;
  no Airflow deployment runs it.
- **Databricks Silver → Snowflake SILVER data movement** — the dbt sources declare the
  requirement (`dbt/models/staging/sources.yml`), but the mechanism (Snowpipe, the
  Databricks Snowflake connector, or external tables) is an open design decision. It is
  deliberately not chosen here: choosing it correctly depends on latency and cost
  constraints that only exist once the cloud side is real. Until then, any named
  mechanism in the docs would be fiction. The dbt incremental watermark's 1-hour
  lookback constrains this decision: whatever mechanism is chosen must land rows in
  Snowflake less than one hour after `_silver_timestamp` is stamped, or rows are
  silently lost (see `dbt/models/marts/fct_service_requests.sql`, incremental filter).

## Why deferral is correct at this scope

1. The artifact being demonstrated is judgment — layer contracts, grant matrices,
   idempotent write semantics, test design — none of which requires provisioned
   capacity to evaluate.
2. A provisioned-but-idle platform decays: credentials expire, providers drift, and the
   repo starts making claims that silently rot. A local execution path plus validate-only
   CI is the largest honest surface a zero-budget repo can keep continuously true.
3. The one thing deferral genuinely costs — evidence that the cloud wiring works — could
   not be faked anyway. Claiming it was the defect this ADR removes.

## Flip condition

Provision when either: (a) a sponsored or employer sandbox account exists, or (b) the
repo's purpose changes from reference to service. First provisioning steps at that
point: implement the azure-infra module, choose the Silver→Snowflake sync mechanism
against the 1-hour watermark constraint above, and add a smoke-test workflow that runs
`dbt build` against the real warehouse nightly.

## Consequences

- Every README claim about runtime behavior must be phrased as specification
  ("the DAG specifies a daily 06:00 UTC run") or scoped to the local runner.
- The pytest suite and the local DuckDB path are the only places where "it works" may be
  asserted without qualification.


---

## Amendment 2026-08-20 — scope narrowed to Snowflake alone

This ADR scoped Azure, Databricks, and Snowflake as specified-not-provisioned.
Two of the three are now simply gone: the Databricks notebooks and the
`azure-infra` Terraform stub were deleted rather than carried as unverifiable
claims. What remains deferred is one thing — a Snowflake account. The dbt
project targets Snowflake and is validated in CI; the mechanism that would load
Silver into it remains an open decision.

The reasoning for deletion: unrun *logic* is a liability, because it invites
claims nothing can verify. Unapplied *declarative* config (the Terraform
module) is a design document, and is labelled as one.
