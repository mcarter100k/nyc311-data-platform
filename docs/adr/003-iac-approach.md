# ADR 003: Infrastructure as Code Approach

**Status:** Accepted
**Date:** 2026-05-27

## Context

Every cloud resource in this platform — Azure Data Lake Storage Gen2, the Databricks workspace,
and all Snowflake objects (databases, schemas, warehouses, roles, grants) — must be reproducible,
auditable, and deployable from CI without manual steps. The chosen tool must cover all three
providers from a single configuration surface.

A secondary requirement shaped this decision: grant structure (who can read and write which
Snowflake layer) must be encoded as version-controlled code, not as a runbook or a set of
ad-hoc SnowSQL commands that drift from reality over time.

## Options Considered

### Terraform (HashiCorp / OpenTofu)

Declarative HCL, plan/apply workflow with drift detection, and the most mature provider ecosystem
for data infrastructure. The AzureRM provider is the reference implementation for Azure IaC.
The `Snowflake-Labs/snowflake` provider supports every object type required (database, schema,
warehouse, role, grant). The `databricks/databricks` provider is maintained by Databricks
themselves. All three providers are version-pinnable and actively maintained. Industry presence
is unmatched — Terraform appears in the job requirements for the majority of senior data
engineering and platform engineering roles.

### SnowSQL scripts

Snowflake's native CLI and scripting language can execute DDL and GRANT statements, but the
approach is fundamentally procedural: scripts must be written to be idempotent manually (using
`CREATE OR REPLACE`, `IF NOT EXISTS`), there is no state file to diff against, drift is
undetectable, and there is no rollback primitive. For a single developer running a one-time
setup, SnowSQL is fine; for a team managing multiple environments, it becomes a maintenance
hazard.

### Pulumi

Pulumi's multi-language support (Python, TypeScript, Go) makes it attractive for teams that
want full programming language expressiveness in their IaC. However, the Snowflake provider
for Pulumi (`pulumi-snowflake`) is a thin wrapper around the Terraform provider and lags the
Terraform version in feature coverage. The Databricks Pulumi provider has similar gaps. For
a project where all three providers must be used simultaneously, Terraform's provider maturity
advantage is decisive.

### Manual UI Provisioning

Clicking through the Snowflake web console or the Azure portal produces no artifact, leaves no
audit trail beyond cloud activity logs, and is trivially reproducible only in the sense that a
human can repeat the clicks. Roles and grants configured through the UI are invisible to peer
review. This approach was rejected immediately.

## Decision

**Terraform** with the `Snowflake-Labs/snowflake`, `hashicorp/azurerm`, and
`databricks/databricks` providers.

Configuration is modularized into two child modules — `snowflake-foundation` and
`azure-infra` — to enforce separation of concerns and allow the modules to be planned and
applied independently. Remote state is stored in Azure Blob Storage (see `backend.tf`) using
blob lease-based locking, enabling safe concurrent use by multiple team members and CI runners.

All Snowflake grants are expressed as `snowflake_grant_privileges_to_role` resources,
making the least-privilege access model machine-readable, diffable, and enforceable in
code review.

## Consequences

**Provider version pinning is mandatory.** The `Snowflake-Labs/snowflake` provider underwent
a comprehensive API rewrite beginning at v0.87, deprecating a large set of grant resources
(`snowflake_role_grants`, `snowflake_schema_grant`, `snowflake_table_grant`) in favour of the
unified `snowflake_grant_privileges_to_role` resource. The pre-rewrite and post-rewrite
resource schemas are mutually incompatible. This configuration pins to `~> 0.89` (the first
stable series after the rewrite completed). Any provider upgrade must be preceded by a careful
review of the provider changelog; do not rely on automated Dependabot bumps without manual
validation.

**Remote state requires the storage account to exist before `terraform init`.** The Azure Blob
backend cannot create its own container. A one-time bootstrap script (documented in
`backend.tf`) must be run by a privileged operator before the first `terraform init`. This is
a deliberate trade-off: the alternative (storing state locally) removes the ability to run
`terraform apply` from CI and creates state-loss risk.

**The state file contains sensitive outputs** (role names, warehouse names, connection strings).
The Azure Blob container must have public access disabled and must be accessible only via the
storage account access key or a short-lived SAS token. The access key must be passed via the
`ARM_ACCESS_KEY` environment variable, not written into any `.tf` or `.tfvars` file.

**Drift detection is automatic but only on `terraform plan`.** If a developer modifies a
Snowflake role or grant manually through the console, Terraform will detect the drift at the
next plan run and propose reverting it. The CI pipeline (`.github/workflows/terraform.yml`)
runs `terraform plan` on every pull request targeting `main` for exactly this reason.

**The `SYSADMIN` role is used for provisioning.** This is Snowflake's recommended pattern for
creating databases and warehouses. `ACCOUNTADMIN` is deliberately not used for day-to-day
provisioning — it holds billing and replication privileges that are not required and that would
expand blast radius if the provisioning service user were compromised.
