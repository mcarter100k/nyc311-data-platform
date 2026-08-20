# ADR 012: The repository's own infrastructure is Terraform, and it is applied

**Status:** Accepted
**Date:** 2026-08-20
**Relates to:** [ADR 003](003-iac-approach.md) (Terraform as the IaC surface), [ADR 008](008-prototype-scope.md) (what is deferred), [ADR 011](011-parallel-ci-tiers.md) (three required checks)

## Context

Terraform in this repo described a Snowflake account that has never existed. It
validated in CI and never ran, which made "Infrastructure as Code" a claim
backed by syntax checking rather than by infrastructure.

Meanwhile the project depended on GitHub infrastructure that was managed by
neither Terraform nor anything else:

- The two operational labels the breach automation writes to were created
  imperatively on **every scheduled run** — `gh label create ... --force || true`
  — infrastructure produced as a job side effect, with failure swallowed.
- **`main` was unprotected.** ADR 011 said branch protection *should* require
  `fast-gate`, `unit` and `behavioral-duckdb`; it said *should* because nobody
  had configured it. The README meanwhile claimed three **required** checks. The
  checks ran; nothing made them required.
- **GitHub Pages was never enabled**, which is why `dbt-docs.yml` failed at
  *Configure GitHub Pages* on every run for three days after the workflow itself
  was fixed. Two independent faults, the first masking the second.

## Decision

A second Terraform root module, `terraform/github/`, manages the repository's
own infrastructure — labels, branch protection, Pages, repo settings — and **is
applied**, with state on disk.

Two root modules rather than one, deliberately: a single root would make
`terraform plan` require Snowflake credentials *and* a GitHub token at the same
time. Splitting them means this module can be applied by anyone holding a token,
while the Snowflake module stays what it honestly is — a design document,
validated in CI.

Existing resources were **imported**, not recreated. The first plan read
`3 to add, 1 to change, 0 to destroy` with 37 repository attributes unchanged;
that no-op is the evidence the import matched reality. Declaring a managed
resource without matching live state is how IaC adoption breaks the thing it
was meant to protect.

## Consequences

- "Three parallel required checks" is now **true** rather than aspirational.
  Branch protection enforces exactly `fast-gate`, `unit`, `behavioral-duckdb`.
- The breach automation may assume its labels exist; their colour and meaning
  are reviewable in a diff instead of buried in a shell line.
- The docs site publishes. The Pages fault that survived the workflow fix is
  closed by configuration rather than by clicking.
- Terraform is no longer a claim about a warehouse nobody can see. Part of it
  runs; the part that does not is labelled as specification.

**Deliberate omissions**, each an argument rather than an oversight:

- **No required pull request reviews.** A single maintainer cannot approve their
  own PR; requiring reviews would make merging impossible, not safer.
- **`enforce_admins = false`.** On a solo repo, enforcing against admins means a
  broken workflow locks out the only person who could repair it.
- **`strict = false`** on required checks — requiring branches to be current
  with main forces a rebase on every unrelated merge, and the tiers take about a
  minute. The churn costs more than the staleness risk.

**State is local and gitignored.** Honest for one maintainer: no second operator
to race with, and state should not sit in a public repo. A team needs a remote
backend with locking; that pattern is already written in `terraform/backend.tf`.
