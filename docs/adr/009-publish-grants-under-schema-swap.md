# ADR 009: Reporter Grants Under Schema-Swap Publishing

**Status:** Accepted
**Date:** 2026-08-17

## Context

Two showcased decisions in this repo currently cancel each other out.

1. **Grants as code, future-proofed.** The Terraform grant matrix gives
   `NYC311_REPORTER` `USAGE` on the GOLD schema plus `SELECT ON FUTURE TABLES/VIEWS`
   (`terraform/modules/snowflake-foundation/main.tf:380-411`), so any table dbt creates
   automatically becomes readable by BI without a re-apply.

2. **Write-audit-publish.** The dbt stage builds and tests everything in `GOLD_AUDIT`,
   then publishes atomically with `ALTER SCHEMA GOLD_AUDIT SWAP WITH GOLD`
   (`dbt/macros/publish_gold.sql`).

The conflict: **Snowflake grants attach to the schema object, not to its name.** A swap
renames the two objects. After the first publish, the object *named* GOLD is the former
audit object — which carries none of REPORTER's grants, because Terraform granted the
original GOLD object. The ping-pong means the two physical objects alternate between the
GOLD and GOLD_AUDIT names every run, and only one of them is granted. BI access breaks
on the first publish, and future-table coverage breaks with it: tables are created in
whichever object is currently named GOLD_AUDIT, whose future grants Terraform never
defined.

## Options

### Option A — symmetric grants on both schema objects (chosen)

Terraform provisions `GOLD_AUDIT` as a first-class schema and duplicates the full
REPORTER and TRANSFORMER grant sets — schema `USAGE`, `SELECT ON FUTURE TABLES`,
`SELECT ON FUTURE VIEWS` — on **both** schema objects. Then it no longer matters which
object currently holds which name: whichever one is named GOLD after any number of
swaps carries reporter access, and tables created in the audit object inherit future
grants that survive the swap.

- Pros: preserves the single-statement, atomic publish — the core WAP guarantee that no
  reader ever observes a half-published state. Keeps every grant declarative in
  Terraform. No query-path indirection.
- Cons: REPORTER can also read the schema currently named GOLD_AUDIT (the previous
  published build — stale but validated data, not unaudited data; the audit object only
  carries a *new* build transiently during the dbt_build task, before tests complete...
  strictly, mid-build reads of GOLD_AUDIT are possible). Accepted: BI tools connect to
  GOLD by name; reading GOLD_AUDIT requires deliberately naming it.

### Option B — publish by view swap

Keep tables permanently in a build schema; GOLD contains only
`CREATE OR REPLACE VIEW gold.x AS SELECT * FROM build.x` views, recreated after tests
pass. Views are recreated inside the same GOLD schema object, so its grants and
future-view grants remain continuously valid.

- Pros: grant model untouched; no symmetric duplication.
- Cons: publish becomes N statements — one per relation — so there is a window where
  view A points at the new build and view B at the old one. That reintroduces exactly
  the cross-table inconsistency that write-audit-publish exists to eliminate. Adds a
  view layer to every BI query and a second thing (the view set) that can drift from
  the model set.

## Decision

**Option A.** Atomicity of the publish is the point of the design; Option B trades it
away to avoid writing ~6 more grant resources. The extra grants are mechanical,
declarative, and self-documenting in the same file as the rest of the matrix.

Same reasoning extends to the `snapshots` schema: it is referenced by the dbt snapshot
config but never provisioned. Terraform should own it alongside `GOLD_AUDIT`.

## Implementation (follow-up — spec-level until then)

Nothing is provisioned yet (ADR 008), so today this ADR changes the specification, not
a live system. The Terraform follow-up:

1. `snowflake_schema` resources for `GOLD_AUDIT` and `SNAPSHOTS`.
2. Mirror `reporter_gold_*` and `transformer_gold_*` grant resources onto `GOLD_AUDIT`;
   grant TRANSFORMER write on `SNAPSHOTS`.
3. Ownership: `ALTER SCHEMA ... SWAP WITH` requires the executing role to own both
   schemas (or hold MODIFY per Snowflake's swap rules) — assign OWNERSHIP of both GOLD
   and GOLD_AUDIT to `NYC311_TRANSFORMER`, which also removes the need for the
   `CREATE SCHEMA` fallback in `publish_gold.sql`.

## Consequences

- The README's future-grants paragraph now states the symmetric-grant requirement
  instead of claiming the old single-schema grant set already survives publishing.
- Until the follow-up lands, `terraform apply` + one pipeline run would still strand
  REPORTER — tracked by this ADR's implementation list, which is the honest state.
