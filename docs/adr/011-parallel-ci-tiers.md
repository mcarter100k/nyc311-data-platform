# ADR 011: Three Parallel CI Tiers as Independent Required Checks

**Status:** Accepted
**Date:** 2026-08-18

## Context

CI was one sequential job ("Compile and test" in dbt.yml) with two structural
problems. First, a red build named no failure class — a dbt parse error, a
claims drift, and a Gold-semantics regression all painted the same single
check red. Second, and worse: the PySpark unit tier **never ran in CI at
all** — the pytest invocation carried `--ignore=tests/unit` from the
workflow's origin, and `tests/unit/conftest.py`'s `importorskip` meant that
even without the ignore flag the tier would have silently skipped on a
runner without pyspark. The repo's own audit history shows what silently-
never-exercised paths cost.

## Decision

One workflow (`ci.yml`, replacing `dbt.yml`), three jobs with no `needs:`
edges — fully parallel, each intended as its own required status check:

| Job name (exact, API) | Failure class it isolates |
|---|---|
| `fast-gate` | the repo disagreeing with itself: dbt parse, structural pytest, claim checker, mirror-drift checker, actionlint |
| `unit-pyspark` | transformation logic: Silver functions on a real SparkSession |
| `behavioral-duckdb` | Gold semantics: the dbt project built twice on seeded DuckDB (watermark, SCD2 join, upsert) |

Why separate *required checks* rather than one job with stages: a red check
names its failure class in the PR UI without opening logs; the tiers share
no dependencies, so serializing them buys nothing but wall time; and branch
protection can require each independently, so weakening one gate is a
visible settings change rather than an invisible flag edit.

**Zero-skip enforcement.** `unit-pyspark` writes a junit report and fails
unless `tests > 0 and skipped == 0`. The conftest `importorskip` stays (it
is correct for laptops); in CI, a skip or an empty collection is a red
build. The tier that was hollow for the repo's entire history can no longer
hollow itself quietly.

**Pins.** `pyspark==3.5.1` on Temurin 17 — pinned in the workflow, not a
requirements file, because pyspark is a CI-only concern here (notebooks run
on Databricks runtimes; local developers may legitimately skip the tier).
This is the combination the tier was verified against. `dbt-duckdb==1.7.4`
continues to come from `local/requirements.txt`.

**actionlint runs unconditionally** in fast-gate: it finishes in seconds,
and a changed-files guard would add a third-party action this repo doesn't
otherwise depend on. Its shellcheck layer (active on ubuntu runners, where
shellcheck is preinstalled — a divergence the first CI run exposed, since a
Mac without shellcheck lints more weakly) gates at `--severity=warning`:
info/style advisories first fired on dbt-docs.yml, the house-standard file
this restructure is not allowed to rewrite, and advisories are not defects.

**Dependabot** (weekly; github-actions + both pip roots) groups minor+patch
per ecosystem; majors arrive solo so breaking bumps are never buried.

## Rejected additions — considered, not deferred by accident

- **dbt slim CI / `state:modified+`** — the project parses in seconds; state
  comparison adds artifact plumbing for no wall-time win at this size.
- **Coverage gates** — the suite's value here is behavioral specificity, not
  a percentage; a threshold would incentivize padding.
- **Python version matrix** — the deploy targets pin exact interpreters;
  extra versions test nothing anyone ships.
- **Nightly scheduled test runs** — the daily-run workflow already exercises
  the pipeline against live data on a schedule; a second cron would blur the
  operational/gating separation ADR 010 draws.
- **SHA-pinning actions** — real supply-chain hardening, deliberately
  deferred until Dependabot is observed handling action updates; pinning
  without automation rots.
- **Path-filtering fast-gate** — the gate is the always-on floor; filters
  save seconds and create "why didn't CI run" confusion.

## Consequences

- Branch protection should require exactly: `fast-gate`, `unit-pyspark`,
  `behavioral-duckdb`. These names are API — renaming them breaks
  protection silently, so they change only with an ADR amendment.
- `daily-run.yml` (ADR 010) is deliberately untouched: operational cron and
  gating CI stay separate surfaces.
- The README badge tracks the `CI` workflow as a whole; per-job status lives
  in the PR checks UI.
