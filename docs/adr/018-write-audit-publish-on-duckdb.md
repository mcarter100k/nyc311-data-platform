# ADR 018: Write-audit-publish on the DuckDB path

**Status:** Accepted
**Date:** 2026-11-08
**Relates to:** [ADR 005](005-orchestration-strategy.md) (write-audit-publish, specified for
Snowflake), [ADR 009](009-publish-grants-under-schema-swap.md) (grants under the Snowflake swap),
[ADR 017](017-history-lives-in-the-actions-cache.md) (Gold's history lives in the Actions cache)

## Context

Stage 4 of `local/local_runner.py` ran `dbt build` straight into the `gold` and `snapshots`
schemas. dbt runs models and tests in one pass, so when a test failed, the tables built before it
were already in Gold. The daily run limited the damage: it saves its database for the next run
only when the run succeeds. But within that run, the uploaded artifact and every check that reads
Gold saw the failed build, and a local run kept it.

ADR 005 specifies write-audit-publish for Snowflake: build and test in `GOLD_AUDIT`, then
`ALTER SCHEMA ... SWAP WITH GOLD` in one statement. DuckDB 1.5 has no equivalent. Tested on
2026-11-08: `ALTER SCHEMA ... RENAME` returns "Altering schemas is not yet supported", and moving
a table to another schema (`ALTER TABLE ... SET SCHEMA`) is not implemented either. What DuckDB
does support is transactional DDL: tables replaced inside a transaction are all replaced at
`COMMIT`, or none of them are after `ROLLBACK`.

Gold also carries state between runs, which a rebuild from empty would lose: the incremental fact
(`fct_service_requests`), the incremental `dim_location`, and the SCD2 agency snapshot.

## Decision

`local/wap.py` builds Gold in three steps.

1. **Write.** Drop any audit schemas left by a failed run. Copy every table in `gold` and
   `snapshots` into `gold_audit` and `snapshots_audit`. Then run
   `dbt build --vars '{"audit_suffix": "_audit"}'`. The schema macro already appends the suffix to
   every model and seed; the local snapshot's `target_schema` appends it too. The incremental
   models and the snapshot therefore build on real history, not on an empty schema.
2. **Audit.** Every dbt test (generic, singular and source) runs inside that build, against the
   audit schemas.
3. **Publish.** Only if the build exits 0: in one transaction, replace every Gold and snapshot
   table with a copy of its audited version, and recreate the staging views. Commit, then drop
   the audit schemas. Any error rolls the whole transaction back.

On failure, stage 4 exits non-zero and Gold is untouched. The audit schemas are kept, so the
failed build can be inspected (they ship in the daily run's artifact). The daily run's breach
issue fires as before.

Two checks stay outside the gate:

- `reconcile.py`, because it queries the live API: publishing would then depend on the city's API
  being up.
- The SLO checks, because they read `gold` directly and already fail the run. An SLO breach is
  reported after a publish, but what was published had passed every dbt test.

The Snowflake path is unchanged: still the schema swap in `dbt/macros/publish_gold.sql`,
specified and never run.

## Consequences

- **Measured on a copy of the production database** (741,384 fact rows, 2026-11-08):
  - A run whose data-quality log was stale failed `assert_dq_log_is_current`. Gold's row counts
    and row hashes were identical before and after, and the audit schemas were kept.
  - A passing run published in one transaction. The 741,384 rows survived, and the audit
    schemas were dropped.
  - Stage 4 took about 17 seconds either way.
- **The database file is larger.** Dropping the audit copy leaves free space that DuckDB reuses
  but does not return. The file settled at about 260–300 MB, against 225 MB before, over four
  runs. That is about 2.3 times the live data, and it stops growing.
- **Publishing copies data**, where Snowflake would only rename. That is acceptable at
  this size.
- **Gold tables lose nothing dbt relies on.** dbt-duckdb creates no constraints or indexes, so
  a copy (`CREATE TABLE ... AS SELECT`) is complete.
- **A view that read the audit schema** would break once that schema is dropped. Publishing
  refuses such a view, which rolls the transaction back. Today's three staging views read only
  Silver.
- **Tests:** `tests/local/test_wap.py` covers a failing test leaving Gold unchanged, a passing
  build publishing exactly what was tested, history surviving a publish, and a publish that fails
  halfway rolling back. A structural test keeps stage 4 on `wap.run`.
