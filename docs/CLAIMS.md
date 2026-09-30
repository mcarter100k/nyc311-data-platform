# Claims Register

Each load-bearing claim in the README, the code that makes it true, and what checks it. A claim
that cannot fill both columns is not made in the README. Counts and links are also checked in CI
by [`scripts/check_claims.py`](../scripts/check_claims.py).

**How to read a citation.** `` `path/to/file.ext#"a unique string"` `` means that string occurs in
that file exactly once; `check_claims.py` fails the build if it occurs zero times or several.
`` `path/to/test.py::test_name` `` means that file defines that function, checked the same way.
Citations use strings, not line numbers, because a string moves with the code it names and a line
number goes stale on every edit above it.

| Claim | Enforcing code | Verified by |
|---|---|---|
| Pipeline runs end-to-end locally on DuckDB | `local/local_runner.py` (5 stages) | `tests/local/test_local_gold.py` (dbt build against seeded DuckDB) |
| The daily run re-fetches the last 37 days; scheduled runs, manual runs and the runner agree | `local/local_runner.py#"LIVE_DAYS    = 37"` | `tests/test_pipeline_components.py::test_daily_run_scheduled_and_manual_windows_agree` |
| Live fetch is capped, retries transient faults twice, and fails on zero rows: red or fully loaded, never partly loaded | `local/local_runner.py::fetch_live_records` | `tests/local/test_live_fetch.py` (mocked at the HTTP boundary, no network) |
| Write-audit-publish on Snowflake only: build and test in GOLD_AUDIT, then swap into GOLD. The DuckDB path builds straight into GOLD; nothing in the repo passes `audit_suffix` | `dbt/macros/generate_schema_name.sql#"{{ base_schema ~ var('audit_suffix', '') }}"`, `dbt/macros/publish_gold.sql#"alter schema"` | `tests/test_dbt_architecture.py::test_all_models_land_in_gold_schema` covers the schema-name override. **The audit-suffix swap itself has no test**; it needs a warehouse |
| Every model lands in the GOLD schema (no `gold_gold`) | `dbt/macros/generate_schema_name.sql` | `tests/test_dbt_architecture.py::test_all_models_land_in_gold_schema` |
| fct_service_requests is incremental MERGE on service_request_id, clustered on cast(created_date as date) | `dbt/models/marts/fct_service_requests.sql#"unique_key          = 'service_request_id'"`, `dbt/models/marts/fct_service_requests.sql#"cluster_by          = ["cast(created_date as date)"]"` | `tests/test_dbt_architecture.py::test_fct_service_requests_is_incremental`, `tests/test_dbt_architecture.py::test_fct_service_requests_cluster_key` |
| Incremental watermark is `_loaded_at` (pipeline time) with a 1-hour lookback | `dbt/models/marts/fct_service_requests.sql#"select dateadd('hour', -1, max(_loaded_at))"` | `tests/local/test_local_gold.py::test_incremental_lookback_picks_up_late_arriving_row` |
| Status changes propagate through the incremental upsert: verified under DuckDB's delete+insert; the Snowflake `merge` is unverified (no warehouse) | `dbt/models/marts/fct_service_requests.sql` (merge on service_request_id), `local/models/marts/fct_service_requests.sql` (delete+insert) | `tests/local/test_local_gold.py::test_upsert_propagates_status_change` |
| Agency key is assigned point-in-time on the SCD2 validity window; rebuilds are idempotent, no fan-out | `dbt/models/marts/fct_service_requests.sql#"and cast(r.created_date as date) >= a.valid_from"`, `dbt/models/marts/dim_agency.sql#"end                                 as valid_from,"` | `tests/local/test_local_gold.py::test_scd2_rename_versions_and_point_in_time_assignment`, `tests/local/test_local_gold.py::test_no_fanout_and_full_refresh_idempotent` |
| Snapshot dedup takes the most recent name, so renames are detectable | `dbt/snapshots/agency_snapshot.sql#"order by created_date desc, agency_name"` | `tests/local/test_local_gold.py::test_scd2_rename_versions_and_point_in_time_assignment` |
| All fact→dimension joins are LEFT; NULL keys are documented; borough is coalesced in the daily rollup | `dbt/models/marts/fct_service_requests.sql#"left join dim_location l"`, `dbt/models/marts/fct_daily_volume.sql#"coalesce(l.borough, 'UNSPECIFIED')                                      as borough,"` | `tests/test_dbt_architecture.py::test_fct_has_relationship_test_on_every_foreign_key`, over the tests declared in `dbt/models/marts/marts.yml` |
| is_overdue is three-valued: NULL while open | `dbt/models/marts/fct_service_requests.sql#"when r.status <> 'Closed'      then null"` | `dbt/tests/assert_is_overdue_null_while_open.sql`, a singular dbt test run in every `dbt build` |
| LOADER role cannot UPDATE or TRUNCATE Bronze (append-only at the grant layer) | `terraform/modules/snowflake-foundation/main.tf#"loader_bronze_future_tables"` | `tests/test_pipeline_components.py::test_terraform_loader_bronze_grants_no_truncate` |
| The publish swap keeps REPORTER access (grants on both GOLD and GOLD_AUDIT; spec) | `terraform/modules/snowflake-foundation/main.tf#"reporter_gold_audit_future_tables"` | `.github/workflows/terraform.yml` validates the syntax only. The effect is unverified: the module has never been applied |
| Terraform validates without cloud credentials | both root modules, `terraform/` and `terraform/github/` | `.github/workflows/terraform.yml` (fmt + validate; not a required check) |
| Source freshness keys on `_silver_timestamp`, not business dates | `dbt/models/staging/sources.yml#"loaded_at_field: _silver_timestamp"` | `tests/test_dbt_architecture.py::test_source_freshness_uses_silver_timestamp` |
| Scheduled to run daily against the live API | `.github/workflows/daily-run.yml` (cron 10:00 UTC + manual dispatch) | observed: the workflow badge and run history |
| SLO breach or pipeline failure files or updates a GitHub issue with the measured numbers | breach step in `.github/workflows/daily-run.yml` | observed: issue #7, and "Daily run failed" comments on #64 (2026-08-27..29) |
| The heartbeat breaches when the daily run is disabled or has not succeeded on main within 30 hours | `scripts/check_daily_run_heartbeat.py::evaluate` | `tests/test_pipeline_components.py::test_heartbeat_fails_when_the_last_success_is_older_than_the_threshold`, `tests/test_pipeline_components.py::test_heartbeat_fails_a_disabled_workflow_even_with_a_fresh_success`, `tests/test_pipeline_components.py::test_heartbeat_workflow_uses_one_threshold` |
| docs/SLO.md shows byte-identical copies of the executable SLO queries | `scripts/check_claims.py::check_slo_doc_sync` | `tests/test_doc_guards.py::test_slo_doc_sync_fails_when_the_doc_differs_from_the_query` |
| README **and docs/** counts (pytest tiers, dbt tests, ADRs, fact and dimension models), links, link fragments, DAG task names, the dbt model inventory, and every citation in this table match the repo | marker system in `README.md` and `docs/ARCHITECTURE.md` | `scripts/check_claims.py::main` in CI (`.github/workflows/ci.yml`) |
| Every documentation guard can fail | `scripts/check_claims.py` | `tests/test_doc_guards.py`: each check run against a synthetic tree with the guarded thing broken, plus proof that the ADR carve-out does not swallow other files |
| Gold agrees with the source, not just with itself: layer conservation, independently recomputed metrics, exact timestamps, live API spot-check | `local/local_runner.py` + `local/models/` | `local/reconcile.py`, run by hand after a local pipeline run; exit 0 = reconciled |
| The local/ DuckDB copy tracks the dbt/ models: every dialect difference is registered, and any other difference fails the build | `scripts/model_drift_baseline.json` (the register) | `scripts/check_model_drift.py` in CI (`.github/workflows/ci.yml`) |

## Claims removed from the README

Removed because no code makes them true (see ADR 008):

- "syncs to Snowflake via Snowpipe or the Databricks Snowflake connector": no sync code exists;
  how Silver would reach Snowflake is an open decision (ADR 008).
- "zero silent failures": a data-quality threshold breach only sets
  `fct_data_quality.is_rolling_threshold_breached`; nothing fails or alerts on it.
- "runs in under 5 seconds on any machine": depended on a hard-coded virtualenv path and on the
  unit tier silently skipping.
- "0.02% of records dropped" and other dataset statistics: not reproducible from the repo.
- "cost-scalability analysis": the document does not exist.

Claims about the removed Databricks path, its cloud DAG and the Azure Terraform stub were deleted
with that code (2026-08-20): a claim whose subject no longer exists cannot be evidenced.
