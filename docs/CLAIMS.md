# Claims Register

Every load-bearing claim the README makes, mapped to the code that enforces it and the
test that verifies it. A claim that cannot fill both columns does not appear in the
README. Counts and links are additionally checked mechanically in CI by
[`scripts/check_claims.py`](../scripts/check_claims.py).

**How to read a citation.** `` `path/to/file.ext#"a unique string"` `` means: that
string occurs in that file exactly once, and `check_claims.py` fails the build if it
occurs zero times or several. `` `path/to/test.py::test_name` `` means that file
defines that function, and is checked the same way.

This register used to cite line numbers — `fct_service_requests.sql:118-128`. Line
numbers rot on every edit made above them, and nothing could see it: the link checker
did `target.split("#")[0]` and only asserted the *file* existed. On 2026-08-26 every
citation was read against the code: **two of nine were still right.** Six pointed at
wholly unrelated lines — the watermark citation landed on a comment about
`is_actioned`, the point-in-time-join citation on the coordinate columns, the
borough-coalesce citation on a paragraph about publish lag, the append-only-grant
citation on a warehouse USAGE grant — and a seventh stopped one line short of the
`order by` clause that was the entire point of the claim. The register whose whole
purpose is evidence had gone decorative in exactly the dimension its own checker
discarded. Every citation below is now an anchor string, which moves with the code
it names.
The second column is checked too, which is how
`test_airflow_dag_uses_write_audit_publish` — cited here, defined nowhere — was found.

| Claim | Enforcing code | Verifying test |
|---|---|---|
| Pipeline runs end-to-end locally on DuckDB | `local/local_runner.py` (5 stages) | `tests/local/test_local_gold.py` (dbt build against seeded DuckDB) |
| dbt stage is write-audit-publish for the Snowflake target: build+test in GOLD_AUDIT, atomic swap into GOLD. The local DuckDB path builds straight into GOLD — no Airflow task here passes `audit_suffix` | `dbt/macros/generate_schema_name.sql#"{{ base_schema ~ var('audit_suffix', '') }}"`, `dbt/macros/publish_gold.sql#"alter schema"` | `tests/test_dbt_architecture.py::test_generate_schema_name_macro_has_override_logic` covers the schema-name override. **The audit-suffix swap itself has no test** — it needs a warehouse. Previously this cell cited `test_airflow_dag_uses_write_audit_publish`, which has never existed |
| fct_service_requests is incremental MERGE on service_request_id, clustered on cast(created_date as date) | `dbt/models/marts/fct_service_requests.sql#"unique_key          = 'service_request_id'"`, `dbt/models/marts/fct_service_requests.sql#"cluster_by          = ["cast(created_date as date)"]"` | `tests/test_dbt_architecture.py::test_fct_service_requests_is_incremental`, `tests/test_dbt_architecture.py::test_fct_service_requests_cluster_key` |
| Incremental watermark is `_loaded_at` (pipeline time) with a 1-hour lookback | `dbt/models/marts/fct_service_requests.sql#"select dateadd('hour', -1, max(_loaded_at))"` | `tests/local/test_local_gold.py::test_incremental_lookback_picks_up_late_arriving_row` |
| Agency FK is assigned point-in-time on the SCD2 validity window; rebuilds are idempotent, no fan-out | `dbt/models/marts/fct_service_requests.sql#"and cast(r.created_date as date) >= a.valid_from"`, `dbt/models/marts/dim_agency.sql#"end                                 as valid_from,"` | `tests/local/test_local_gold.py::test_scd2_rename_versions_and_point_in_time_assignment`, `tests/local/test_local_gold.py::test_no_fanout_and_full_refresh_idempotent` |
| Snapshot dedup takes the most recent name so renames are detectable | `dbt/snapshots/agency_snapshot.sql#"order by created_date desc, agency_name"` | `tests/local/test_local_gold.py::test_scd2_rename_versions_and_point_in_time_assignment` |
| All fact→dimension joins are LEFT; NULL FKs documented, borough coalesced in the daily rollup | `dbt/models/marts/fct_service_requests.sql#"left join dim_location l"`, `dbt/models/marts/fct_daily_volume.sql#"coalesce(l.borough, 'UNSPECIFIED')                                      as borough,"` | `tests/test_dbt_architecture.py::test_fct_has_relationship_test_on_every_foreign_key`, over the manifest-declared tests in `dbt/models/marts/marts.yml` |
| is_overdue is three-valued: NULL while open | `dbt/models/marts/fct_service_requests.sql#"when r.status <> 'Closed'      then null"` | `dbt/tests/assert_is_overdue_null_while_open.sql` — a singular dbt test, run during `dbt build`. It is no longer "no test": keying on `resolution_days is null` alone let 4,139 open rows carry `is_overdue = false` |
| Calendar semantics immune to Snowflake WEEK_START (ISO day-of-week) | `dbt/models/marts/dim_date.sql#"immune to the Snowflake WEEK_START session"` | none yet — flagged in audit; candidate for the local harness |
| Every model lands in the GOLD schema (no gold_gold) | `dbt/macros/generate_schema_name.sql` | `tests/test_dbt_architecture.py::test_all_models_land_in_gold_schema` |
| LOADER role cannot UPDATE/TRUNCATE Bronze (append-only at the grant layer) | `terraform/modules/snowflake-foundation/main.tf#"loader_bronze_future_tables"` | `tests/test_pipeline_components.py::test_terraform_loader_bronze_grants_no_truncate` |
| Terraform validates without cloud credentials | whole `terraform/` tree | `tests/test_pipeline_components.py::test_terraform_validate` (behavioral, subprocess) |
| Source freshness keys on `_silver_timestamp`, not business dates | `dbt/models/staging/sources.yml#"loaded_at_field: _silver_timestamp"` | `tests/test_dbt_architecture.py::test_source_freshness_uses_silver_timestamp` |
| Status changes propagate through the fct incremental upsert — verified under DuckDB's delete+insert strategy; the Snowflake `merge` strategy is unverified (no warehouse) | `dbt/models/marts/fct_service_requests.sql` (merge on service_request_id), `local/models/marts/fct_service_requests.sql` (delete+insert) | `tests/local/test_local_gold.py::test_upsert_propagates_status_change` |
| Publish swap keeps reporter access (symmetric grants — spec) | `docs/adr/009-publish-grants-under-schema-swap.md` (Terraform follow-up listed there) | none until the Terraform lands — tracked in ADR 009 |
| Scheduled to run daily against the live API (spec until the badge is green — the badge IS the live status) | `.github/workflows/daily-run.yml` (cron 10:00 UTC + workflow_dispatch) | self-verifying: the workflow badge and run history |
| Live fetch is capped, single-retry, and fails on zero rows — red or fully green, never partial | `local/local_runner.py::fetch_live_records` | `tests/local/test_live_fetch.py` — mocked at the HTTP boundary, no network |
| SLO breach or pipeline failure files/updates a GitHub issue with the measured numbers | breach step in `.github/workflows/daily-run.yml` | none until the first real breach — will be observed, not simulated |
| docs/SLO.md shows byte-identical copies of the executable SLO queries | `scripts/check_claims.py::check_slo_doc_sync` | CI claim check (demonstrated failing on a perturbed doc) |
| README **and docs/** counts (pytest tiers, dbt tests, ADRs, fact and dimension models), links, link fragments, DAG task names, the dbt model inventory, and every citation in this table match the repo | marker system in `README.md` and `docs/ARCHITECTURE.md` | `scripts/check_claims.py::main` in CI (`.github/workflows/ci.yml`) |
| Every documentation guard can actually fail — none of them is vacuous | `scripts/check_claims.py` | `tests/test_doc_guards.py` — each check run against a synthetic tree with the guarded thing broken, plus the ADR carve-out proved to be a carve-out rather than a hole |
| Gold output agrees with the source, not just with itself: layer conservation, independently recomputed metrics, exact timestamps, live API spot-check | the full transform stack (`local_runner.py` + `local/models/`) | `local/reconcile.py` — run after any local pipeline run; exit 0 = reconciled |
| The local/ DuckDB mirror tracks the dbt/ models — every intentional dialect divergence is registered; unregistered drift fails the build | `scripts/model_drift_baseline.json` (the divergence register) | `scripts/check_model_drift.py` in CI (`.github/workflows/ci.yml`) |

## Deleted from the README rather than listed here

Claims removed because no enforcing code exists (see ADR 008 and the audit):

- "updated automatically every morning" / any live-schedule claim — no deployment.
- "syncs to Snowflake via Snowpipe or the the transform layer Snowflake connector" — no sync
  code exists; the mechanism is an open decision (ADR 008).
- "zero silent failures" — DQ threshold breaches warn by design and do not fail.
- "runs in under 5 seconds on any machine" — depended on a hardcoded venv path and on
  the unit tier silently skipping.
- "0.02% of records dropped" and other dataset statistics — not reproducible from the repo.
- "cost-scalability analysis" — the document does not exist.


---

*Amendment 2026-08-20 — the Databricks path and the `azure-infra` Terraform
stub were removed. Claims that referenced unrun notebooks, the cloud DAG, or
Azure resources were deleted rather than restated: a claim whose subject no
longer exists cannot be evidenced.*
