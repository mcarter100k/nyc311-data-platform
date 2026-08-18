# Claims Register

Every load-bearing claim the README makes, mapped to the code that enforces it and the
test that verifies it. A claim that cannot fill both columns does not appear in the
README. Counts and links are additionally checked mechanically in CI by
[`scripts/check_claims.py`](../scripts/check_claims.py).

| Claim | Enforcing code | Verifying test |
|---|---|---|
| 7-task DAG: gate → ingest → bronze → silver → build → publish → notify | `airflow/dags/nyc311_pipeline.py:489-498` | `tests/test_pipeline_components.py::test_airflow_dag_contains_expected_task` |
| dbt stage is write-audit-publish: build+test in GOLD_AUDIT, atomic swap into GOLD | `dbt/macros/generate_schema_name.sql` (audit_suffix), `dbt/macros/publish_gold.sql` | `tests/test_pipeline_components.py::test_airflow_dag_uses_write_audit_publish` |
| HttpSensor gates on HTTP 200 + non-empty body | `airflow/dags/nyc311_pipeline.py:262-265` | `tests/test_pipeline_components.py::test_airflow_dag_has_http_sensor_gate` |
| Ingest is rerun-safe: existing partition skips the API call | `databricks/notebooks/01_ingest_raw.py:83-96` | `tests/test_pipeline_components.py::test_ingest_notebook_has_idempotency_check` (structure-level) |
| Silver dedups deterministically on unique_key, newest `_ingest_timestamp` wins | `databricks/notebooks/silver_transformations.py:109-124` | `tests/unit/test_silver_transformations.py::test_deduplication` (behavioral, crafted duplicate) |
| Silver writes via Delta MERGE on unique_key (upsert, retry-safe) | `databricks/notebooks/03_silver.py:404-413` | `tests/test_pipeline_components.py::test_silver_notebook_uses_delta_merge` (structure-level) |
| resolution_days: NULL for open requests, 0 for same-day, negative preserved for quarantine | `databricks/notebooks/silver_transformations.py:86-106` | `tests/unit/test_silver_transformations.py::test_resolution_days_calculation` |
| Replay checksum is row-order independent | `databricks/notebooks/silver_transformations.py:132-162` | `tests/unit/test_silver_transformations.py::test_unique_key_checksum_is_order_independent` |
| Borough variants collapse to 5 canonical names + UNSPECIFIED | `databricks/notebooks/silver_transformations.py:44-83` | `tests/unit/test_silver_transformations.py::test_borough_standardization` |
| fct_service_requests is incremental MERGE on service_request_id, clustered on cast(created_date as date) | `dbt/models/marts/fct_service_requests.sql:1-9` | `tests/test_dbt_architecture.py::test_fct_service_requests_is_incremental`, `::test_fct_service_requests_cluster_key` |
| All fact→dimension joins are LEFT; NULL FKs documented, borough coalesced in the daily rollup | `dbt/models/marts/fct_service_requests.sql`, `dbt/models/marts/fct_daily_volume.sql:36` | manifest-declared tests listed in `dbt/models/marts/marts.yml` |
| is_overdue is three-valued: NULL while open | `dbt/models/marts/fct_service_requests.sql:88-95` | documented decision (README "Design Decisions"); no test — pure CASE expression |
| Calendar semantics immune to Snowflake WEEK_START (ISO day-of-week) | `dbt/models/marts/dim_date.sql:51-70` | none yet — flagged in audit; candidate for the local harness |
| Every model lands in the GOLD schema (no gold_gold) | `dbt/macros/generate_schema_name.sql` | `tests/test_dbt_architecture.py::test_all_models_land_in_gold_schema` |
| LOADER role cannot UPDATE/TRUNCATE Bronze (append-only at the grant layer) | `terraform/modules/snowflake-foundation/main.tf:218-228` | `tests/test_pipeline_components.py::test_terraform_loader_bronze_grants_no_truncate` |
| Terraform validates without cloud credentials | whole `terraform/` tree | `tests/test_pipeline_components.py::test_terraform_validate` (behavioral, subprocess) |
| No hardcoded credentials: secret scopes / env_var() / ARM_* env | `databricks/notebooks/01_ingest_raw.py:57-59`, `dbt/profiles.yml.example`, `terraform/main.tf:8-20` | `tests/test_pipeline_components.py::test_profiles_example_uses_env_vars_for_credentials` |
| Source freshness keys on `_silver_timestamp`, not business dates | `dbt/models/staging/sources.yml:20-23` | `tests/test_dbt_architecture.py::test_source_freshness_uses_silver_timestamp` |
| README counts (tests, ADRs, fact models) and links match the repo | marker system in `README.md` | `scripts/check_claims.py` in CI (`.github/workflows/dbt.yml`) |

## Deleted from the README rather than listed here

Claims removed because no enforcing code exists (see ADR 008 and the audit):

- "updated automatically every morning" / any live-schedule claim — no deployment.
- "syncs to Snowflake via Snowpipe or the Databricks Snowflake connector" — no sync
  code exists; the mechanism is an open decision (ADR 008).
- "zero silent failures" — DQ threshold breaches warn by design and do not fail.
- "runs in under 5 seconds on any machine" — depended on a hardcoded venv path and on
  the unit tier silently skipping.
- "0.02% of records dropped" and other dataset statistics — not reproducible from the repo.
- "cost-scalability analysis" — the document does not exist.
