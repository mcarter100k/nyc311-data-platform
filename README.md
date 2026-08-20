# NYC 311 Data Platform

[![CI](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/ci.yml)
[![Terraform](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/terraform.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/terraform.yml)
[![Daily Live Run](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/daily-run.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/daily-run.yml)

> A **reference implementation** of a data platform for NYC 311 service requests — a ~22M-row
> public dataset covering 2020 to present ([NYC Open Data, erm2-nwe9](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2020-to-Present/erm2-nwe9);
> in Dec 2025 the city split 2010–2019 into a separate historical dataset).
> The full Bronze→Silver→Gold flow runs end-to-end on a laptop against DuckDB
> ([local/local_runner.py](local/local_runner.py)); the cloud deployment is specified in
> Terraform, Databricks notebooks, and an Airflow DAG, but deliberately **not provisioned**
> ([ADR 008](docs/adr/008-prototype-scope.md)).

**What is real here, and what is deferred:**

| Real (verifiable in this repo) | Evidence |
|---|---|
| End-to-end local pipeline: ingest → bronze → silver → dbt gold → queries, on DuckDB | [local/local_runner.py](local/local_runner.py), [local/README_LOCAL.md](local/README_LOCAL.md) |
| Scheduled to run daily against the live API, gated by two [SLOs](docs/SLO.md); the Daily Live Run badge above is the live status | [.github/workflows/daily-run.yml](.github/workflows/daily-run.yml), [ADR 010](docs/adr/010-scheduled-operation.md) |
| Airflow orchestrates the pipeline locally — 7-task DAG, verified end to end with `airflow dags test` (all tasks green). A demonstration, not the scheduler; the daily run above remains GitHub Actions ([ADR 010](docs/adr/010-scheduled-operation.md)) | [airflow/dags/nyc311_local.py](airflow/dags/nyc311_local.py), [scripts/airflow_local.sh](scripts/airflow_local.sh) |
| dbt project parses and its architecture is pytest-verified in CI on every push to main and PR — three parallel required checks: fast-gate, unit-pyspark, behavioral-duckdb ([ADR 011](docs/adr/011-parallel-ci-tiers.md)) | [.github/workflows/ci.yml](.github/workflows/ci.yml) |
| Terraform passes `terraform validate` in CI | [.github/workflows/terraform.yml](.github/workflows/terraform.yml), [tests/test_pipeline_components.py:320](tests/test_pipeline_components.py#L320) |
| Silver transformation logic unit-tested against a local SparkSession | [tests/unit/test_silver_transformations.py](tests/unit/test_silver_transformations.py) |

| Deferred (specified, not provisioned) | Where specified |
|---|---|
| Azure storage, Databricks workspace, Snowflake account | [terraform/](terraform/) — never applied; azure-infra module is a stub |
| Cloud-scheduled runs (Airflow) | [airflow/dags/nyc311_pipeline.py](airflow/dags/nyc311_pipeline.py) — the DAG is the schedule spec, no Airflow deployment exists; the daily GitHub Actions run above operates the *local* pipeline ([ADR 010](docs/adr/010-scheduled-operation.md)) |
| Databricks Silver → Snowflake SILVER data movement | requirement stated in [sources.yml](dbt/models/staging/sources.yml); mechanism is an open decision ([ADR 008](docs/adr/008-prototype-scope.md)) |

---

## The Problem This Solves

New York City fields millions of 311 calls every year — potholes, broken streetlights, noise complaints, rat infestations. All of it is public data. None of it is usable in its raw form.

The source dataset is 22 million rows of messy JSON covering 2020 to the present (the city split the earlier decade into a companion historical dataset in December 2025): duplicate records from API pagination, borough names spelled 15 different ways, timestamps that show a case closing before it was opened, and columns that appear and disappear as the city changes its data entry systems over the years.

In that state, you can't answer a basic question like "which borough has the worst response time?" without hours of manual work — and the answer is already stale by the time you have it.

**This platform is designed to solve that.** As specified, it runs every morning, pulls the latest complaints, cleans and validates them through three checkpointed layers, and lands them in a dimensional model any BI tool can query. This repo demonstrates that design at reference scale: the identical layer flow runs locally against DuckDB ([local/local_runner.py](local/local_runner.py)), and the cloud deployment is written down but not stood up ([ADR 008](docs/adr/008-prototype-scope.md)).

---

## What This Demonstrates

This project documents not just working code, but the reasoning behind every architectural decision — the trade-offs considered, the option chosen, and what it costs.

| Skill Area | What You'll Find Here |
|---|---|
| **Data Architecture** | Full medallion lakehouse (Bronze → Silver → Gold) with a clear failure-isolation contract at each layer |
| **Infrastructure as Code** | Terraform provisions 45+ Snowflake objects with least-privilege role grants enforced as code, not documentation |
| **Pipeline Engineering** | Idempotent, paginated API ingestion with Airflow orchestration, HTTP availability gating, and retry-safe Delta MERGE |
| **Dimensional Modeling** | Star schema with <!--claim:fct_models-->3<!--/claim--> fact tables, 3 dimension tables, and a 21-year calendar spine (2010–2030) — built and tested in dbt |
| **Data Quality** | A pytest suite verifying pipeline architecture plus PySpark unit tests on the Silver transforms — no live cloud credentials required ([Test Suite](#test-suite)) |
| **Security** | Zero hardcoded credentials — Databricks secret scopes, Airflow Connections, dbt env_var(), RSA key-pair auth for prod |
| **CI/CD** | GitHub Actions runs `terraform fmt` + `validate` on every PR and push to main, and validates the dbt project (`dbt parse` + pytest suite) on pushes to main and PRs. (`terraform plan` needs live credentials and state, so it is a local/apply-time step, not CI — see ADR 003) |

---

## Architecture

![Architecture Diagram](architecture/architecture-diagram.png)

```
┌─────────────────────────────────────────────────────────────────────┐
│  NYC Open Data — Socrata API (data.cityofnewyork.us)                │
│  311 Service Requests · 22M+ rows (2020–present) · daily            │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ paginated JSON / REST
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Azure Data Lake Storage Gen2 — Raw Zone                            │
│  Partitioned by ingest_date — immutable, append-only archive        │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ Auto Loader (cloudFiles)
                               ▼
┌──────────────────────────────────────────────────────┐
│  Databricks — Bronze Delta Table                     │
│  Raw types only · audit columns · no data loss       │
└──────────────────────────────┬───────────────────────┘
                               │ Delta MERGE on unique_key
                               ▼
┌──────────────────────────────────────────────────────┐
│  Databricks — Silver Delta Table                     │
│  Deduplication · borough standardization · quality   │
│  filters · resolution_days · _silver_timestamp       │
│  (Silver → Snowflake sync: deferred — ADR 008)       │
└──────────────────────────────┬───────────────────────┘
                               │ dbt incremental MERGE
                               ▼
┌──────────────────────────────────────────────────────┐
│  Snowflake — GOLD Schema                             │
│  dim_agency · dim_date · dim_location                │
│  fct_service_requests · fct_daily_volume             │
│  fct_data_quality · tested before publish (WAP)      │
└──────────────────────────────┬───────────────────────┘
                               │
                               ▼
                    BI / Reporting Tools
```

**Orchestration:** the Apache Airflow DAG ([nyc311_pipeline.py](airflow/dags/nyc311_pipeline.py)) specifies a daily 06:00 UTC run with an HttpSensor gate — nothing starts until the source API is confirmed live. The DAG is a deployment spec; no Airflow instance is running it ([ADR 008](docs/adr/008-prototype-scope.md)).

**Infrastructure:** Terraform provisions the Snowflake objects (database, 5 schemas — including the GOLD_AUDIT write-audit-publish area and SNAPSHOTS for SCD2 state — warehouse, 4 roles, 35+ grants) in a single `terraform apply`. One apply-time step remains outside Terraform: the OWNERSHIP transfer the schema swap requires (ADR 009).

---

## Stack

| Layer | Tool | Why This Tool |
|---|---|---|
| Infrastructure | Terraform | Declarative drift detection; least-privilege enforced as code |
| Storage | Azure Data Lake Storage Gen2 | Cheap, durable raw archive; native Databricks integration |
| Processing | Databricks (PySpark) | Auto Loader schema evolution; Delta MERGE for idempotency |
| Warehouse | Snowflake | Auto-suspend compute; best-in-class dbt adapter; strict role isolation |
| Transformation | dbt Core | Version-controlled SQL; DAG lineage; schema test framework as a quality gate |
| Orchestration | Apache Airflow | Code-defined DAGs; DatabricksRunNowOperator; reschedule-mode sensor |
| CI/CD | GitHub Actions | `terraform fmt`+`validate` on PRs and main pushes; `dbt parse` + pytest suite on main pushes and PRs |

---

## Outcomes at Each Layer

### Raw Ingestion — `01_ingest_raw.py`
Pulls 311 records from the Socrata API in 50,000-row pages (the API maximum), writing raw JSON to Azure Data Lake partitioned by date. The notebook checks whether today's partition already exists before calling the API — Airflow retries are free and safe. All credentials come from Databricks secret scopes.

**Outcome:** A permanent, unmodified audit trail of every complaint ever filed. If a downstream bug corrupts Silver or Gold, the pipeline can replay from this layer without re-calling the API.

---

### Bronze — `02_bronze.py`
Loads raw JSON into a Delta table using Databricks Auto Loader. Auto Loader checkpoints the inferred schema — when the city adds new columns to the dataset (which has happened multiple times in 15 years), they propagate automatically without a schema migration ticket. Bronze is append-only. No records are ever deleted here.

**Outcome:** A structured, queryable audit layer that absorbs schema drift without breaking. The only transformations are timestamp casts and audit columns — no business logic that could go wrong.

---

### Silver — `03_silver.py`
Applies the data quality rules that make analysis trustworthy:
- Deduplication on `unique_key` (the city's natural key) removes duplicates created by API pagination overlap
- 24 borough name variants are standardized to five canonical forms, from one shared mapping ([config/borough_variants.csv](config/borough_variants.csv)) read by the PySpark transform, the pandas runner, and both dbt projects
- Records where the closed date precedes the created date are quarantined and logged ([03_silver.py:291](databricks/notebooks/03_silver.py#L291)) — data entry errors that would corrupt resolution time metrics
- `resolution_days` is calculated as a null-safe value: null for open requests, never zero

Silver writes using Delta `MERGE` on `unique_key` — re-running the notebook for any date updates existing records and inserts new ones with no duplicates.

**Outcome:** A clean, deduplicated, typed dataset where `resolution_days` is always meaningful and borough names always join correctly to the dimensional model.

---

### Gold — dbt models
dbt builds the full star schema from Silver:

- `stg_service_requests` — renames and casts columns, generates surrogate keys, maps `_silver_timestamp` as the incremental watermark
- `int_service_requests_cleaned` — applies business classification (complaint categories, borough normalization)
- `fct_service_requests` — the core fact table; incremental MERGE on `unique_key` with a 1-hour lookback buffer; clustered on `cast(created_date as date)` for Snowflake scan efficiency
- `fct_daily_volume` — pre-aggregated complaint counts by day, borough, and category for fast dashboard queries
- `dim_date` — 21-year calendar spine (2010–2030) with US federal holiday flags
- `dim_agency` — clean agency dimension with full names and abbreviations
- `dim_location` — geographic dimension: borough, community board, ZIP code (point coordinates stay on the fact as `latitude`/`longitude`)

**Outcome:** A dimensional model a BI analyst can connect to immediately. Every join is LEFT JOIN (open requests with no closed date don't silently disappear from the fact table). Every key is tested for uniqueness and non-null values.

---

### Orchestration — Airflow DAG
Seven tasks in a linear dependency chain:

```
check_api_availability → ingest_raw → load_bronze → load_silver → dbt_build → dbt_publish → notify_success
```

The `HttpSensor` at the front validates both HTTP 200 and a non-empty response body before any Databricks cluster starts. The dbt stage follows **write-audit-publish**: `dbt_build` runs a single `dbt build` that resolves the whole DAG in dependency order (the agency SCD Type 2 snapshot runs before `dim_agency` automatically) and routes every model into a `GOLD_AUDIT` schema, testing each model right after it builds. Only when every model and test passes does `dbt_publish` swap the audited schema into `GOLD` with an atomic `ALTER SCHEMA ... SWAP WITH` — BI consumers never see unvalidated data, and a failed run leaves the previous published build serving.

**Outcome:** No wasted Databricks compute on a broken source, and no window where bad data is live in Gold. A failed build or test halts before publish; production keeps serving the last validated build.

---

### Infrastructure — Terraform
Provisions the Snowflake hierarchy from scratch (all five schemas — BRONZE, SILVER, GOLD, GOLD_AUDIT, SNAPSHOTS; the swap's OWNERSHIP transfer stays an apply-time step, ADR 009):

- Four roles with a least-privilege grant matrix: `NYC311_ADMIN`, `NYC311_LOADER` (Bronze write-only), `NYC311_TRANSFORMER` (Bronze read, Silver/Gold/audit/snapshots write), `NYC311_REPORTER` (Gold read-only, symmetric on GOLD_AUDIT for the publish swap)
- `FUTURE TABLES` grants mean any new dbt model automatically inherits the correct permissions — no post-deploy Terraform re-apply required
- All environment differences (dev vs. prod) are handled by a single `environment` variable — no duplicated config
- Remote state in Azure Blob with lease locking for safe team collaboration

**Outcome:** A new environment (dev, staging, prod) is one `terraform apply` away. Role permissions are auditable, diffable, and version-controlled — not scattered across Snowflake worksheets run by hand.

---

<a name="test-suite"></a>
## Operating This Platform

This platform is operated, not just built. It runs daily against a live API, it has written service level objectives, it has had a real incident, and the incident produced a control.

**Service level objectives** ([docs/SLO.md](docs/SLO.md), evaluated by [scripts/check_slos.py](scripts/check_slos.py) after every scheduled build):

| SLO | Measures | Threshold |
|---|---|---|
| **SLO-1 freshness** | age of the newest `_loaded_at` in `gold.fct_service_requests` | < 26 hours — one daily cycle plus 2h grace. Measures *our* pipeline's liveness, not source staleness |
| **SLO-2 completeness** | rows we loaded vs rows the city actually **published** for yesterday | ≥ 98%. The source's own count is captured at fetch time; the 2% absorbs documented quarantine and dedup removals |

The executable queries live in [scripts/slo/](scripts/slo/); CI fails if `docs/SLO.md` and those files drift apart.

**Breach automation.** A failed run or SLO breach files a `daily-run-breach` GitHub issue with the measured numbers and run URL; a persisting breach comments on the open issue instead of duplicating it ([daily-run.yml](.github/workflows/daily-run.yml)). An issue beats an email: it is a tracked, assignable work item with history that a postmortem can link to.

**A real incident.** On 2026-08-18 the city's publish process left Aug 17 ~96% incomplete, then published nothing for 21+ hours. Every pipeline stage ran green; only the source-facing check saw it, on the tier's first scheduled day. SLO-2 detected it and auto-filed [issue #7](https://github.com/mcarter100k/nyc311-data-platform/issues/7). The control that followed: SLO-2 was **redefined as a source reconciliation** — it now asks whether we loaded everything the city published, so an upstream outage no longer reddens our reliability signal, while a separate non-gating [upstream-stall check](scripts/check_upstream_stall.py) keeps the outage visible. [Full postmortem](docs/postmortems/2026-08-18-upstream-publish-stall.md).

**A self-audit finding.** The ingestion watermark keyed on `created_date`, fetching each record exactly once — on the day it was filed. But 311 requests mutate after creation (status flips to Closed days later), so every downstream update path was unreachable and resolution metrics would only ever have counted same-day closures. Found by systematic self-audit, not by a failure. The watermark now keys on `:updated_at`, guarded by two tests: [one asserting the predicate](tests/test_ingest_config.py), [one proving an update reaches Gold](tests/local/test_local_gold.py).

---

## Test Suite — <!--claim:test_count-->120<!--/claim--> pytest tests, Zero Live Credentials Required

Two separate populations, deliberately not summed: **120 pytest tests** that run without any cloud account, and **100 dbt data tests** that run against the warehouse during `dbt build`. The pytest count is recomputed by [scripts/check_claims.py](scripts/check_claims.py) in CI — the build fails if this section drifts from the repo.

The pytest suite has three tiers, and they verify genuinely different things:

**Structural — 98 tests.** Assert *configuration correctness* against the compiled dbt manifest, the DAG, Terraform, and workflow files: schema resolution, incremental strategy and unique key, DAG lineage from `ref()`/`source()`, source freshness pointing at the pipeline timestamp rather than the business date, Terraform validity, workflow permissions, and the LOADER-has-no-TRUNCATE append-only contract.

*What they catch:* config drift and silent contract violations, in CI, in seconds — a model that would land in the wrong schema, a watermark keyed on the wrong column, a role that quietly gained write access. *What they do not catch:* whether a transformation produces correct output. A model can be perfectly configured and still compute the wrong number; that is the next tier's job.

**Unit — 7 tests.** Execute the Silver transformation functions against a local SparkSession with crafted inputs and assert exact outputs ([tests/unit/](tests/unit/)). Real logic, real Spark, no cluster. This tier is why the transformation code lives in `silver_transformations.py` rather than inside the notebook — notebooks are not unit-testable.

**Behavioral — 15 tests.** Build the real dbt project twice against a seeded DuckDB database and assert on *output rows*, not config ([tests/local/](tests/local/)): the `_loaded_at` watermark and its 1-hour lookback boundary, snapshot rename detection, the SCD2 point-in-time agency join, update propagation through the merge, and the reconciliation delete that keeps incremental and `--full-refresh` identical.

**dbt data tests — 100** (96 generic + 4 singular). Uniqueness, not-null, accepted-values, referential integrity, and Gold-integrity assertions, run by `dbt build` in dependency order — each model's tests execute immediately after it builds, so a failure stops downstream models. These run against the warehouse, not on a laptop; they are the population the local suite cannot replace.

```bash
./run_tests.sh          # full pytest suite (tiers skip cleanly without pyspark / dbt-duckdb)
./run_tests.sh dbt      # dbt architecture tests only
./run_tests.sh pipeline # pipeline component tests only
```

---

## Repository Structure

```
nyc311-data-platform/
├── terraform/                          # All cloud infrastructure as code
│   ├── main.tf                         # Provider pins, module wiring
│   ├── backend.tf                      # Azure Blob remote state
│   ├── outputs.tf                      # Exports for CI/CD consumption
│   └── modules/
│       ├── snowflake-foundation/       # Database, schemas, warehouse, roles, grants
│       └── azure-infra/               # ADLS Gen2 + Databricks workspace (stub — ADR 008)
│
├── databricks/notebooks/
│   ├── 01_ingest_raw.py               # Paginated Socrata API → ADLS raw zone
│   ├── 02_bronze.py                   # Auto Loader → Bronze Delta
│   └── 03_silver.py                   # Clean, dedup, MERGE → Silver
│
├── dbt/
│   ├── models/staging/                # Rename, cast, surrogate key generation
│   ├── models/intermediate/           # Business rules, complaint classification
│   ├── models/marts/                  # dim_* and fct_* tables
│   ├── macros/                        # generate_schema_name, generate_date_spine
│   └── tests/                         # Singular data quality test
│
├── airflow/dags/
│   ├── nyc311_pipeline.py            # 7-task cloud DAG (spec — not deployed)
│   └── nyc311_local.py               # 7-task DAG that runs the local pipeline
├── config/borough_variants.csv        # shared borough mapping (dbt seed + Python)
├── tests/                             # 120 pytest tests, three tiers (count checked in CI)
├── scripts/check_claims.py            # CI guard: README counts/links vs the repo
├── docs/adr/                          # <!--claim:adr_count-->11<!--/claim--> architecture decision records
├── docs/CLAIMS.md                     # claim → enforcing code → verifying test
├── architecture/                      # Architecture diagram
└── .github/workflows/                 # CI/CD: Terraform plan, dbt parse + pytest, docs deploy
```

---

## Architecture Decision Records

ADRs document the reasoning behind major technology choices: the alternatives weighed, the decision, and the consequences accepted.

| ADR | Decision | Outcome |
|---|---|---|
| [001](docs/adr/001-warehouse-selection.md) | Snowflake over Databricks SQL for the serving layer | ETL and BI workloads isolated on separate warehouses; dbt adapter maturity |
| [002](docs/adr/002-transformation-tool.md) | dbt over continued PySpark for the Gold layer | Gold logic becomes testable SQL with lineage; Spark stays for Bronze/Silver |
| [003](docs/adr/003-iac-approach.md) | Terraform as the single IaC surface across three providers | One state file, one plan; drift detection is an apply-time step, not CI |
| [004](docs/adr/004-medallion-vs-elt.md) | Medallion layering over direct ELT | One failure mode per layer, so a break is isolated to a known stage |
| [005](docs/adr/005-orchestration-strategy.md) | Airflow, single DAG, write-audit-publish dbt stage | Sensor gate before paid compute; Gold never serves unvalidated data |
| [006](docs/adr/006-schema-evolution.md) | Schema version stamp over runtime column detection | Each fact row records the contract that built it; additive changes need no bump |
| [007](docs/adr/007-scd-type-2-dim-agency.md) | dbt snapshot (check strategy) for agency SCD Type 2 | Point-in-time fact join; a 2021 request keeps its 2021 agency name |
| [008](docs/adr/008-prototype-scope.md) | Cloud services specified, not provisioned | Every cloud claim in this repo is scoped as spec, never as running |
| [009](docs/adr/009-publish-grants-under-schema-swap.md) | Symmetric grants on GOLD and GOLD_AUDIT | The publish swap keeps REPORTER access; grants follow renamed objects |
| [010](docs/adr/010-scheduled-operation.md) | Scheduled daily operation with written SLOs | The pipeline runs live daily; a breach files a tracked issue |
| [011](docs/adr/011-parallel-ci-tiers.md) | Three parallel required CI checks, not one sequential job | A red check names its failure class; wall time is the slowest tier |

---

## Design Decisions Worth Discussing

These are the decisions where the trade-off was genuinely close, and where the reasoning matters more than the result.

**`is_overdue` is NULL for open requests, not FALSE.**
`fct_service_requests` uses a three-valued flag: TRUE (closed in > 30 days), FALSE (closed in ≤ 30 days), NULL (still open). A boolean FALSE would cause `COUNT(*) FILTER (WHERE NOT is_overdue)` to count open requests as "on time" — silently inflating the resolution rate. NULL forces analysts to explicitly decide whether to include or exclude open requests, which is the correct default for a mixed-status fact table.

**The HttpSensor is a cost gate, not just a health check.**
The sensor validates HTTP 200 and a non-empty JSON body before any Databricks cluster starts. If the Socrata API is up but the daily refresh hasn't completed, the sensor waits. The cost of a five-minute sensor timeout is zero. The cost of a cluster spinning up, pulling an incomplete dataset, and writing a partial Bronze partition is a manual replay job plus incident investigation.

**`FUTURE TABLES` grants interact with schema-swap publishing — and the interaction has to be designed, not assumed.**
`SELECT ON FUTURE TABLES` lets any table dbt creates inherit reporter permissions without a Terraform re-apply ([main.tf:471-481](terraform/modules/snowflake-foundation/main.tf#L471-L481)). But Snowflake grants attach to the schema *object*, and the write-audit-publish swap renames objects — so grants defined only on GOLD stop covering it after the first publish. [ADR 009](docs/adr/009-publish-grants-under-schema-swap.md) resolves this: the grant matrix is specified symmetrically on both GOLD and GOLD_AUDIT, keeping the single atomic swap (the alternative — per-table view swaps — was rejected because it reintroduces the cross-table inconsistency window WAP exists to eliminate).

**All dimension joins in `fct_service_requests` are LEFT JOINs.**
Not every 311 complaint has a recognized agency code or a geocodable address. INNER JOINs against imperfect dimension coverage silently drop fact rows — a `COUNT(*)` on the fact table then disagrees with Silver, and that discrepancy surfaces at 11pm before a board presentation. NULL foreign keys in the fact table are visible and fixable. Silently dropped rows are not.

**Three layers are a debugging protocol, not an architecture pattern.**
The most important property of Bronze/Silver/Gold is not the materialization or the tool — it is that each layer has exactly one failure mode. When a data quality issue appears in a Gold mart, you query Silver. If the issue is there, you query Bronze. If not, the bug is in a dbt model. Without three checkpointed layers, you are debugging a black box. With them, you bisect any pipeline bug in two queries.

---

## How to Run / Deploy

### 0. Run locally — no cloud accounts

```bash
cd local
pip install -r requirements.txt
python local_runner.py            # ingest → bronze → silver → dbt gold → sample queries
python reconcile.py               # verify the output against the source, record by record
```

See [local/README_LOCAL.md](local/README_LOCAL.md) for stage-by-stage detail.
[local/reconcile.py](local/reconcile.py) is the source-to-target check: layer conservation,
independent recomputation from the raw JSON, and a live spot-check against the city's API —
it proves the Gold layer agrees with reality, not just with itself.

### 1. Provision infrastructure (deferred — requires your own Azure + Snowflake accounts; see ADR 008)

```bash
export SNOWFLAKE_ACCOUNT="your-org.your-account"
export SNOWFLAKE_USER="terraform_user"
export SNOWFLAKE_PASSWORD="..."
export ARM_ACCESS_KEY="..."   # Azure storage account key for remote state

cd terraform
terraform init -backend-config="key=nyc311/dev/terraform.tfstate"
terraform plan -out=tfplan
terraform apply tfplan
```

### 2. Install dbt and run models

```bash
pip install dbt-snowflake
cd dbt && dbt deps

cp profiles.yml.example ~/.dbt/profiles.yml
# Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD env vars

# dbt build resolves the whole DAG — including the agency SCD2 snapshot that
# dim_agency depends on — and runs each model's tests right after it builds.
# (A plain `dbt run` would fail: it never builds the snapshot.)
dbt build --target dev
dbt docs generate && dbt docs serve   # lineage graph at localhost:8080
```

### 3. Run the test suite (no cloud credentials needed)

```bash
pip install pytest pyyaml
cd dbt && dbt parse --profiles-dir . --project-dir . --target ci
cd .. && ./run_tests.sh
```

---

## Contact

**Marquis Carter**
Data Engineer
marq.dcarter@gmail.com
[LinkedIn](https://www.linkedin.com/in/marquis-c-45132325b/) · [GitHub](https://github.com/mcarter100k)

---

*The counts, links, and inventory claims in this README are checked mechanically:
[scripts/check_claims.py](scripts/check_claims.py) runs in CI and fails the build when the
README drifts from the repo. The claim-by-claim evidence map is
[docs/CLAIMS.md](docs/CLAIMS.md).*
