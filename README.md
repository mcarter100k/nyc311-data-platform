# NYC 311 Data Platform

[![dbt CI](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/dbt.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/dbt.yml)
[![Terraform](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/terraform.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/terraform.yml)

> A **reference implementation** of a data platform for NYC 311 service requests — a ~35M-row
> public dataset ([NYC Open Data, erm2-nwe9](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9)).
> The full Bronze→Silver→Gold flow runs end-to-end on a laptop against DuckDB
> ([local/local_runner.py](local/local_runner.py)); the cloud deployment is specified in
> Terraform, Databricks notebooks, and an Airflow DAG, but deliberately **not provisioned**
> ([ADR 008](docs/adr/008-prototype-scope.md)).

**What is real here, and what is deferred:**

| Real (verifiable in this repo) | Evidence |
|---|---|
| End-to-end local pipeline: ingest → bronze → silver → dbt gold → queries, on DuckDB | [local/local_runner.py](local/local_runner.py), [local/README_LOCAL.md](local/README_LOCAL.md) |
| dbt project parses and its architecture is pytest-verified in CI on every push to main and PR | [.github/workflows/dbt.yml](.github/workflows/dbt.yml) |
| Terraform passes `terraform validate` in CI | [.github/workflows/terraform.yml](.github/workflows/terraform.yml), [tests/test_pipeline_components.py:320](tests/test_pipeline_components.py#L320) |
| Silver transformation logic unit-tested against a local SparkSession | [tests/unit/test_silver_transformations.py](tests/unit/test_silver_transformations.py) |

| Deferred (specified, not provisioned) | Where specified |
|---|---|
| Azure storage, Databricks workspace, Snowflake account | [terraform/](terraform/) — never applied; azure-infra module is a stub |
| Scheduled daily runs | [airflow/dags/nyc311_pipeline.py](airflow/dags/nyc311_pipeline.py) — the DAG is the schedule spec, no Airflow deployment exists |
| Databricks Silver → Snowflake SILVER data movement | requirement stated in [sources.yml](dbt/models/staging/sources.yml); mechanism is an open decision ([ADR 008](docs/adr/008-prototype-scope.md)) |

---

## The Problem This Solves

New York City fields millions of 311 calls every year — potholes, broken streetlights, noise complaints, rat infestations. All of it is public data. None of it is usable in its raw form.

The source dataset is 35 million rows of messy JSON going back to 2010: duplicate records from API pagination, borough names spelled 15 different ways, timestamps that show a case closing before it was opened, and columns that appear and disappear as the city changes its data entry systems over the years.

In that state, you can't answer a basic question like "which borough has the worst response time?" without hours of manual work — and the answer is already stale by the time you have it.

**This platform is designed to solve that.** As specified, it runs every morning, pulls the latest complaints, cleans and validates them through three checkpointed layers, and lands them in a dimensional model any BI tool can query. This repo demonstrates that design at reference scale: the identical layer flow runs locally against DuckDB ([local/local_runner.py](local/local_runner.py)), and the cloud deployment is written down but not stood up ([ADR 008](docs/adr/008-prototype-scope.md)).

---

## What This Demonstrates

This project was built to show what a senior data engineer / principal architect produces — not just working code, but the reasoning behind every decision.

| Skill Area | What You'll Find Here |
|---|---|
| **Data Architecture** | Full medallion lakehouse (Bronze → Silver → Gold) with a clear failure-isolation contract at each layer |
| **Infrastructure as Code** | Terraform provisions 30+ Snowflake objects with least-privilege role grants enforced as code, not documentation |
| **Pipeline Engineering** | Idempotent, paginated API ingestion with Airflow orchestration, HTTP availability gating, and retry-safe Delta MERGE |
| **Dimensional Modeling** | Star schema with <!--claim:fct_models-->3<!--/claim--> fact tables, 3 dimension tables, and a 21-year calendar spine (2010–2030) — built and tested in dbt |
| **Data Quality** | A pytest suite verifying pipeline architecture plus PySpark unit tests on the Silver transforms — no live cloud credentials required ([Test Suite](#test-suite)) |
| **Security** | Zero hardcoded credentials — Databricks secret scopes, Airflow Connections, dbt env_var(), RSA key-pair auth for prod |
| **CI/CD** | GitHub Actions runs `terraform plan` on every PR and validates the dbt project (`dbt parse` + pytest suite) on pushes to main and PRs |

---

## Architecture

![Architecture Diagram](architecture/architecture-diagram.png)

```
┌─────────────────────────────────────────────────────────────────────┐
│  NYC Open Data — Socrata API (data.cityofnewyork.us)                │
│  311 Service Requests · 35M+ rows · updated daily                   │
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

**Infrastructure:** Terraform provisions all Snowflake objects (database, 3 schemas, warehouse, 4 roles, 25+ grants) in a single `terraform apply`.

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
| CI/CD | GitHub Actions | `terraform plan` on PR; `dbt parse` + pytest suite on main pushes and PRs |

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
- 15+ borough name variants are standardized to five canonical forms
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
Provisions the complete Snowflake hierarchy from scratch:

- Four roles with a least-privilege grant matrix: `NYC311_ADMIN`, `NYC311_LOADER` (Bronze write-only), `NYC311_TRANSFORMER` (Bronze read, Silver/Gold write), `NYC311_REPORTER` (Gold read-only)
- `FUTURE TABLES` grants mean any new dbt model automatically inherits the correct permissions — no post-deploy Terraform re-apply required
- All environment differences (dev vs. prod) are handled by a single `environment` variable — no duplicated config
- Remote state in Azure Blob with lease locking for safe team collaboration

**Outcome:** A new environment (dev, staging, prod) is one `terraform apply` away. Role permissions are auditable, diffable, and version-controlled — not scattered across Snowflake worksheets run by hand.

---

<a name="test-suite"></a>
## Test Suite — <!--claim:test_count-->93<!--/claim--> Tests, Zero Live Credentials Required

The suite has three tiers, none of which connects to Snowflake, Databricks, or Azure (the total above is recomputed by `scripts/check_claims.py` in CI — the build fails if this section drifts):

- **Structural tests** assert the compiled dbt manifest, DAG, Terraform, and workflow files. They run in a few seconds.
- **PySpark unit tests** execute the Silver transformation functions against a local SparkSession with crafted inputs. They skip automatically when pyspark is not installed ([tests/unit/conftest.py:31](tests/unit/conftest.py#L31)).
- **Local-gold behavioral tests** build the dbt project twice against a seeded DuckDB database and assert incremental semantics: the `_loaded_at` watermark and its 1-hour lookback boundary, snapshot rename detection, and the SCD2 point-in-time agency join ([tests/local/test_local_gold.py](tests/local/test_local_gold.py)). They skip when dbt-duckdb is not installed.

```bash
./run_tests.sh          # full suite (unit tier skips without pyspark)
./run_tests.sh dbt      # dbt architecture tests only
./run_tests.sh pipeline # pipeline component tests only
```

Tests cover:
- dbt schema resolution (models land in the correct Snowflake schemas)
- Incremental strategy configuration and watermark correctness
- DAG lineage (every model's `ref()` and `source()` dependency chain)
- Source freshness config points to the pipeline timestamp, not the business event date
- Databricks notebooks use secret scopes, paginate correctly, deduplicate, and assert non-zero row counts
- Airflow DAG has the HttpSensor gate, reschedule mode, and all 7 expected tasks
- Terraform HCL passes `terraform validate` — the configuration is deployable
- The LOADER role has no TRUNCATE on Bronze tables (append-only contract enforced in code)
- GitHub Actions workflow has the correct permissions and artifact upload steps
- `profiles.yml.example` uses `env_var()` for all credentials and configures RSA key-pair auth for prod

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
├── airflow/dags/nyc311_pipeline.py    # 7-task orchestration DAG (spec — not deployed)
├── tests/                             # structural + PySpark unit suite (count checked in CI)
├── scripts/check_claims.py            # CI guard: README counts/links vs the repo
├── docs/adr/                          # <!--claim:adr_count-->8<!--/claim--> architecture decision records
├── docs/CLAIMS.md                     # claim → enforcing code → verifying test
├── architecture/                      # Architecture diagram
└── .github/workflows/                 # CI/CD: Terraform plan, dbt parse + pytest, docs deploy
```

---

## Architecture Decision Records

ADRs document the reasoning behind major technology choices — written to be defensible in a principal-level interview.

| ADR | Decision |
|---|---|
| [001](docs/adr/001-warehouse-selection.md) | Snowflake over Databricks SQL — ETL/BI workload isolation, adapter maturity |
| [002](docs/adr/002-transformation-tool.md) | dbt over continued PySpark for the Gold layer |
| [003](docs/adr/003-iac-approach.md) | Terraform as the single IaC surface across three providers |
| [004](docs/adr/004-medallion-vs-elt.md) | Medallion layering over direct ELT — one failure mode per layer |
| [005](docs/adr/005-orchestration-strategy.md) | Airflow, single DAG, write-audit-publish dbt stage |
| [006](docs/adr/006-schema-evolution.md) | Schema version stamp over runtime column detection |
| [007](docs/adr/007-scd-type-2-dim-agency.md) | dbt snapshot (check strategy) for agency SCD Type 2; point-in-time fact join |
| [008](docs/adr/008-prototype-scope.md) | Prototype scope — cloud services specified, not provisioned |

---

## Design Decisions Worth Discussing

These are the decisions most likely to generate substantive conversation in a senior or principal-level interview.

**`is_overdue` is NULL for open requests, not FALSE.**
`fct_service_requests` uses a three-valued flag: TRUE (closed in > 30 days), FALSE (closed in ≤ 30 days), NULL (still open). A boolean FALSE would cause `COUNT(*) FILTER (WHERE NOT is_overdue)` to count open requests as "on time" — silently inflating the resolution rate. NULL forces analysts to explicitly decide whether to include or exclude open requests, which is the correct default for a mixed-status fact table.

**The HttpSensor is a cost gate, not just a health check.**
The sensor validates HTTP 200 and a non-empty JSON body before any Databricks cluster starts. If the Socrata API is up but the daily refresh hasn't completed, the sensor waits. The cost of a five-minute sensor timeout is zero. The cost of a cluster spinning up, pulling an incomplete dataset, and writing a partial Bronze partition is a manual replay job plus incident investigation.

**`FUTURE TABLES` grants eliminate a class of deployment bugs.**
Granting `SELECT ON FUTURE TABLES IN SCHEMA GOLD TO ROLE NYC311_REPORTER` means any table dbt creates — including models added months after the initial Terraform apply — automatically inherits correct permissions. Without this, adding a new mart model creates a silent permissions gap that breaks BI dashboards until someone re-runs `terraform apply`.

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
```

See [local/README_LOCAL.md](local/README_LOCAL.md) for stage-by-stage detail.

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
Data Engineer · Principal Architect
marq.dcarter@gmail.com
[LinkedIn](https://www.linkedin.com/in/marquis-c-45132325b/) · [GitHub](https://github.com/mcarter100k)

---

*The counts, links, and inventory claims in this README are checked mechanically:
[scripts/check_claims.py](scripts/check_claims.py) runs in CI and fails the build when the
README drifts from the repo. The claim-by-claim evidence map is
[docs/CLAIMS.md](docs/CLAIMS.md).*
