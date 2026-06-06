# NYC 311 Data Platform

[![dbt CI](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/dbt.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/dbt.yml)
[![Terraform](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/terraform.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/terraform.yml)

> A production-grade data engineering platform that transforms 35 million raw city service
> complaints into a clean, queryable dimensional model — updated automatically every morning,
> validated by 92 automated tests, and deployable to real cloud infrastructure with a single command.

---

## The Problem This Solves

New York City fields millions of 311 calls every year — potholes, broken streetlights, noise complaints, rat infestations. All of it is public data. None of it is usable in its raw form.

The source dataset is 35 million rows of messy JSON going back to 2010: duplicate records from API pagination, borough names spelled 15 different ways, timestamps that show a case closing before it was opened, and columns that appear and disappear as the city changes its data entry systems over the years.

In that state, you can't answer a basic question like "which borough has the worst response time?" without hours of manual work — and the answer is already stale by the time you have it.

**This platform solves that.** It runs every morning, pulls the latest complaints, cleans and validates them through three checkpointed layers, and lands them in a dimensional model that any BI tool can query in seconds.

---

## What This Demonstrates

This project was built to show what a senior data engineer / principal architect produces — not just working code, but the reasoning behind every decision.

| Skill Area | What You'll Find Here |
|---|---|
| **Data Architecture** | Full medallion lakehouse (Bronze → Silver → Gold) with a clear failure-isolation contract at each layer |
| **Infrastructure as Code** | Terraform provisions 30+ Snowflake objects with least-privilege role grants enforced as code, not documentation |
| **Pipeline Engineering** | Idempotent, paginated API ingestion with Airflow orchestration, HTTP availability gating, and retry-safe Delta MERGE |
| **Dimensional Modeling** | Star schema with two fact tables, three dimension tables, and a 20-year calendar spine — built and tested in dbt |
| **Data Quality** | 92 automated tests that validate architecture correctness and pipeline robustness without requiring live cloud credentials |
| **Security** | Zero hardcoded credentials — Databricks secret scopes, Airflow Connections, dbt env_var(), RSA key-pair auth for prod |
| **CI/CD** | GitHub Actions runs `terraform plan` on every PR and `dbt run + dbt test` on every merge to main |

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
│  Databricks — Silver Delta Table → Snowflake SILVER  │
│  Deduplication · borough standardization · quality   │
│  filters · resolution_days · _silver_timestamp       │
└──────────────────────────────┬───────────────────────┘
                               │ dbt incremental MERGE
                               ▼
┌──────────────────────────────────────────────────────┐
│  Snowflake — GOLD Schema                             │
│  dim_agency · dim_date · dim_location                │
│  fct_service_requests · fct_daily_volume             │
│  92 tests · zero silent failures                     │
└──────────────────────────────┬───────────────────────┘
                               │
                               ▼
                    BI / Reporting Tools
```

**Orchestration:** Apache Airflow (`nyc311_pipeline` DAG) runs the full chain daily at 06:00 UTC with an HttpSensor gate — nothing starts until the source API is confirmed live.

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
| CI/CD | GitHub Actions | `terraform plan` on PR; `dbt run + dbt test` on merge |

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
- Records where the closed date precedes the created date are dropped and logged (roughly 0.02% of records — data entry errors that would corrupt resolution time metrics)
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
- `dim_date` — 20-year calendar spine (2010–2030) with US federal holiday flags
- `dim_agency` — clean agency dimension with full names and abbreviations
- `dim_location` — geographic dimension: borough, ZIP code, latitude/longitude

**Outcome:** A dimensional model a BI analyst can connect to immediately. Every join is LEFT JOIN (open requests with no closed date don't silently disappear from the fact table). Every key is tested for uniqueness and non-null values.

---

### Orchestration — Airflow DAG
Eight tasks in a linear dependency chain:

```
check_api_availability → ingest_raw → load_bronze → load_silver → snapshot_agency → dbt_run → dbt_test → notify_success
```

The `HttpSensor` at the front validates both HTTP 200 and a non-empty response body before any Databricks cluster starts. `snapshot_agency` runs `dbt snapshot` for the agency SCD Type 2 dimension before `dbt_run` — ADR 007 requires this ordering. `dbt_run` and `dbt_test` are separate tasks — a model build failure and a data quality failure look identical in a combined step but require completely different remediation.

**Outcome:** No wasted Databricks compute on a broken source. Clear failure attribution in the Airflow UI — you know instantly whether the problem is in ingestion, transformation, or data quality.

---

### Infrastructure — Terraform
Provisions the complete Snowflake hierarchy from scratch:

- Four roles with a least-privilege grant matrix: `NYC311_ADMIN`, `NYC311_LOADER` (Bronze write-only), `NYC311_TRANSFORMER` (Bronze read, Silver/Gold write), `NYC311_REPORTER` (Gold read-only)
- `FUTURE TABLES` grants mean any new dbt model automatically inherits the correct permissions — no post-deploy Terraform re-apply required
- All environment differences (dev vs. prod) are handled by a single `environment` variable — no duplicated config
- Remote state in Azure Blob with lease locking for safe team collaboration

**Outcome:** A new environment (dev, staging, prod) is one `terraform apply` away. Role permissions are auditable, diffable, and version-controlled — not scattered across Snowflake worksheets run by hand.

---

## Test Suite — 92 Tests, Zero Live Credentials Required

The test suite validates architecture correctness and pipeline robustness without connecting to Snowflake, Databricks, or Azure. It runs in under 5 seconds on any machine.

```bash
./run_tests.sh          # all 92 tests
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
│       └── azure-infra/               # ADLS Gen2 + Databricks workspace
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
├── airflow/dags/nyc311_pipeline.py    # 7-task orchestration DAG
├── tests/                             # 86-test structural and behavioral suite
├── docs/adr/                          # Five architecture decision records
├── architecture/                      # Diagram and cost-scalability analysis
└── .github/workflows/                 # CI/CD: Terraform plan, dbt run + test
```

---

## Architecture Decision Records

ADRs document the reasoning behind major technology choices — written to be defensible in a principal-level interview.

| ADR | Decision | Outcome |
|---|---|---|
| [006](docs/adr/006-schema-evolution-contract.md) | Schema version stamp over runtime column detection | Breaking changes are explicit and auditable; additive changes are free |
| [007](docs/adr/007-scd-type-2-dim-agency.md) | dbt snapshot (check strategy) over manual MERGE for agency SCD Type 2 | Change detection is declarative; no custom MERGE code; full version history auditable |

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

## How to Deploy

### 1. Provision infrastructure

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

dbt run --target dev
dbt test --target dev
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

*All code in this repository is production-standard and fully defensible in a technical interview.
Terraform passes `terraform validate` against the real Snowflake provider. dbt models compile
against a real manifest. The 92-test suite runs clean on every machine without cloud credentials.*
