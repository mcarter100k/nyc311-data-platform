# Architecture

Deep detail moved out of the README so that document stays skimmable. This is
the *how it is built* companion; the README covers what it does, how it is
operated, and the decisions worth arguing about.

---

## Architecture

![Architecture Diagram](../architecture/architecture-diagram.png)

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
                               │ file load
                               ▼
┌──────────────────────────────────────────────────────┐
│  Bronze layer                     │
│  Raw types only · audit columns · no data loss       │
└──────────────────────────────┬───────────────────────┘
                               │ Delta MERGE on unique_key
                               ▼
┌──────────────────────────────────────────────────────┐
│  Silver layer                     │
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

**Orchestration:** the Airflow DAG ([nyc311_local.py](../airflow/dags/nyc311_local.py)) runs the pipeline as seven tasks with a source gate at the front — nothing starts until the API is confirmed live. It executes for real ([smoke-tested green](../docs/adr/010-scheduled-operation.md)); the scheduled daily run remains GitHub Actions, because a laptop scheduler misses any run fired while the machine is asleep.

**Infrastructure:** Terraform provisions the Snowflake objects (database, 5 schemas — including the GOLD_AUDIT write-audit-publish area and SNAPSHOTS for SCD2 state — warehouse, 4 roles, 35+ grants) in a single `terraform apply`. One apply-time step remains outside Terraform: the OWNERSHIP transfer the schema swap requires (ADR 009).

---

## Stack

| Layer | Tool | Why This Tool |
|---|---|---|
| Infrastructure | Terraform | Declarative drift detection; least-privilege enforced as code |
| Storage | Azure Data Lake Storage Gen2 | Cheap, durable raw archive; native the transform layer integration |
| Processing | the transform layer (pandas) | Auto Loader schema evolution; Delta MERGE for idempotency |
| Warehouse | Snowflake | Auto-suspend compute; best-in-class dbt adapter; strict role isolation |
| Transformation | dbt Core | Version-controlled SQL; DAG lineage; schema test framework as a quality gate |
| Orchestration | Apache Airflow | Code-defined DAGs; the transform layerRunNowOperator; reschedule-mode sensor |
| CI/CD | GitHub Actions | `terraform fmt`+`validate` on PRs and main pushes; `dbt parse` + pytest suite on main pushes and PRs |

---

## Outcomes at Each Layer

### Raw Ingestion — `local_runner.py` stage 1
Pulls 311 records from the Socrata API in 50,000-row pages (the API maximum), writing raw JSON to Azure Data Lake partitioned by date. The notebook checks whether today's partition already exists before calling the API — Airflow retries are free and safe. All credentials come from the transform layer secret scopes.

**Outcome:** A permanent, unmodified audit trail of every complaint ever filed. If a downstream bug corrupts Silver or Gold, the pipeline can replay from this layer without re-calling the API.

---

### Bronze — `local_runner.py` stage 2
Loads raw JSON into a Delta table using the transform layer Auto Loader. Auto Loader checkpoints the inferred schema — when the city adds new columns to the dataset (which has happened multiple times in 15 years), they propagate automatically without a schema migration ticket. Bronze is append-only. No records are ever deleted here.

**Outcome:** A structured, queryable audit layer that absorbs schema drift without breaking. The only transformations are timestamp casts and audit columns — no business logic that could go wrong.

---

### Silver — `local_runner.py` stage 3 + `silver_transformations.py`
Applies the data quality rules that make analysis trustworthy:
- Deduplication on `unique_key` (the city's natural key) removes duplicates created by API pagination overlap
- 24 borough name variants are standardized to five canonical forms, from one shared mapping ([config/borough_variants.csv](../config/borough_variants.csv)) read by the pandas transform, the pandas runner, and both dbt projects
- Records where the closed date precedes the created date are quarantined and logged ([silver_transformations.select_quarantine](../local/silver_transformations.py)) — data entry errors that would corrupt resolution time metrics
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

The `HttpSensor` at the front validates both HTTP 200 and a non-empty response body before any the transform layer cluster starts. The dbt stage follows **write-audit-publish**: `dbt_build` runs a single `dbt build` that resolves the whole DAG in dependency order (the agency SCD Type 2 snapshot runs before `dim_agency` automatically) and routes every model into a `GOLD_AUDIT` schema, testing each model right after it builds. Only when every model and test passes does `dbt_publish` swap the audited schema into `GOLD` with an atomic `ALTER SCHEMA ... SWAP WITH` — BI consumers never see unvalidated data, and a failed run leaves the previous published build serving.

**Outcome:** No wasted the transform layer compute on a broken source, and no window where bad data is live in Gold. A failed build or test halts before publish; production keeps serving the last validated build.

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

---

## Where to go next

- [README](../README.md) — findings, design decisions, operating record
- [docs/adr/](adr/) — the decision records behind each choice
- [docs/SLO.md](SLO.md) — the service level objectives, with executable queries
- [docs/CLAIMS.md](CLAIMS.md) — claim → enforcing code → verifying test
