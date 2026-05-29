# NYC 311 Data Platform

A production-grade data engineering portfolio project demonstrating Principal Architect and
Senior Data Engineer competencies across the full modern data stack: infrastructure-as-code
with Terraform, medallion lakehouse ingestion with Databricks PySpark, dimensional modeling
with dbt, orchestration with Apache Airflow, and cloud data warehousing with Snowflake —
built against the NYC 311 Service Request dataset (35M+ records, 2010–present) as a
domain-rich, publicly available source that exercises real-world data quality problems
including borough name variants, negative resolution times, and schema drift over 15 years.

---

## Architecture

![Architecture Diagram](architecture/architecture-diagram.png)

```
┌─────────────────────────────────────────────────────────────────────┐
│  NYC Open Data — Socrata API (data.cityofnewyork.us)                │
│  311 Service Requests · 35M rows · updated daily                    │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ JSON / REST
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Azure Data Lake Storage Gen2 — Raw Zone                            │
│  Partitioned: abfss://raw/.../ingest_date=YYYY-MM-DD/               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ Auto Loader (cloudFiles)
                               ▼
┌──────────────────────────────────────────────────────┐
│  Databricks — Bronze Delta Table                     │
│  Structural types only · audit columns · append-only │
└──────────────────────────────┬───────────────────────┘
                               │ PySpark MERGE
                               ▼
┌──────────────────────────────────────────────────────┐
│  Databricks — Silver Delta Table → Snowflake SILVER  │
│  Dedup · borough std · derived cols · quality filter │
└──────────────────────────────┬───────────────────────┘
                               │ dbt (Snowflake adapter)
                               ▼
┌──────────────────────────────────────────────────────┐
│  Snowflake — GOLD Schema                             │
│  dim_agency · dim_date · dim_location                │
│  fct_service_requests · fct_daily_volume             │
│  59 schema tests · 1 singular test                   │
└──────────────────────────────┬───────────────────────┘
                               │
                               ▼
                    BI / Reporting Tools
```

**Orchestration:** Apache Airflow (`nyc311_pipeline` DAG) sequences all layers daily at 06:00 UTC.  
**Infrastructure:** Terraform provisions all Snowflake objects (database, schemas, warehouse, 4 roles, 25+ grants) and Azure resources.

---

## Stack

| Layer           | Tool                          | Purpose                                                  |
|-----------------|-------------------------------|----------------------------------------------------------|
| Infrastructure  | Terraform                     | Declarative provisioning of Snowflake and Azure objects  |
| Storage         | Azure Data Lake Storage Gen2  | Raw and Bronze/Silver Delta partitions                   |
| Processing      | Databricks (PySpark)          | Bronze ingestion, Silver cleaning and deduplication      |
| Warehouse       | Snowflake                     | Gold dimensional model, BI serving layer                 |
| Transformation  | dbt Core (Snowflake adapter)  | Staging → intermediate → marts with tests and docs       |
| Orchestration   | Apache Airflow                | Daily DAG with HttpSensor gate, retry logic, SLA alerts  |
| CI/CD           | GitHub Actions                | `terraform plan` on PR, `dbt run && dbt test` on merge  |

---

## Repo Structure

```
nyc311-data-platform/
├── terraform/                  # IaC — Snowflake objects and Azure resources
│   ├── main.tf                 # Root module: provider pins, module wiring
│   ├── variables.tf            # All root variables; sensitive vars point to env vars
│   ├── outputs.tf              # Exports database/warehouse/role names for CI/CD
│   ├── backend.tf              # Azure Blob remote state with bootstrap instructions
│   └── modules/
│       ├── snowflake-foundation/   # Database, 3 schemas, warehouse, 4 roles, 25+ grants
│       └── azure-infra/            # ADLS Gen2 account, Databricks workspace (stub)
│
├── databricks/
│   ├── notebooks/
│   │   ├── 01_ingest_raw.py    # Socrata API pagination → ADLS raw zone
│   │   ├── 02_bronze.py        # Auto Loader → Bronze Delta + audit columns
│   │   └── 03_silver.py        # Dedup, clean, MERGE → Silver Delta + Snowflake sync
│   └── jobs/pipeline_config.json   # Databricks Jobs API 2.1 job definition
│
├── dbt/
│   ├── dbt_project.yml         # Project config: materializations, tags, vars
│   ├── profiles.yml.example    # Snowflake connection template (env-var based)
│   ├── packages.yml            # dbt-utils dependency
│   ├── macros/
│   │   └── generate_date_spine.sql   # Wraps dbt_utils.date_spine for dim_date
│   ├── models/
│   │   ├── staging/            # Rename, cast, surrogate key — no business logic
│   │   ├── intermediate/       # Borough std, resolution_days, complaint categories
│   │   └── marts/              # dim_agency, dim_date, dim_location, fct_* tables
│   └── tests/
│       └── assert_resolution_days_nonnegative.sql   # Singular data quality gate
│
├── airflow/dags/
│   └── nyc311_pipeline.py      # 7-task DAG: HttpSensor → 3× Databricks → dbt → notify
│
├── docs/adr/                   # Five full Architecture Decision Records
├── architecture/               # Diagram and cost-scalability analysis
└── .github/workflows/          # terraform.yml and dbt.yml CI pipelines
```

---

## Layer-by-Layer Design

### Ingestion (`01_ingest_raw.py`)
Pulls NYC 311 records from the Socrata API using offset-based pagination (50,000 rows per request), writing raw JSON to ADLS Gen2 partitioned by `ingest_date`. The notebook is idempotent — it checks for an existing partition before calling the API, making Airflow retries safe at zero cost. Credentials come exclusively from Databricks secret scopes; no values are hardcoded. The incremental filter (`$where=created_date >= run_date`) is the primary production path; full load is supported for bootstrap and backfill.

### Bronze (`02_bronze.py`)
Registers raw JSON as a partitioned Delta table using Databricks Auto Loader (`cloudFiles` format). Auto Loader checkpoints the inferred schema — when the Socrata API adds new columns, they propagate automatically without manual schema migration. The only transformations at Bronze are structural: timestamp and coordinate casts (permissive — parse failures become null) and audit columns (`_ingest_timestamp`, `_source_file`, `_run_date`). All other columns remain `StringType`. Bronze is append-only; it is the immutable raw record that enables full replay from any point in time.

### Silver (`03_silver.py`)
Applies data quality and conformance rules to Bronze records for `run_date`. Deduplication on `unique_key` (the NYC Open Data natural key) collapses API pagination overlap. Borough names are standardized across 15+ observed variants to five canonical forms matching the Gold dimensional model. `resolution_days` is computed as a null-safe datediff (null for open requests, not zero). Records with `resolution_days < 0` are logged as a data quality metric and dropped — this is the first of two quality gates, with the second being the dbt singular test in Gold. The Silver write uses Delta `MERGE` on `unique_key`, making the notebook safe to re-run for any date.

### Gold — dbt (`dbt/models/`)
dbt builds the full dimensional model from Silver: a thin staging view (`stg_service_requests`) that renames and casts columns, an intermediate view that applies business classification logic (complaint categories, borough normalization for the star schema), and five mart tables — three dimensions and two facts. The `dim_date` table is generated from a custom `generate_date_spine` macro wrapping `dbt_utils.date_spine`, spanning 2010–2030 with US federal holiday flags. `fct_service_requests` clusters on `created_date_id` for Snowflake query performance. All 59 schema tests and the singular test run as a blocking CI step — a failing test blocks merge.

### Orchestration (`airflow/dags/nyc311_pipeline.py`)
A single Airflow DAG with a linear 7-task dependency chain scheduled at `0 6 * * *` (06:00 UTC). The pipeline gate is an `HttpSensor` that probes the Socrata API with `mode=reschedule` (releases the worker slot between pokes) — nothing downstream runs until the source returns HTTP 200 with a non-empty body. `dbt_run` and `dbt_test` are separate tasks so the Airflow UI can distinguish a model build failure from a data quality failure; these require different remediation paths. An `on_failure_callback` stub is wired to a `slack_alert` function with inline instructions for connecting a Slack webhook via Airflow Connection.

### Infrastructure (`terraform/`)
Terraform provisions the complete Snowflake object hierarchy: one database, three schemas (BRONZE, SILVER, GOLD), one XS virtual warehouse, and four roles with a least-privilege grant matrix enforced as code. `FUTURE TABLES` grants mean any table created by dbt in the GOLD schema automatically inherits the correct permissions without re-running Terraform. The provider is pinned to `~> 0.89` (post-rewrite stable series) with an explicit comment in `main.tf` explaining the v0.87 API break. Remote state lives in Azure Blob Storage with blob lease locking; bootstrap instructions are in `backend.tf`.

---

## How to Run

### 1. Provision infrastructure

```bash
# Bootstrap Azure remote state storage (one-time)
az group create --name nyc311-tfstate-rg --location eastus2
az storage account create --name nyc311tfstate --resource-group nyc311-tfstate-rg \
  --sku Standard_LRS --allow-blob-public-access false
az storage container create --name tfstate --account-name nyc311tfstate

# Set credentials
export ARM_ACCESS_KEY=$(az storage account keys list \
  --account-name nyc311tfstate --query "[0].value" -o tsv)
export SNOWFLAKE_ACCOUNT="MYORG-MYACCOUNT"
export SNOWFLAKE_USER="terraform_user"
export SNOWFLAKE_PASSWORD="..."

# Apply
cd terraform
terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

### 2. Install dbt dependencies

```bash
cd dbt
pip install dbt-snowflake
dbt deps
```

### 3. Configure dbt profile

```bash
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your Snowflake account and credentials
export SNOWFLAKE_ACCOUNT="MYORG-MYACCOUNT"
export SNOWFLAKE_USER="NYC311_TRANSFORMER_DEV"
export SNOWFLAKE_PASSWORD="..."
export SNOWFLAKE_DATABASE="NYC311_DB_DEV"
export SNOWFLAKE_WAREHOUSE="NYC311_WH_DEV"

# Validate connection
cd dbt && dbt debug
```

### 4. Run dbt models and tests

```bash
cd dbt
dbt run --target dev
dbt test --target dev

# Generate and serve lineage documentation
dbt docs generate
dbt docs serve  # opens http://localhost:8080
```

### 5. Run Airflow locally (Astro CLI)

```bash
# Install Astro CLI: https://docs.astronomer.io/astro/cli/install-cli
brew install astro

# Start local Airflow
astro dev start

# Set required Variables (Admin → Variables in Airflow UI, or via CLI):
astro dev run airflow variables set DATABRICKS_INGEST_JOB_ID 12345
astro dev run airflow variables set DATABRICKS_BRONZE_JOB_ID 12346
astro dev run airflow variables set DATABRICKS_SILVER_JOB_ID 12347
astro dev run airflow variables set ALERT_EMAIL "you@example.com"

# Trigger a manual run
astro dev run airflow dags trigger nyc311_pipeline --exec-date 2024-01-15
```

---

## Architecture Decision Records

| ADR | Title | One-sentence summary |
|-----|-------|----------------------|
| [001](docs/adr/001-warehouse-selection.md) | Warehouse Selection | Snowflake chosen over Databricks SQL and Synapse for auto-suspend compute, best-in-class dbt adapter, and strict ETL/BI compute isolation |
| [002](docs/adr/002-transformation-tool.md) | Transformation Tool | dbt Core chosen for version-controlled SQL, DAG lineage graph publishable to GitHub Pages, and schema test framework as a data quality gate |
| [003](docs/adr/003-iac-approach.md) | IaC Approach | Terraform chosen for declarative drift detection, most mature Snowflake provider, and least-privilege enforced as code rather than documentation |
| [004](docs/adr/004-medallion-vs-elt.md) | Medallion vs. ELT | Full medallion chosen so that data quality issues can be diagnosed at their layer of origin rather than appearing as undifferentiated Gold failures |
| [005](docs/adr/005-orchestration-strategy.md) | Orchestration Strategy | Airflow chosen for code-based DAG definition, DatabricksRunNowOperator, HttpSensor gate, and industry recognition in data engineering job requirements |

---

## Design Decisions Worth Discussing

These are the decisions most likely to generate substantive technical conversation in a
Principal Architect or Senior Data Engineer interview.

- **`is_overdue` is NULL for open requests, not FALSE.** `fct_service_requests` uses a three-valued flag: TRUE (closed in > 30 days), FALSE (closed in ≤ 30 days), NULL (still open). A boolean FALSE would make `COUNT(*) FILTER (WHERE NOT is_overdue)` include open requests in the "on-time" bucket — silently inflating the resolution rate. The NULL forces analysts to explicitly decide whether to include or exclude open requests in their aggregations, which is the right default for a fact table with a mixed-status population.

- **The HttpSensor is a cost gate, not just a health check.** The sensor validates both HTTP 200 and a non-empty JSON body before any compute starts. If the Socrata API is up but the dataset refresh hasn't completed (empty response), the sensor stays in POKE mode. The cost of a 5-minute sensor timeout is effectively zero; the cost of a Databricks cluster starting, pulling an incomplete dataset, and writing a partial Bronze partition is a manual replay job plus an incident investigation. Fail-fast at the cheapest point is always correct.

- **`FUTURE TABLES` grants in Terraform eliminate a class of deployment bugs.** Granting `SELECT ON FUTURE TABLES IN SCHEMA GOLD TO ROLE NYC311_REPORTER` means any table dbt creates in GOLD — including new models added after the initial Terraform apply — automatically inherits the correct permissions. The alternative is re-running `terraform apply` after every `dbt run` that creates a new model, which creates a hidden deployment dependency that breaks silently when someone adds a mart model without knowing Terraform must be re-applied.

- **All dimension joins in `fct_service_requests` are LEFT JOINs with an explicit comment explaining why.** Using INNER JOINs against imperfect dimension coverage (not every 311 complaint has a recognized agency code, not every address geocodes to a valid borough) silently drops fact rows. A `COUNT(*)` on the fact table would then disagree with a `COUNT(*)` on the Silver table — a discrepancy that surfaces at 11pm before a board presentation, not during development. NULL foreign keys in the fact table are observable and fixable; silently dropped rows are not.

- **The three-layer contract is a debugging protocol, not an architecture pattern.** The most important property of Bronze/Silver/Gold is not the materialization or the tool — it is that each layer has one failure mode. When a data quality issue surfaces in a Gold mart, you can query Silver to check if the issue is present there. If yes, you query Bronze to check if it came from the source. If no, the bug is in a dbt model. Without three checkpointable layers, you are debugging a black box. With them, you can bisect the pipeline in two queries.

---

## Contact

**Built by:** Marcus Carter  
**Email:** mcarter100k@gmail.com  
**LinkedIn:** [linkedin.com/in/marcuscarter](https://linkedin.com/in/marcuscarter)  

*Portfolio project — all code is production-standard and interview-defensible but is not
deployed against live infrastructure. Terraform is written for a real Snowflake account;
dbt models compile and run against a real Snowflake SILVER schema.*
