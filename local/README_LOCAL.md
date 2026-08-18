# Local Pipeline Runner

Pulls real NYC 311 data from the Socrata API and runs the complete
Bronze → Silver → Gold pipeline on your laptop using DuckDB.
No cloud credentials, no Databricks, no Snowflake needed.

---

## Requirements

- Python 3.11+
- Internet access (to call the Socrata API)

## Setup

```bash
# From the repo root
cd local

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

---

## Run the pipeline

```bash
# All 5 stages, 10,000 rows (takes ~2 min on first run)
python local_runner.py

# Larger dataset — more representative results
python local_runner.py --rows 50000

# Resume from a specific stage (skips earlier stages)
python local_runner.py --stage 3      # Silver forward
python local_runner.py --stage 4      # dbt only
python local_runner.py --stage 5      # reprint results only
```

---

## What it does

| Stage | What happens |
|---|---|
| **1 — Ingest** | Paginates the Socrata API (1,000 rows/request), writes `local/data/raw/nyc311_raw.json` |
| **2 — Bronze** | Loads JSON into DuckDB `bronze.service_requests` with audit columns |
| **3 — Silver** | Deduplicates on `unique_key`, standardizes boroughs, computes `resolution_days`, writes `silver.service_requests` and `silver.data_quality_log` |
| **4 — Gold** | Runs `dbt build --full-refresh` — models, the agency SCD Type 2 snapshot, and tests resolve in DAG order in one command |
| **5 — Results** | Queries the Gold tables and prints five summary tables |

---

## Output

Stage 5 prints:

- **Top 10 complaint types** — what people call 311 about most
- **Avg resolution days by borough** — which boroughs get faster service
- **Complaints per year** — volume trend over the dataset range
- **Open vs closed requests** — current completion rate
- **Data quality checks** — duplicate rate, null rate, borough unrecognized rate

---

## Files

```
local/
├── local_runner.py          # main script
├── requirements.txt         # pinned Python deps
├── profiles.yml             # dbt-duckdb profile
├── dbt_project.yml          # local dbt project
├── packages.yml             # dbt_utils dependency
├── macros/
│   ├── generate_date_spine.sql    # DuckDB-compatible date spine
│   └── generate_schema_name.sql   # schema routing (same as production)
├── models/
│   ├── staging/             # source declarations + staging views
│   ├── intermediate/        # business rules (borough, resolution_days, categories)
│   └── marts/               # dim_* and fct_* tables
├── snapshots/
│   └── agency_snapshot.sql  # SCD Type 2 for agency dimension
└── data/                    # gitignored — created at runtime
    └── raw/
        └── nyc311_raw.json
```

---

## DuckDB compatibility notes

The production dbt models target Snowflake. The local models in `local/models/`
are DuckDB-adapted versions with these changes:

| Production (Snowflake) | Local (DuckDB) |
|---|---|
| `::TIMESTAMP_NTZ` | `::TIMESTAMP` |
| `to_char(date, 'MMMM')` | `strftime(date, '%B')` |
| `dateadd('day', 1, date)` | `date + INTERVAL '1 day'` |
| `cluster_by` config | removed (DuckDB has no clustering) |

All other SQL is identical.

---

## Query the data directly

After running the pipeline, you can explore all the Gold tables with DuckDB:

```python
import duckdb
con = duckdb.connect("local/data/nyc311_local.duckdb", read_only=True)

# All Gold tables
con.sql("SHOW ALL TABLES").df()

# Custom query
con.sql("""
    SELECT complaint_category, borough, COUNT(*) AS n
    FROM gold.fct_service_requests f
    JOIN gold.dim_location l ON f.location_id = l.location_id
    GROUP BY 1, 2
    ORDER BY n DESC
    LIMIT 20
""").df()
```

Or use the DuckDB CLI:

```bash
duckdb local/data/nyc311_local.duckdb
SHOW ALL TABLES;
SELECT * FROM gold.dim_date LIMIT 5;
```
