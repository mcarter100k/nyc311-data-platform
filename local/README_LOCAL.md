# Local Pipeline Runner

Pulls real NYC 311 data from the Socrata API and runs the complete
Bronze → Silver → Gold pipeline on your laptop using DuckDB.
No cloud credentials, no the cloud spec, no Snowflake needed.

---

## Requirements

- Python 3.11+
- Internet access (to call the Socrata API)

## Setup

```bash
# From the repo root
cd local

python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

---

## Run the pipeline

```bash
# All 5 stages, the 10,000 most recent rows (takes ~2 min on first run)
python local_runner.py

# Larger sample — same recency, more of it
python local_runner.py --rows 50000

# The trailing 7-day window the scheduled daily run uses
python local_runner.py --live

# Resume from a specific stage (skips earlier stages)
python local_runner.py --stage 3      # Silver forward
python local_runner.py --stage 4      # dbt only
python local_runner.py --stage 5      # reprint results only
```

---

## What it does

| Stage | What happens |
|---|---|
| **1 — Ingest** | Paginates the Socrata API (1,000 rows/request), **newest first** so the sample matches the recent window the models are tuned on, writes `local/data/raw/nyc311_raw.json` |
| **2 — Bronze** | Registers `bronze.service_requests` as a **view** over the raw file — raw stays SQL-queryable without being copied into the warehouse |
| **3 — Silver** | Reads the raw file, deduplicates on `unique_key`, standardizes boroughs, computes `resolution_days`, then writes `silver.service_requests` and `silver.data_quality_log` |
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
| `to_char(date, 'MMMM')` / `decode(...)` | `strftime(date, '%B')` |
| `dateadd(...)` | `INTERVAL` arithmetic |
| `dayofweekiso` / `weekiso` | `isodow` / `weekofyear` (ISO-equivalent) |
| `cluster_by` config | removed (DuckDB has no clustering) |
| `merge` incremental strategy | `delete+insert` (upsert-equivalent on the unique key) |
| `initcap(name)` | space-delimited title-casing in the snapshot — **known divergence:** Snowflake's `initcap` also breaks words at hyphens/parens, DuckDB has no `initcap`, so hyphenated agency names normalize differently |
| `publish_gold` macro (schema swap) | not mirrored — no write-audit-publish locally |
| explanatory comments | largely stripped |

The model `.yml` test suites, the three singular tests in `tests/`, and the
project vars are mirrored verbatim. The authoritative, machine-checked list of
every remaining divergence is `scripts/model_drift_baseline.json` — CI fails
if the trees drift beyond it (see "Keeping the mirror honest" below).

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

Or use the DuckDB CLI — **note that `requirements.txt` does not install it.**
`pip install duckdb` ships the Python library only; the `duckdb` binary is a
separate download ([duckdb.org/docs/installation](https://duckdb.org/docs/installation),
or `brew install duckdb`). The Python snippet above needs nothing extra.

```bash
duckdb local/data/nyc311_local.duckdb
SHOW ALL TABLES;
SELECT * FROM gold.dim_date LIMIT 5;
```

## Verifying against the source

Tests prove the pipeline agrees with itself; `reconcile.py` proves it agrees
with the city. After any run:

```bash
python local/reconcile.py
```

Three rungs: (1) **conservation** — every ingested record accounted for
across layers, quarantine count independently recomputed; (2) **independent
recomputation** — closed counts, borough distribution, per-record resolution
days, and exact `created_date` timestamps recomputed from the raw JSON with
no DuckDB or dbt involved; (3) **live spot-check** — sampled records fetched
back from the Socrata API by `unique_key` and compared field by field
(skips gracefully offline). Exit 0 means reconciled; any mismatch names the
exact record and field and exits 1.

This check exists because a run with a fully green test suite once carried a
4-hour timestamp shift (`utc=True` mislabeling in the runner) that only
rung 2's exact-timestamp comparison exposed.

## Keeping the mirror honest

These models are a hand-maintained DuckDB mirror of `dbt/` — deliberate
duplication (it is what makes the behavioral test tier possible), guarded the
same way the README's numbers are: every intentional dialect divergence
(`dayofweekiso` vs `isodow`, `merge` vs `delete+insert`, …) is registered in
`scripts/model_drift_baseline.json`, and `scripts/check_model_drift.py` fails
CI if the two trees drift beyond the register. After an intentional change to
BOTH sides, re-register with `python scripts/check_model_drift.py --update`
and let the baseline diff be reviewed like any other code.
