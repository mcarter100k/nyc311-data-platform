# NYC 311 Data Platform

[![CI](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/ci.yml)
[![Terraform](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/terraform.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/terraform.yml)
[![Daily Live Run](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/daily-run.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/daily-run.yml)

A medallion data platform over NYC's 311 service requests — **it runs daily against the live API, it has service level objectives, and it has survived a real upstream incident.** The full Bronze→Silver→Gold flow runs on a laptop against DuckDB; the cloud deployment is written down but deliberately not provisioned ([ADR 008](docs/adr/008-prototype-scope.md)).

---

## What the data says

Any pipeline can move rows. These came out of this one — measured on the 72,312 requests it loaded for **12–19 Aug 2026**:

**"Closed" usually does not mean "fixed."** The city closed 46,627 requests in that window. Reading each closure's own text, **only 35.9% describe the city doing anything** — the rest closed as *no violation found*, *nothing there*, duplicate, or handed off.

| Category | Marked resolved | Actually actioned |
|---|---|---|
| Illegal Parking | 93% | 42% |
| Abandoned Vehicle | 93% | 28% |
| **Homeless Services** | **89%** | **16%** |
| Noise | 89% | 32% |

A resolution rate of 89% and an action rate of 16% describe very different cities. The platform reports both, because reporting only the first would be flattering and wrong.

**The weekend city complains about different things, not more things.** Total volume barely moves (8,918/day → 9,403/day), but noise complaints **multiply 2.4×** — 1,411/day on weekdays to 3,410/day at weekends. Composition flips; volume doesn't.

**And "nothing found" mostly means nothing was there.** The only test of resolution quality available from 311 alone is whether the same complaint reappears at the same address. Comparing recurrence within 3 days, excluding chronic locations and controlling for how much observation time each closure actually had:

| Closed as | Recurred |
|---|---|
| Access Failed — the city couldn't get in | **15.3%** |
| No Violation Found | 12.3% |
| Work Performed | 9.2% |
| **No Condition Found** | **8.5%** |

Physical work sticks; failed access does not. But the result that matters is the last row: closures reporting *"nothing there"* recur **least**, which argues they were mostly correct — the problem had resolved itself before anyone arrived — rather than premature. That distinction is invisible in the resolution rate, and it reverses the obvious assumption.

Caveats, because they are load-bearing: one week of history supports only a 3-day window, and recurrence is evidence rather than proof — a repeat can mean the fix failed *or* that the condition is legal and residents keep reporting it. Excluding chronic locations roughly halves the spread, so an unfiltered rate is not a finding. Both guards are columns on [`fct_complaint_recurrence`](dbt/models/marts/fct_complaint_recurrence.sql).

Run the same analysis yourself in about a minute — no cloud account needed:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r local/requirements.txt -r dbt/requirements.txt -r requirements-dev.txt
python local/local_runner.py --live          # fetch → bronze → silver → dbt gold
```

**You will not get the numbers above, and that is expected.** `--live` fetches a
trailing seven days ending *today*, so your window is not 12–19 Aug 2026 — the
counts and percentages will differ. The comparisons should hold; the digits will not.

Then query Gold. There is no `duckdb` command to run: `pip install duckdb` ships
the Python library, not a CLI binary. Query it from Python instead —

```python
import duckdb
con = duckdb.connect("local/data/nyc311_local.duckdb", read_only=True)

# Action rate. The headline denominator is CLOSED requests, not all requests:
con.sql("""
    select count(*) filter (where is_actioned) * 1.0 / count(*) as pct_actioned_of_closed
    from gold.fct_service_requests
    where is_resolved
""").show()

# gold.fct_daily_volume carries pct_actioned too, but over ALL rows — open
# requests included — so it is a different, lower number. Same column name,
# different denominator; check which one a chart is using.

# Recurrence is not in fct_daily_volume at all; it has its own fact table,
# with the two guards (chronic locations, observation time) as columns:
con.sql("""
    select closure_type,
           avg(case when days_to_next_same_complaint <= 3 then 1.0 else 0.0 end) as recurred_3d
    from gold.fct_complaint_recurrence
    where not is_chronic_location and observation_days >= 3
    group by 1 order by 2 desc
""").show()
```

---

## Design Decisions Worth Discussing

These are the decisions where the trade-off was genuinely close, and where the reasoning matters more than the result.

**`is_overdue` is NULL for open requests, not FALSE.**
`fct_service_requests` uses a three-valued flag: TRUE (closed in > 30 days), FALSE (closed in ≤ 30 days), NULL (still open). A boolean FALSE would cause `COUNT(*) FILTER (WHERE NOT is_overdue)` to count open requests as "on time" — silently inflating the resolution rate. NULL forces analysts to explicitly decide whether to include or exclude open requests, which is the correct default for a mixed-status fact table.

**The HttpSensor is a cost gate, not just a health check.**
The sensor validates HTTP 200 and a non-empty JSON body before any downstream work starts. If the Socrata API is up but the daily refresh hasn't completed, the sensor waits. The cost of a five-minute sensor timeout is zero. The cost of proceeding — pulling an incomplete dataset and writing a partial Bronze partition — is a manual replay plus an incident investigation. On a warehouse billed by the second, that gap is also money.

**`FUTURE TABLES` grants interact with schema-swap publishing — and the interaction has to be designed, not assumed.**
`SELECT ON FUTURE TABLES` lets any table dbt creates inherit reporter permissions without a Terraform re-apply ([main.tf:471-481](terraform/modules/snowflake-foundation/main.tf#L471-L481)). But Snowflake grants attach to the schema *object*, and the write-audit-publish swap renames objects — so grants defined only on GOLD stop covering it after the first publish. [ADR 009](docs/adr/009-publish-grants-under-schema-swap.md) resolves this: the grant matrix is specified symmetrically on both GOLD and GOLD_AUDIT, keeping the single atomic swap (the alternative — per-table view swaps — was rejected because it reintroduces the cross-table inconsistency window WAP exists to eliminate).

**All dimension joins in `fct_service_requests` are LEFT JOINs.**
Not every 311 complaint has a recognized agency code or a geocodable address. INNER JOINs against imperfect dimension coverage silently drop fact rows — a `COUNT(*)` on the fact table then disagrees with Silver, and that discrepancy surfaces at 11pm before a board presentation. NULL foreign keys in the fact table are visible and fixable. Silently dropped rows are not.

**Three layers are a debugging protocol, not an architecture pattern.**
The most important property of Bronze/Silver/Gold is not the materialization or the tool — it is that each layer has exactly one failure mode. When a data quality issue appears in a Gold mart, you query Silver. If the issue is there, you query Bronze. If not, the bug is in a dbt model. Without three checkpointed layers, you are debugging a black box. With them, you bisect any pipeline bug in two queries.

---

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

## What is real, and what is deferred

The distinction is enforced, not asserted: [scripts/check_claims.py](scripts/check_claims.py) fails CI when this README drifts from the repo, and [docs/CLAIMS.md](docs/CLAIMS.md) maps every claim to the code that enforces it and the test that verifies it.

| Real — verifiable in this repo | Evidence |
|---|---|
| End-to-end pipeline on DuckDB: ingest → bronze → silver → dbt gold | [local/local_runner.py](local/local_runner.py) |
| Runs daily against the live API, gated by two SLOs | [daily-run.yml](.github/workflows/daily-run.yml), [ADR 010](docs/adr/010-scheduled-operation.md) |
| Airflow orchestrates it locally — 7-task DAG, verified with `airflow dags test`, all tasks green (a demonstration; GitHub Actions remains the scheduler) | [nyc311_local.py](airflow/dags/nyc311_local.py) |
| Three parallel CI checks, **enforced** by branch protection declared in Terraform | [ci.yml](.github/workflows/ci.yml), [terraform/github/](terraform/github/), [ADR 012](docs/adr/012-github-repo-as-code.md) |
| Silver transform logic unit-tested against fixtures | [tests/unit/](tests/unit/), [local/silver_transformations.py](local/silver_transformations.py) |
| Terraform **applied** — this repo's labels, branch protection and Pages are declared and live | [terraform/github/](terraform/github/), [ADR 012](docs/adr/012-github-repo-as-code.md) |

| Deferred — specified, never provisioned | Where |
|---|---|
| A Snowflake account | [terraform/](terraform/) — the warehouse module is validated in CI, never applied. The GitHub module beside it *is* applied |
| Loading Silver into Snowflake | the dbt project targets Snowflake; the load mechanism is an open decision ([ADR 008](docs/adr/008-prototype-scope.md)) |

Nothing here claims to run in a cloud account. Everything that claims to run, runs.

---

## Architecture

Bronze → Silver → Gold, where **three layers are a debugging protocol, not an architecture pattern**: each layer has exactly one failure mode, so a break is isolated to a known stage rather than hunted across a monolith.

```
Socrata API → raw JSON → Bronze → Silver → GOLD (dbt star schema) → BI
                 pandas ──────────────────┘        └──────── dbt: staging → intermediate → marts
```

Gold is a Kimball star: <!--claim:fct_models-->4<!--/claim--> fact tables, 3 dimensions, and a 21-year calendar spine (2010–2030). Terraform provisions the Snowflake side — 5 schemas (including `GOLD_AUDIT` for write-audit-publish and `SNAPSHOTS` for SCD2 state), 4 roles, and a least-privilege grant matrix enforced as code.

**Full detail:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — per-layer contracts, the dbt model inventory, the stack table, and what each layer guarantees.

---

## Test Suite

Two populations, deliberately not summed: **<!--claim:test_count-->124<!--/claim--> pytest tests** that need no cloud account, and **119 dbt data tests (113 generic + 6 singular)** that run against the warehouse during `dbt build`. The pytest count is recomputed in CI.

| Tier | Count | What it proves |
|---|---|---|
| **Structural** | 89 | Configuration correctness — schema resolution, incremental strategy, DAG lineage, freshness target, Terraform validity, the LOADER-has-no-TRUNCATE contract |
| **Unit** | 8 | Silver transformation *logic* ([local/silver_transformations.py](local/silver_transformations.py)) against hand-built fixtures |
| **Behavioral** | 27 | Gold *semantics* — builds the dbt project twice on seeded DuckDB and asserts on output rows: watermark lookback, SCD2 point-in-time join, update propagation. Also import health for every module in [local/](local/), which needs the real runtime dependencies this tier installs |

The structural tier is the largest and the weakest, and it is worth saying so: it catches config drift and silent contract violations in seconds, but **a model can be perfectly configured and still compute the wrong number.** That is what the other two tiers are for.

```bash
./run_tests.sh          # full pytest suite (the behavioral tier skips without dbt-duckdb)
```

---

## Architecture Decision Records

ADRs document the reasoning behind major technology choices: the alternatives weighed, the decision, and the consequences accepted.

| ADR | Decision | Outcome |
|---|---|---|
| [001](docs/adr/001-warehouse-selection.md) | Snowflake over Databricks SQL for the serving layer | ETL and BI workloads isolated on separate warehouses; dbt adapter maturity |
| [002](docs/adr/002-transformation-tool.md) | dbt over hand-written transform code for the Gold layer | Gold logic becomes testable SQL with lineage and documentation |
| [003](docs/adr/003-iac-approach.md) | Terraform as the single IaC surface across three providers | One state file, one plan; drift detection is an apply-time step, not CI |
| [004](docs/adr/004-medallion-vs-elt.md) | Medallion layering over direct ELT | One failure mode per layer, so a break is isolated to a known stage |
| [005](docs/adr/005-orchestration-strategy.md) | Airflow, single DAG, write-audit-publish dbt stage | Sensor gate before paid compute; Gold never serves unvalidated data |
| [006](docs/adr/006-schema-evolution.md) | Schema version stamp over runtime column detection | Each fact row records the contract that built it; additive changes need no bump |
| [007](docs/adr/007-scd-type-2-dim-agency.md) | dbt snapshot (check strategy) for agency SCD Type 2 | Point-in-time fact join; a 2021 request keeps its 2021 agency name |
| [008](docs/adr/008-prototype-scope.md) | Cloud services specified, not provisioned | Every cloud claim in this repo is scoped as spec, never as running |
| [009](docs/adr/009-publish-grants-under-schema-swap.md) | Symmetric grants on GOLD and GOLD_AUDIT | The publish swap keeps REPORTER access; grants follow renamed objects |
| [010](docs/adr/010-scheduled-operation.md) | Scheduled daily operation with written SLOs | The pipeline runs live daily; a breach files a tracked issue |
| [011](docs/adr/011-parallel-ci-tiers.md) | Three parallel required CI checks, not one sequential job | A red check names its failure class; wall time is the slowest tier |
| [012](docs/adr/012-github-repo-as-code.md) | The repository's own infrastructure is Terraform, and it is applied | Labels, branch protection and Pages declared and applied; "required checks" became true rather than aspirational |
| [013](docs/adr/013-no-source-freshness-slo.md) | No source-freshness SLO — gate on what we control, warn on what we don't | A proposed third SLO was measured and rejected; source staleness stays a warning |
| [014](docs/adr/014-transform-before-load.md) | Transform before load — Bronze is the raw file, not a warehouse table | The raw round-trip through DuckDB is gone; Bronze is a view, and the Bronze→Silver hop is honestly ETL |

---

## Run it locally

No cloud accounts, no credentials:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r local/requirements.txt -r dbt/requirements.txt -r requirements-dev.txt
python local/local_runner.py --live      # live data; omit --live for a sample
./run_tests.sh
```

Three requirements files, because the pipeline and the test suite need different dbt adapters: `local/` runs on **dbt-duckdb**, while `run_tests.sh` parses the `dbt/` project and needs **dbt-snowflake**. `requirements-dev.txt` carries the test tooling (pytest, pyyaml, ruff). CI runs this exact install and then `./run_tests.sh` in the `front-door` job.

Airflow (optional, demonstration): `./scripts/airflow_local.sh ui` → localhost:8080.
Cloud deployment steps, which require your own Azure + Snowflake accounts, are in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## Repository map

```
local/          the pipeline — ingest, pandas Silver (silver_transformations.py), DuckDB Gold
dbt/            Snowflake dbt project (models, snapshots, macros, tests)
airflow/dags/   nyc311_local.py — the 7-task DAG, smoke-tested green
terraform/      Snowflake foundation — 5 schemas, 4 roles, grant matrix (validated, not applied)
terraform/github/  this repo's own infrastructure — labels, branch protection, Pages (applied)
config/         borough_variants.csv — one mapping, read by Python and dbt alike
scripts/        SLO checks, claim checker, model-drift guard
docs/           ARCHITECTURE · SLO · CLAIMS · BACKLOG · adr/ (<!--claim:adr_count-->14<!--/claim-->) · postmortems/
tests/          124 pytest tests across three tiers
```

---

## Contact

**Marquis Carter**
Data Engineer
marq.dcarter@gmail.com
[LinkedIn](https://www.linkedin.com/in/marquis-c-45132325b/) · [GitHub](https://github.com/mcarter100k)
