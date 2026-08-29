# NYC 311 Data Platform

[![CI](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/ci.yml)
[![Terraform](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/terraform.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/terraform.yml)
[![Daily Live Run](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/daily-run.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/daily-run.yml)

**A medallion data platform over NYC's 311 service requests that actually runs.** It pulls live data every day, holds itself to written service level objectives, files a GitHub issue when it breaches them, and has already survived a real upstream outage. Clone it and the whole Bronze → Silver → Gold pipeline builds on your laptop in about 20 seconds — no cloud account, no credentials.

|  |  |
|---|---|
| **Stack** | Python · pandas · DuckDB · dbt · Airflow · Terraform · GitHub Actions |
| **Warehouse model** | Kimball star — <!--claim:fct_models-->4<!--/claim--> facts, <!--claim:dim_models-->3<!--/claim--> dimensions, SCD Type 2, write-audit-publish |
| **Scale** | ~127k requests per 12-day window, pulled from the live Socrata API |
| **Tested by** | <!--claim:test_count-->255<!--/claim--> pytest tests + <!--claim:dbt_test_count-->132<!--/claim--> dbt data tests |
| **Runs** | Daily at 10:20 UTC, gated by 2 SLOs, with a heartbeat watching the scheduler |
| **Documented by** | <!--claim:adr_count-->16<!--/claim--> ADRs and a postmortem — and a CI job that fails if any of it drifts from the code |

---

## Why this one is different

Most portfolio pipelines are a DAG that moved some rows once. Four things here are unusual, and each is verifiable in the repo:

**It is operated, not just built.** A scheduled run against the live API, two SLOs with thresholds, automatic breach issues, and a separate 4-hourly heartbeat that notices when the daily run doesn't happen at all — because a monitor that lives inside the thing it monitors cannot see that failure.

**It survived a real incident and produced a control.** On 2026-08-18 the city's publish process stalled. Every pipeline stage ran green; only the source-facing check caught it. The fix redefined what the completeness SLO measures. → [postmortem](docs/postmortems/2026-08-18-upstream-publish-stall.md)

**Its documentation cannot lie.** [`scripts/check_claims.py`](scripts/check_claims.py) fails CI when this README or anything under `docs/` drifts from the code — test counts, DAG task names, the dbt model inventory, every link, every cited code string. Retired claims are registered so they cannot quietly return.

**Its tests are proven able to fail.** A green check that cannot go red is worse than no check, because it gets read as evidence. Every guard here has been mutation-tested: break the thing it protects, watch it fail, revert. This repo shipped three checks that couldn't fail before that became the standard.

---

## What the data says

Measured on the 127,255 requests loaded for the twelve complete days **13–24 Aug 2026**.

**"Closed" usually does not mean "fixed."** Of 89,506 closures, between **35% and 44%** describe the city doing anything — the rest closed as *no violation found*, *nothing there*, duplicate, or handed off.

It's a range because 7,571 closures (8.5%) carry resolution text no rule can classify. Publishing the interval is the honest move; per category, the width *is* the finding:

| Category | Actioned | Uncertainty |
|---|---|---|
| Illegal Parking | 42–43% | 0.7pp |
| Noise | 40–41% | 1.5pp |
| **Homeless Services** | **17–20%** | 3.6pp |
| Street Condition | 28–46% | 17.9pp |
| Water & Sewer | 17–40% | 22.3pp |

For Illegal Parking the answer is known to a point. For Water & Sewer we cannot say whether the city acted on a fifth or two fifths — so the platform prints the interval instead of a midpoint dressed as a result.

**A closure the city couldn't complete is the one that comes back.** *Access Failed* recurs at 13.8% within 3 days, ranking first in **all eight** specifications tried (windows of 2/3/4/5 days, each with and without chronic locations), leading the runner-up by 1.1–5.8 points.

**And one headline finding was withdrawn.** An earlier version of this section argued that "nothing there" closures recur least and were therefore mostly correct. On twelve days of data that ordering reverses. It came from a seven-day load, and a conclusion that changes sign when the window grows was never strong enough to publish. It is registered in the claim checker so it cannot come back.

<details>
<summary><b>The methodology, and what doesn't survive it</b></summary>

Absolute recurrence rates are not portable: *Work Performed* moves between 4.2% and 15.8% on identical data depending only on the window and whether chronic locations are included — a nearly fourfold spread. Only the *ranking* of Access Failed is stable, and only among closure types whose text could be decoded. Ranked over all rows the top slot goes to `Unspecified` — closures with no resolution text at all — which is an absence of information, not a finding about the city.

Full 3-day table, every decoded closure type with ≥200 closures, not a selection:

| Closed as | Recurred within 3 days |
|---|---|
| **Access Failed** | **13.8%** |
| Resolved on Scene | 10.5% |
| Duplicate | 10.4% |
| Enforcement Action | 10.4% |
| Referred Elsewhere | 10.2% |
| No Violation Found | 9.7% |
| No Condition Found | 6.7% |
| Work Performed | 5.7% |

Recurrence is evidence rather than proof — a repeat can mean the fix failed *or* that the condition is legal and residents keep reporting it. Both guards are columns on [`fct_complaint_recurrence`](dbt/models/marts/fct_complaint_recurrence.sql), so any specification above can be reproduced.

Volume, for completeness: weekday traffic runs 10,955/day against 9,903/day at weekends, while noise complaints more than double, 1,730 → 3,589. Composition shifts hard; volume drifts down. An earlier version of this README reported the volume comparison backwards, from dividing weekday and weekend totals by the same day count over a window holding eight of one and four of the other.

</details>

<details>
<summary><b>Reproduce it yourself — the exact queries</b></summary>

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r local/requirements.txt -r dbt/requirements.txt -r requirements-dev.txt
python local/local_runner.py --live          # fetch → bronze → silver → dbt gold
```

**You will not get the numbers above, and that is expected.** `--live` fetches a trailing seven days ending *today*. Only the Access Failed ranking has been shown to hold across specifications — treat a single window as one observation, not a result. A 7-day window is also narrower than the twelve days measured above, so `fct_daily_volume` will return NULL for every rate in it: a 7-day load cannot answer a 30-day question, and `n/a` is the correct answer rather than a bug.

There is no `duckdb` CLI — `pip install duckdb` ships the Python library. Query it from Python:

```python
import duckdb
con = duckdb.connect("local/data/nyc311_local.duckdb", read_only=True)

# Action rate. Denominator is CLOSED requests, and it has a known floor: rows
# whose resolution text the decoder could not read are is_actioned = FALSE,
# so measure that too rather than quoting the rate alone.
con.sql("""
    select count(*) filter (where is_actioned) * 1.0 / count(*)                 as pct_actioned_of_closed,
           count(*) filter (where closure_type = 'Undecodable') * 1.0 / count(*) as pct_undecodable
    from gold.fct_service_requests
    where is_resolved
""").show()

# Every rate on fct_daily_volume is NULL unless is_denominator_closed — a rate
# over requests created recently is right-censored, so it is suppressed.
con.sql("""
    select is_denominator_closed, count(*) as groups,
           count(pct_closed_within_window) as groups_publishing_a_rate
    from gold.fct_daily_volume group by 1
""").show()

# Recurrence has its own fact table, with both guards as columns:
con.sql("""
    select closure_type,
           avg(case when days_to_next_same_complaint <= 3 then 1.0 else 0.0 end) as recurred_3d
    from gold.fct_complaint_recurrence
    where not is_chronic_location and observation_days >= 3
    group by 1 order by 2 desc
""").show()
```

</details>

---

## Architecture

```
Socrata API → raw JSON → Bronze → Silver → GOLD (dbt star schema) → BI
                 pandas ──────────────────┘        └──────── dbt: staging → intermediate → marts
```

Bronze → Silver → Gold, where **three layers are a debugging protocol, not an architecture pattern**: each layer has exactly one failure mode. When a number looks wrong in Gold you query Silver; if it's wrong there you query Bronze. Two queries bisect any pipeline bug. Without checkpointed layers you are debugging a black box.

Gold is a Kimball star with a 21-year calendar spine (2010–2030). Terraform provisions the Snowflake side — 5 schemas (including `GOLD_AUDIT` for write-audit-publish and `SNAPSHOTS` for SCD2 state), 4 roles, and a least-privilege grant matrix as code.

**Full detail:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — per-layer contracts, model inventory, and what each layer guarantees.

<details>
<summary><b>Four design decisions where the trade-off was genuinely close</b></summary>

**`is_overdue` is NULL for open requests, not FALSE.** A boolean FALSE would make `COUNT(*) FILTER (WHERE NOT is_overdue)` count open requests as "on time," silently inflating the resolution rate. NULL forces analysts to decide explicitly. The flag keys on `status`, not on whether a `closed_date` is present — the source emits rows carrying a closed_date while still Open, and keying on the date alone let 4,139 of them count as "on time" by the very expression this design exists to protect.

**All dimension joins in `fct_service_requests` are LEFT JOINs.** Not every complaint has a recognized agency code or geocodable address. INNER JOINs against imperfect dimension coverage silently drop fact rows — then `COUNT(*)` disagrees with Silver and you find out at 11pm before a board presentation. NULL foreign keys are visible and fixable; dropped rows are not.

**The source gate is a cost decision, and weaker than it looks.** `check_source` runs `curl` against the API before any downstream task, so a dead endpoint fails the run in seconds rather than part-way through a load. But be precise: it checks HTTP status and **discards the body** (`-o /dev/null`), so a source returning `200` with an empty array — the published August 2026 stall — passes cleanly. The zero-row check in `fetch_live_records` is what actually catches that, one task later.

**`FUTURE TABLES` grants interact with schema-swap publishing.** `SELECT ON FUTURE TABLES` lets dbt-created tables inherit reporter permissions without a Terraform re-apply. But Snowflake grants attach to the schema *object*, and the write-audit-publish swap renames objects — so grants defined only on GOLD stop covering it after the first publish. [ADR 009](docs/adr/009-publish-grants-under-schema-swap.md) specifies the matrix symmetrically on both GOLD and GOLD_AUDIT, keeping the single atomic swap.

</details>

---

## Operating it

| SLO | Measures | Threshold |
|---|---|---|
| **SLO-1 freshness** | age of the newest `_loaded_at` in `gold.fct_service_requests` | < 26 hours — one daily cycle plus 2h grace. Measures *our* liveness, not source staleness |
| **SLO-2 completeness** | rows we loaded vs rows the city actually **published**, for every day the load shows **complete** | ≥ 98% on every such day |

A failed run or breach files a `daily-run-breach` GitHub issue with the measured numbers and run URL; a persisting breach comments on the open issue rather than duplicating it. An issue beats an email — it is a tracked, assignable work item a postmortem can link to.

**Watching the watcher.** The SLO check runs *inside* the daily run, so it cannot see the case where no run happens — GitHub disables scheduled workflows on public repos after 60 days, and cron delivery is best-effort. A separate 4-hourly [heartbeat](.github/workflows/heartbeat.yml) reads the Actions API from outside and files into the same issue stream if the daily run is disabled or hasn't succeeded within 26 hours — SLO-1's threshold, measured from the other side.

<details>
<summary><b>Why the 2% tolerance is mostly not ours — and the 7-day settling horizon</b></summary>

Socrata answers identical queries from two replicas, one behind the other by an amount that shrinks with a day's age and reaches **zero at 7 days**. So a day still settling can be reconciled against a fresher count than the load was served. Quarantine and dedup account for up to 0.24% of the budget; the settling lag accounts for up to 0.96% — [ADR 016](docs/adr/016-source-settling-horizon.md).

The gate's population is the load's own completeness verdict, not an offset from the clock: the publish lag is not a constant (23.3h, 23.5h, then 49.0h measured within one week), so any fixed window is a whole day sometimes and a two-hour stub other times — [ADR 015](docs/adr/015-slo2-population-is-complete-days.md). The executable queries live in [scripts/slo/](scripts/slo/), and CI fails if [docs/SLO.md](docs/SLO.md) and those files drift apart.

</details>

<details>
<summary><b>A self-audit finding, and why the obvious fix was rejected</b></summary>

The ingestion watermark keyed on `created_date`, fetching each record exactly once — on the day it was filed. But 311 requests mutate after creation (status flips to Closed days later), so every downstream update path was unreachable and resolution metrics would only ever have counted same-day closures. Found by systematic self-audit, not by a failure.

The obvious repair — keying on `:updated_at` — was measured and **rejected**: that field is mass re-stamped nightly, ~540k rows/day against ~53k created per week ([ADR 010](docs/adr/010-scheduled-operation.md)), which a row-capped daily fetch cannot absorb. The daily run instead re-pulls a trailing 7-day `created_date` window in full, so status changes *inside* that window are captured. Updates to rows older than the window are still missed, and that limit is tracked in [docs/BACKLOG.md](docs/BACKLOG.md) rather than papered over.

</details>

---

## What is real, and what is deferred

**Nothing here claims to run in a cloud account. Everything that claims to run, runs.** The distinction is enforced by [`scripts/check_claims.py`](scripts/check_claims.py), and [docs/CLAIMS.md](docs/CLAIMS.md) maps every claim to the code that enforces it and the test that verifies it.

| Real — verifiable in this repo | Evidence |
|---|---|
| End-to-end pipeline on DuckDB: ingest → bronze → silver → dbt gold | [local/local_runner.py](local/local_runner.py) |
| Runs daily against the live API, gated by two SLOs | [daily-run.yml](.github/workflows/daily-run.yml), [ADR 010](docs/adr/010-scheduled-operation.md) |
| Airflow orchestrates it locally — 7-task DAG, dependency graph asserted in CI | [nyc311_local.py](airflow/dags/nyc311_local.py) |
| Three parallel CI checks, **enforced** by branch protection declared in Terraform | [ci.yml](.github/workflows/ci.yml), [ADR 012](docs/adr/012-github-repo-as-code.md) |
| Terraform **applied** — this repo's labels, branch protection and Pages are live | [terraform/github/](terraform/github/) |

| Deferred — specified, never provisioned | Where |
|---|---|
| A Snowflake account | [terraform/](terraform/) — the warehouse module is validated in CI, never applied |
| Loading Silver into Snowflake | the dbt project targets Snowflake; the load mechanism is an open decision ([ADR 008](docs/adr/008-prototype-scope.md)) |

---

## Test suite

Two populations, deliberately not summed: **<!--claim:test_count-->255<!--/claim--> pytest tests** needing no cloud account, and **<!--claim:dbt_test_count-->132<!--/claim--> dbt data tests (<!--claim:dbt_generic_tests-->121<!--/claim--> generic + <!--claim:dbt_singular_tests-->11<!--/claim--> singular)** running against the warehouse during `dbt build`. Both are recomputed in CI — pytest from collection, dbt from the parsed manifest.

| Tier | Count | What it proves |
|---|---|---|
| **Structural** | <!--claim:structural_test_count-->175<!--/claim--> | Configuration correctness — schema resolution, incremental strategy, DAG lineage and task *ordering*, Terraform validity, the LOADER-has-no-TRUNCATE contract. Also the documentation guards' own failure modes ([tests/test_doc_guards.py](tests/test_doc_guards.py)) |
| **Unit** | <!--claim:unit_test_count-->9<!--/claim--> | Silver transformation *logic* ([local/silver_transformations.py](local/silver_transformations.py)) against hand-built fixtures |
| **Behavioral** | <!--claim:behavioral_test_count-->71<!--/claim--> | Gold *semantics* — builds the dbt project twice on seeded DuckDB and asserts on output rows: watermark lookback, SCD2 point-in-time join, update propagation, dimension retention when Silver's window moves past a member the fact still references |

The structural tier is the largest and the weakest, and it's worth saying so: it catches config drift in seconds, but **a model can be perfectly configured and still compute the wrong number.** That is what the other two tiers are for. The behavioral job also fails if any test *skips* — a skipped test is green, and two of them silently never ran in CI until that gate existed.

---

## Run it locally

No cloud accounts, no credentials:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r local/requirements.txt -r dbt/requirements.txt -r requirements-dev.txt
python local/local_runner.py --live      # live data; omit --live for a sample
./run_tests.sh
```

Three requirements files because the pipeline and the test suite need different dbt adapters: `local/` runs on **dbt-duckdb**, while `run_tests.sh` parses the `dbt/` project and needs **dbt-snowflake**. CI runs this exact install in its `front-door` job.

Airflow (optional, demonstration): `./scripts/airflow_local.sh ui` → localhost:8080.

There is no cloud deployment runbook — and this line used to promise one, with a link that resolved to a document not containing the steps. What exists is the Terraform module under [terraform/](terraform/), validated in CI and never applied.

---

## Architecture Decision Records

<details>
<summary><b>All <!--claim:adr_count-->16<!--/claim--> decisions — the alternatives weighed, and the consequences accepted</b></summary>

| ADR | Decision | Outcome |
|---|---|---|
| [001](docs/adr/001-warehouse-selection.md) | Snowflake over Databricks SQL for the serving layer | ETL and BI workloads isolated on separate warehouses; dbt adapter maturity |
| [002](docs/adr/002-transformation-tool.md) | dbt over hand-written transform code for the Gold layer | Gold logic becomes testable SQL with lineage and documentation |
| [003](docs/adr/003-iac-approach.md) | Terraform as the single IaC surface across three providers | One state file, one plan; drift detection is an apply-time step, not CI |
| [004](docs/adr/004-medallion-vs-elt.md) | Medallion layering over direct ELT | One failure mode per layer, so a break is isolated to a known stage |
| [005](docs/adr/005-orchestration-strategy.md) | Airflow, single DAG, write-audit-publish dbt stage | Gate before paid compute; Gold never serves unvalidated data |
| [006](docs/adr/006-schema-evolution.md) | Schema version stamp over runtime column detection | Each fact row records the contract that built it; additive changes need no bump |
| [007](docs/adr/007-scd-type-2-dim-agency.md) | dbt snapshot (check strategy) for agency SCD Type 2 | Point-in-time fact join; a 2021 request keeps its 2021 agency name |
| [008](docs/adr/008-prototype-scope.md) | Cloud services specified, not provisioned | Every cloud claim in this repo is scoped as spec, never as running |
| [009](docs/adr/009-publish-grants-under-schema-swap.md) | Symmetric grants on GOLD and GOLD_AUDIT | The publish swap keeps REPORTER access; grants follow renamed objects |
| [010](docs/adr/010-scheduled-operation.md) | Scheduled daily operation with written SLOs | The pipeline runs live daily; a breach files a tracked issue |
| [011](docs/adr/011-parallel-ci-tiers.md) | Three parallel required CI checks, not one sequential job | A red check names its failure class; wall time is the slowest tier |
| [012](docs/adr/012-github-repo-as-code.md) | The repository's own infrastructure is Terraform, and it is applied | "Required checks" became true rather than aspirational |
| [013](docs/adr/013-no-source-freshness-slo.md) | No source-freshness SLO — gate on what we control, warn on what we don't | A proposed third SLO was measured and rejected |
| [014](docs/adr/014-transform-before-load.md) | Transform before load — Bronze is the raw file, not a warehouse table | Bronze is a view, and the Bronze→Silver hop is honestly ETL |
| [015](docs/adr/015-slo2-population-is-complete-days.md) | SLO-2's population is complete days, chosen by the data | The publish lag is not a constant, so no fixed window works |
| [016](docs/adr/016-source-settling-horizon.md) | NYC 311 data settles after 7 days — the replica spread is a recency lag | SLO-2's real loss budget is 1.20%, not 0.24% |

</details>

<details>
<summary><b>Repository map</b></summary>

```
local/          the pipeline — ingest, pandas Silver (silver_transformations.py), DuckDB Gold
dbt/            Snowflake dbt project (models, snapshots, macros, tests)
airflow/dags/   nyc311_local.py — the 7-task DAG, smoke-tested green
terraform/      Snowflake foundation — 5 schemas, 4 roles, grant matrix (validated, not applied)
terraform/github/  this repo's own infrastructure — labels, branch protection, Pages (applied)
config/         borough_variants.csv — one mapping, read by Python and dbt alike
scripts/        SLO checks, claim checker, model-drift guard
docs/           ARCHITECTURE · SLO · CLAIMS · BACKLOG · adr/ · postmortems/
tests/          <!--claim:test_count-->255<!--/claim--> pytest tests across three tiers
```

</details>

---

**Marquis Carter** · Data Engineer
marq.dcarter@gmail.com · [LinkedIn](https://www.linkedin.com/in/marquis-c-45132325b/) · [GitHub](https://github.com/mcarter100k)
