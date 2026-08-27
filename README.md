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

# Action rate. The headline denominator is CLOSED requests, not all requests —
# and it has a known floor: rows whose resolution text the closure_type decoder
# could not read are is_actioned = FALSE, so measure that too rather than
# quoting the rate alone.
con.sql("""
    select count(*) filter (where is_actioned) * 1.0 / count(*)                as pct_actioned_of_closed,
           count(*) filter (where closure_type = 'Undecodable') * 1.0 / count(*) as pct_undecodable
    from gold.fct_service_requests
    where is_resolved
""").show()

# gold.fct_daily_volume publishes pct_actioned_within_window at (day, borough,
# category), with undecodable_closure_requests beside it for the same reason.
# Every rate on that table is NULL unless is_denominator_closed — a rate over
# requests created recently is right-censored, so it is suppressed rather than
# printed. On a 7-day --live mirror that is every row: a 14-day load cannot
# answer a 30-day question, and n/a is the honest answer.
con.sql("""
    select is_denominator_closed, count(*) as groups,
           count(pct_closed_within_window) as groups_publishing_a_rate
    from gold.fct_daily_volume group by 1
""").show()

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
`fct_service_requests` uses a three-valued flag: TRUE (closed in > 30 days), FALSE (closed in ≤ 30 days), NULL (still open). A boolean FALSE would cause `COUNT(*) FILTER (WHERE NOT is_overdue)` to count open requests as "on time" — silently inflating the resolution rate. NULL forces analysts to explicitly decide whether to include or exclude open requests, which is the correct default for a mixed-status fact table. The flag keys on `status`, not on whether a `closed_date` happens to be present — the source emits rows carrying a closed_date while still Open, and keying on the date alone let 4,139 of them be counted as "on time" by the very expression this design exists to protect. `assert_is_overdue_null_while_open` now enforces it.

**The source gate is a cost decision, and it is weaker than it looks.**
`check_source` runs a plain `curl` against the API before any downstream task starts, so a dead endpoint fails the run in seconds rather than part-way through a load. Failing early is close to free; failing late means a manual replay and an incident. But be precise about what it does and does not verify: it checks the HTTP status and **discards the body** (`-o /dev/null`), so a source returning `200` with an empty array — the published August 2026 stall — passes this gate cleanly. The zero-row check in `fetch_live_records` is what actually catches that, one task later. There is no sensor, no poke interval and no waiting; an earlier version of this README described all three, none of which exist.

**`FUTURE TABLES` grants interact with schema-swap publishing — and the interaction has to be designed, not assumed.**
`SELECT ON FUTURE TABLES` lets any table dbt creates inherit reporter permissions without a Terraform re-apply (`terraform/modules/snowflake-foundation/main.tf#"reporter_gold_future_tables"`). But Snowflake grants attach to the schema *object*, and the write-audit-publish swap renames objects — so grants defined only on GOLD stop covering it after the first publish. [ADR 009](docs/adr/009-publish-grants-under-schema-swap.md) resolves this: the grant matrix is specified symmetrically on both GOLD and GOLD_AUDIT, keeping the single atomic swap (the alternative — per-table view swaps — was rejected because it reintroduces the cross-table inconsistency window WAP exists to eliminate).

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
| **SLO-2 completeness** | rows we loaded vs rows the city actually **published**, for every day the load shows as **complete** | ≥ 98% on every such day. The source's per-day counts are captured at fetch time; the 2% absorbs documented quarantine and dedup removals *and* the source's settling lag |

The population is the load's own completeness verdict, not an offset from the clock: the source's publish lag is not a constant (23.3 h, 23.5 h, then 49.0 h measured within one week), so any fixed window is a whole day sometimes and a two-hour stub other times — [ADR 015](docs/adr/015-slo2-population-is-complete-days.md).

The 2% is mostly not ours. Socrata answers from two replicas, one behind the other by an amount that shrinks with a day's age and reaches zero at 7 days, so a day still settling can be reconciled against a fresher count than the load was served. Quarantine and dedup account for up to 0.24% of the budget; the settling lag accounts for up to 0.96% — [ADR 016](docs/adr/016-source-settling-horizon.md).

The executable queries live in [scripts/slo/](scripts/slo/); CI fails if `docs/SLO.md` and those files drift apart.

**Breach automation.** A failed run or SLO breach files a `daily-run-breach` GitHub issue with the measured numbers and run URL; a persisting breach comments on the open issue instead of duplicating it ([daily-run.yml](.github/workflows/daily-run.yml)). An issue beats an email: it is a tracked, assignable work item with history that a postmortem can link to.

**Watching the watcher.** The SLO check runs *inside* the daily run, so it cannot see the failure mode where no run happens at all — GitHub disables scheduled workflows on public repos after 60 days of inactivity, and cron delivery is best-effort. A separate 4-hourly [heartbeat](.github/workflows/heartbeat.yml) reads the Actions API from outside and files into the same issue stream if `daily-run.yml` is disabled or has not concluded successfully on `main` within 26 hours — the same threshold as SLO-1, measured from the other side.

**A real incident.** On 2026-08-18 the city's publish process left Aug 17 ~96% incomplete, then published nothing for 21+ hours. Every pipeline stage ran green; only the source-facing check saw it, on the tier's first scheduled day. SLO-2 detected it and auto-filed [issue #7](https://github.com/mcarter100k/nyc311-data-platform/issues/7). The control that followed: SLO-2 was **redefined as a source reconciliation** — it now asks whether we loaded everything the city published, so an upstream outage no longer reddens our reliability signal, while a separate non-gating [upstream-stall check](scripts/check_upstream_stall.py) keeps the outage visible. [Full postmortem](docs/postmortems/2026-08-18-upstream-publish-stall.md).

**A self-audit finding, and the fix is not the obvious one.** The ingestion watermark keyed on `created_date`, fetching each record exactly once — on the day it was filed. But 311 requests mutate after creation (status flips to Closed days later), so every downstream update path was unreachable and resolution metrics would only ever have counted same-day closures. Found by systematic self-audit, not by a failure.

The obvious repair — key on `:updated_at` — was measured and **rejected**: that field is mass re-stamped nightly, ~540k rows/day against ~53k created per week ([ADR 010](docs/adr/010-scheduled-operation.md)), which a row-capped daily fetch cannot absorb. The daily run instead re-pulls a trailing 7-day `created_date` window in full, so status changes *inside* that window are captured. Updates to rows older than the window are still missed, and that limit is tracked in [docs/BACKLOG.md](docs/BACKLOG.md) rather than papered over. An earlier version of this README claimed the watermark now keys on `:updated_at`; it does not, and the only caller passes `created_window`.

---

## What is real, and what is deferred

The distinction is enforced, not asserted: [scripts/check_claims.py](scripts/check_claims.py) fails CI when this README **or anything under docs/** drifts from the repo — counts against the code that produces them, documented DAG task names against the DAG, the dbt model inventory against the parsed manifest, every relative link and fragment against the tree, and every citation against a unique string in the file it names. [docs/CLAIMS.md](docs/CLAIMS.md) maps every claim to the code that enforces it and the test that verifies it, and both of those columns are now checked too.

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

Gold is a Kimball star: <!--claim:fct_models-->4<!--/claim--> fact tables, <!--claim:dim_models-->3<!--/claim--> dimensions, and a 21-year calendar spine (2010–2030). Terraform provisions the Snowflake side — 5 schemas (including `GOLD_AUDIT` for write-audit-publish and `SNAPSHOTS` for SCD2 state), 4 roles, and a least-privilege grant matrix enforced as code.

**Full detail:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — per-layer contracts, the dbt model inventory, the stack table, and what each layer guarantees.

---

## Test Suite

Two populations, deliberately not summed: **<!--claim:test_count-->237<!--/claim--> pytest tests** that need no cloud account, and **<!--claim:dbt_test_count-->132<!--/claim--> dbt data tests (<!--claim:dbt_generic_tests-->121<!--/claim--> generic + <!--claim:dbt_singular_tests-->11<!--/claim--> singular)** that run against the warehouse during `dbt build`. Both counts are recomputed in CI — the pytest one from collection, the dbt one from the parsed manifest. The dbt figure was a bare literal until 2026-08-26 and had rotted twice through merges, silently, while every marker-guarded number beside it stayed correct.

| Tier | Count | What it proves |
|---|---|---|
| **Structural** | <!--claim:structural_test_count-->165<!--/claim--> | Configuration correctness — schema resolution, incremental strategy, DAG lineage, freshness target, Terraform validity, the LOADER-has-no-TRUNCATE contract, a relationships test on every fact foreign key. Also the documentation guards' own failure modes ([tests/test_doc_guards.py](tests/test_doc_guards.py)): each check in `scripts/check_claims.py` is exercised against a synthetic tree with the thing it guards broken, because a check that cannot fail reports green and is read as evidence — this repo has shipped three of those by accident |
| **Unit** | <!--claim:unit_test_count-->9<!--/claim--> | Silver transformation *logic* ([local/silver_transformations.py](local/silver_transformations.py)) against hand-built fixtures |
| **Behavioral** | <!--claim:behavioral_test_count-->63<!--/claim--> | Gold *semantics* — builds the dbt project twice on seeded DuckDB and asserts on output rows: watermark lookback, SCD2 point-in-time join, update propagation, and dimension retention when Silver's rolling window moves past a member the fact still references. Also import health for every module in [local/](local/), which needs the real runtime dependencies this tier installs |

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
| [015](docs/adr/015-slo2-population-is-complete-days.md) | SLO-2's population is complete days, chosen by the data — not an offset from the clock | The publish lag is not a constant, so no fixed window works; the gate reads `int_load_completeness` and zero is never a pass |
| [016](docs/adr/016-source-settling-horizon.md) | NYC 311 data settles after 7 days — the replica spread is a recency lag, not noise | The 17% spread is one replica running behind, not noise; SLO-2's real loss budget is 1.20%, not 0.24% |

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

**There is no cloud deployment runbook, and this line used to promise one.** It said the
steps were in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md); the link resolved, so the
link checker passed, and the steps were not there. What exists is the Terraform module
under [terraform/](terraform/) — validated in CI, never applied — and the open decision
about how Silver would reach Snowflake at all ([ADR 008](docs/adr/008-prototype-scope.md)).
Writing a runbook for a path nobody has walked would be the same kind of claim this repo
spends a checker removing.

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
docs/           ARCHITECTURE · SLO · CLAIMS · BACKLOG · adr/ (<!--claim:adr_count-->15<!--/claim-->) · postmortems/
tests/          <!--claim:test_count-->237<!--/claim--> pytest tests across three tiers
```

---

## Contact

**Marquis Carter**
Data Engineer
marq.dcarter@gmail.com
[LinkedIn](https://www.linkedin.com/in/marquis-c-45132325b/) · [GitHub](https://github.com/mcarter100k)
