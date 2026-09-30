# NYC 311 Data Platform

[![CI](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/ci.yml)
[![Terraform](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/terraform.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/terraform.yml)
[![Daily Live Run](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/daily-run.yml/badge.svg)](https://github.com/mcarter100k/nyc311-data-platform/actions/workflows/daily-run.yml)

A data pipeline over New York City's 311 service requests (the city's non-emergency complaint line). Every day it downloads recent requests from the city's open-data API, cleans them, builds analysis tables, and checks two written promises about the result. When a promise is broken, it opens a GitHub issue. It also runs on a laptop in a couple of minutes, with no cloud account.

|  |  |
|---|---|
| **Stack** | Python · pandas · DuckDB · dbt · Airflow · Terraform · GitHub Actions |
| **Model** | Star schema: <!--claim:fct_models-->4<!--/claim--> fact tables, <!--claim:dim_models-->3<!--/claim--> dimensions |
| **Scale** | ~385k requests re-fetched daily (the last 37 days); ~500k accumulated since Aug 2026 |
| **Tests** | <!--claim:test_count-->223<!--/claim--> pytest tests + <!--claim:dbt_test_count-->131<!--/claim--> dbt data tests |
| **Runs** | Daily (cron 10:00 UTC; GitHub usually starts it 3–8 hours late), gated by 2 SLOs |
| **Decisions** | <!--claim:adr_count-->16<!--/claim--> decision records and a postmortem |

## What it does

The pipeline uses the **medallion** pattern: data passes through three layers, each with one job. **Bronze** is the data exactly as the source sent it. **Silver** is the same data cleaned. **Gold** is the data shaped for analysis as a **star schema**: one central *fact* table (one row per request) joined to *dimension* tables that describe it (agency, date, location). When a Gold number looks wrong, check Silver, then Bronze; two queries find the layer that broke.

The source is NYC Open Data's **Socrata** API. Python and pandas build Bronze and Silver. **dbt**, a tool that builds tables from version-controlled SQL and tests them, builds Gold. Everything lives in **DuckDB**, a database stored in one file. A Snowflake version of the warehouse is written and validated but has never been deployed.

## How data flows

```
Socrata API ─► raw JSON (Bronze) ─► pandas (Silver) ─► dbt (Gold) ─► SLO checks
```

1. **Fetch.** [`local/local_runner.py`](local/local_runner.py) downloads every request created in the last 37 days into one JSON file, and asks the city how many requests it holds for each day. Zero rows, or more than 800,000, fails the run.
2. **Bronze.** DuckDB puts a view over the raw file. Nothing is copied.
3. **Silver.** pandas removes duplicates, maps 24 spellings of borough names to five, and sets aside ("quarantines") rows closed before they were created.
4. **Gold.** `dbt build` builds the star schema and runs every dbt test. The fact table is **incremental**: each run inserts new requests and updates changed ones, so Gold keeps every request ever loaded.
5. **Check.** Two SQL queries test the result against the SLOs.

**Why 37 days.** The pipeline only sees a request's status change while the request is inside the fetch window, because it only sees what it re-fetches. NYC's closure standard is 30 days, and the city's data for a day keeps shifting for about 7 more ([ADR 016](docs/adr/016-source-settling-horizon.md)), so the window is 30 + 7. A request older than 37 days keeps the status it had when it left the window: fine for 30-day metrics, wrong for longer ones ([ADR 010](docs/adr/010-scheduled-operation.md)). Fetching whatever changed (`:updated_at`) was measured and rejected: the city re-stamps about half a million rows a night.

Layer detail, the model list and the design trade-offs: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## How it is operated and checked

An **SLO** (service level objective) is a written, measured promise. [`daily-run.yml`](.github/workflows/daily-run.yml) runs the pipeline daily, then checks two:

| SLO | Question | Threshold |
|---|---|---|
| **SLO-1 freshness** | Did a run recently load rows? | newest load < 26 hours old |
| **SLO-2 completeness** | Did we load what the city published? | ≥ 98% of the city's own count, on every day the load shows as complete |

A failed run or broken SLO opens a `daily-run-breach` issue with the measured numbers. [docs/SLO.md](docs/SLO.md) explains the thresholds. Two more checks cover what the SLOs cannot see:

- **Upstream stall warning.** If the city stops publishing, the run stays green (the loss is not ours) but an `upstream-stall` issue opens. When the city's publishing stalled on 2026-08-18, every pipeline stage ran green and only a source-facing check noticed ([postmortem](docs/postmortems/2026-08-18-upstream-publish-stall.md)).
- **Heartbeat.** A check inside the daily run cannot report a run that never starts. A separate [heartbeat](.github/workflows/heartbeat.yml), scheduled every 4 hours (GitHub may delay or skip it), alerts if the daily run is disabled or has not succeeded in 30 hours. Not 24: GitHub starts the daily run late, and gaps between healthy runs reach 27 hours.

**Tests.** <!--claim:test_count-->223<!--/claim--> pytest tests run in CI as three required jobs, split by what each needs installed: <!--claim:structural_test_count-->143<!--/claim--> structural (dbt config, the Airflow DAG's task order, Terraform grants, workflows, the docs checker), <!--claim:unit_test_count-->9<!--/claim--> unit (the pandas cleaning), and <!--claim:behavioral_test_count-->71<!--/claim--> behavioral (real dbt builds on seeded data, the SLO queries, the fetcher against a fake API). A model can be configured perfectly and still compute the wrong number, which is why the behavioral tier checks output rows. Any skipped test fails its job, because a skip shows green. Separately, <!--claim:dbt_test_count-->131<!--/claim--> dbt tests (<!--claim:dbt_generic_tests-->121<!--/claim--> generic, <!--claim:dbt_singular_tests-->10<!--/claim--> hand-written) check the data inside every build.

**Docs checked against code.** [`scripts/check_claims.py`](scripts/check_claims.py) fails CI when this README or `docs/` disagrees with the repo: counts, DAG task names, the model list, links, and quoted code. [docs/CLAIMS.md](docs/CLAIMS.md) maps each claim to its code and test. The checks are themselves tested by breaking what they guard ([tests/test_doc_guards.py](tests/test_doc_guards.py)).

**Real versus specified.** What runs: the DuckDB pipeline, the daily run, an Airflow demo (a 7-task DAG), three required CI checks, and Terraform for this repo's own GitHub settings ([terraform/github/](terraform/github/), applied). What is only specified: a Snowflake warehouse with 5 schemas, 4 roles and least-privilege grants ([terraform/](terraform/), validated in CI, never applied), and **write-audit-publish**, where dbt builds and tests in an audit schema and then swaps it into Gold so readers never see untested data ([ADR 005](docs/adr/005-orchestration-strategy.md)). The DuckDB path builds straight into Gold: a failed dbt test fails the run but leaves the new tables in place, and the next run starts from the last good database.

## What the data shows

Measured on the 127,255 requests created in the twelve complete days **13–24 Aug 2026**.

**"Closed" usually does not mean "fixed."** Of 89,506 closures, between **35% and 44%** describe the city doing something. The rest closed as *no violation found*, *nothing there*, duplicate, or handed off. It is a range because 8.5% of closures carry text no rule can classify, and per category the width is itself the finding:

| Category | Actioned | Uncertainty |
|---|---|---|
| Illegal Parking | 42–43% | 0.7pp |
| Noise | 40–41% | 1.5pp |
| **Homeless Services** | **17–20%** | 3.6pp |
| Street Condition | 28–46% | 17.9pp |
| Water & Sewer | 17–40% | 22.3pp |

**A closure the city couldn't complete is the one that comes back.** After an *Access Failed* closure, the same complaint returns to the same address within 3 days 13.8% of the time. That ranks first in **all eight** specifications tried (windows of 2–5 days, with and without chronic locations), 1.1–5.8 points ahead of the runner-up.

<details>
<summary><b>Methodology, and what does not survive it</b></summary>

Absolute recurrence rates do not travel: *Work Performed* ranges from 4.2% to 15.8% on the same data depending only on the window and whether chronic locations are included. Only the Access Failed *ranking* is stable, and only among closure types whose text could be decoded. A repeat is evidence, not proof: the fix may have failed, or the condition may be legal and keep being reported. Both guards are columns on [`fct_complaint_recurrence`](dbt/models/marts/fct_complaint_recurrence.sql).

3-day recurrence, every decoded closure type with at least 200 closures:

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

Volume: weekdays average 10,955 requests a day and weekends 9,903, while noise complaints more than double at weekends (1,730 → 3,589 a day).

</details>

**What was corrected.** A claim that "nothing there" closures recur least was withdrawn: it came from 7 days of data and reversed on 12. A weekday/weekend volume comparison was reported backwards (both totals had been divided by the same number of days). Both are registered in the claim checker so they cannot return. And the daily run first fetched only 7 days, so Gold never saw a request close after day 7: for requests created 24–28 Aug, the 30-day closure rate read 68.4% instead of 89.5%. The window is now 37 days; rows stored before the change keep their old status until one wide run (`--live --days N`, N larger than their age) refreshes them.

## Run it

Needs Python 3.11+ and internet access; no credentials. Run from the repo root.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r local/requirements.txt -r dbt/requirements.txt -r requirements-dev.txt

python local/local_runner.py            # 10,000 newest requests, all 5 stages (1-2 min, mostly download)
python local/local_runner.py --live     # what the daily run does: 37 days, ~385k rows, ~8 min
python local/local_runner.py --stage 4  # resume from a stage (1 fetch ... 5 print results)
python local/reconcile.py               # check Gold against the raw file and the live API
./run_tests.sh                          # rebuild the dbt manifest, run every pytest tier
python scripts/check_claims.py          # the docs-vs-code check
```

Three requirements files, because the pipeline runs dbt on DuckDB while the tests parse the Snowflake project. `reconcile.py` checks what tests cannot: it accounts for every fetched row across layers, recomputes key numbers from the raw JSON without DuckDB or dbt, and compares a sample against the live API. The Airflow demo has its own setup in [`scripts/airflow_local.sh`](scripts/airflow_local.sh).

<details>
<summary><b>Query the data and reproduce the findings</b></summary>

`pip install duckdb` gives the Python library, not the `duckdb` command-line tool, so query from Python:

```python
import duckdb
con = duckdb.connect("local/data/nyc311_local.duckdb", read_only=True)

# Share of closed requests the city acted on, and the share it could not decode.
con.sql("""
    select count(*) filter (where is_actioned) * 1.0 / count(*)                 as pct_actioned_of_closed,
           count(*) filter (where closure_type = 'Undecodable') * 1.0 / count(*) as pct_undecodable
    from gold.fct_service_requests
    where is_resolved
""").show()

# Closure rates stay NULL until 30 complete days follow a day (is_denominator_closed).
con.sql("""
    select is_denominator_closed, count(*) as groups,
           count(pct_closed_within_window) as groups_publishing_a_rate
    from gold.fct_daily_volume group by 1
""").show()

# 3-day recurrence, excluding chronic locations and closures too recent to judge.
con.sql("""
    select closure_type,
           avg(case when days_to_next_same_complaint <= 3 then 1.0 else 0.0 end) as recurred_3d
    from gold.fct_complaint_recurrence
    where not is_chronic_location and observation_days >= 3
    group by 1 order by 2 desc
""").show()
```

Expect different numbers: `--live` fetches the 37 days ending today, not 13–24 Aug. On a fresh `--live` database, closure rates publish only for the oldest five or so days of the window; the rest stay NULL. Only the Access Failed ranking has held across specifications.

</details>

## Architecture Decision Records

<details>
<summary><b>All <!--claim:adr_count-->16<!--/claim--> decisions</b></summary>

| ADR | Decision |
|---|---|
| [001](docs/adr/001-warehouse-selection.md) | Snowflake over Databricks SQL for the serving layer |
| [002](docs/adr/002-transformation-tool.md) | dbt over hand-written transform code for Gold |
| [003](docs/adr/003-iac-approach.md) | Terraform as the one infrastructure-as-code tool |
| [004](docs/adr/004-medallion-vs-elt.md) | Medallion layering over direct ELT |
| [005](docs/adr/005-orchestration-strategy.md) | Airflow, one DAG, write-audit-publish dbt stage (Snowflake only; not used on the DuckDB path) |
| [006](docs/adr/006-schema-evolution.md) | A schema version stamp on each fact row, not runtime column detection |
| [007](docs/adr/007-scd-type-2-dim-agency.md) | Agency history as SCD Type 2 via a dbt snapshot, so a 2021 request keeps its 2021 agency name |
| [008](docs/adr/008-prototype-scope.md) | Cloud services specified, not provisioned |
| [009](docs/adr/009-publish-grants-under-schema-swap.md) | Reader grants on both GOLD and GOLD_AUDIT, so the publish swap keeps access |
| [010](docs/adr/010-scheduled-operation.md) | Run daily against the live source, with written SLOs |
| [011](docs/adr/011-parallel-ci-tiers.md) | Three parallel required CI checks, not one sequential job |
| [012](docs/adr/012-github-repo-as-code.md) | This repository's own settings are Terraform, and applied |
| [013](docs/adr/013-no-source-freshness-slo.md) | No source-freshness SLO: gate on what we control, warn on what we don't |
| [014](docs/adr/014-transform-before-load.md) | Transform before load: Bronze is the raw file, not a warehouse table |
| [015](docs/adr/015-slo2-population-is-complete-days.md) | SLO-2 checks the days the data shows are complete, not a fixed offset from today |
| [016](docs/adr/016-source-settling-horizon.md) | NYC 311 data settles after 7 days |

</details>

---

**Marquis Carter** · Data Engineer
marq.dcarter@gmail.com · [LinkedIn](https://www.linkedin.com/in/marquis-c-45132325b/) · [GitHub](https://github.com/mcarter100k)
