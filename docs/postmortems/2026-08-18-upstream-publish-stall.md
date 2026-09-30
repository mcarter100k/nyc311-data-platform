# Postmortem: Upstream publish stall left the source ~96% incomplete for Aug 17

**Date of incident:** 2026-08-18
**Date written:** 2026-08-18
**Date finalized:** 2026-08-20 (corrected 2026-08-27)
**Status:** reviewed — the source backfilled on its own; the control redesigned in response was itself corrected later (see *Did the control work*)
**Breach issue:** [#7](https://github.com/mcarter100k/nyc311-data-platform/issues/7)
**Severity:** SLO breach (upstream data incident — no pipeline defect)

Blameless: this document names causes and defenses, never people. If a step
was error-prone enough for a careful person to get wrong, the step is the
finding.

## Timeline (UTC, all 2026-08-18)

| Time | What happened |
|---|---|
| 01:44 | Dataset metadata `rowsUpdatedAt` — the last publish the source reports |
| 04:33 | Local verification run: Aug-17 shows **9,119** created rows; both SLOs pass |
| 05:28 | Manual `workflow_dispatch` (ADR 010 verification): fetch and build green, **SLO-2 breach — `rows_yesterday=0`** vs median 10,508; issue #7 auto-filed with the numbers |
| ~05:35 | First hypothesis recorded on #7: dispatch ran inside NYC's ~00:00–02:00 ET refresh window; expected the scheduled run to pass |
| 10:22 | First **scheduled** run: fetch and build green, **SLO-2 breach — `rows_yesterday=410`** vs median 10,523; monitor commented on the existing issue (no duplicate filed). Hypothesis falsified |
| 22:58 | Source measured directly: Aug 15 = 9,535, Aug 16 = 9,134, **Aug 17 = 410, Aug 18 = 0**; `rowsUpdatedAt` still 01:44 — no publish in 21+ hours. Diagnosis revised: upstream incident |

## Recovery (UTC)

| Date | What happened |
|---|---|
| 2026-08-19 10:22 | Scheduled run **failed red**: SLO-2, still the old median-based check, measured `rows_yesterday=319` against a median of 10,449.5 |
| 2026-08-19 (later) | Source resumed publishing. Aug 17 filled in **410 → 10,473** and Aug 18 **0 → 10,833**. Recovery was entirely upstream; the fetch window (7 days at the time) picked up both days on the next run |
| 2026-08-20 03:22 | Redesigned SLO-2 (source reconciliation) and the non-gating upstream-stall warning merged to `main` ([#24](https://github.com/mcarter100k/nyc311-data-platform/pull/24)) |
| 2026-08-20 10:24 | First scheduled run under the redesign. **Green.** SLO-1 `age_hours=0` (threshold 26); SLO-2 reconciled 372 loaded against 372 published (a two-hour stub; see below); `dbt build` PASS=124 ERROR=0. The upstream-stall warning fired (`rows_yesterday=372`, `median_prior_7d=10494.5`, floor 0.40) and filed issue [#40](https://github.com/mcarter100k/nyc311-data-platform/issues/40) without reddening the run |

## Did the control work

*Corrected 2026-08-27.* This section first concluded that the 2026-08-20 run
showed the redesigned SLO-2 working. It did not. The lesson about which
question to ask still stands.

The run reconciled Aug 19 at **372 / 372** and passed. Aug 19 eventually held
**10,701** rows: 372 was the roughly two-hour stub that the source's publish
lag leaves in the previous day at run time, so the gate certified **3.5%** of
the day. On a day the source's own count came back as zero, SLO-2's query
(`WHEN source = 0 THEN true`) would have passed with nothing loaded at all. The
upstream-stall warning fired on the same stub, not because Aug 19 was abnormal:
the previous day always looks like that at run time, so the warning fired on
every run until 2026-08-27. That day both checks were rebuilt to judge only
days the load shows as complete
([ADR 015](../adr/015-slo2-population-is-complete-days.md)).

What the incident did teach is which question to ask. Volume against history
asks **did the city publish normally**, which this pipeline cannot control or
fix. Reconciliation asks **did we load what the city published**, which it can.
The first question still matters to anyone reading the dashboards, so it
survives as a warning that files a tracked issue and leaves the run green. That
split is the standing rule, recorded in
[ADR 013](../adr/013-no-source-freshness-slo.md): **gate on what we control,
warn on what we don't.**

A green run with an open `upstream-stall` issue is the designed outcome, not a
compromise. A red build for a fault in someone else's publishing schedule,
recurring daily, trains the operator to ignore red builds.

## Detection

SLO-2 (then: yesterday's count ≥ 40% of the trailing-7-day median), evaluated by
the daily workflow. It fired on both runs that day: the manual run at 05:28 and
the first scheduled run at 10:22. Every pipeline stage was green both times;
only the source-facing check saw the problem. Without it, the run would have published
a Gold layer missing ~96% of the day and reported success.

## Root cause

**Observed:** at ~01:44 UTC the source's publish process replaced the dataset
with a version holding 410 of Aug 17's ≈9,100+ rows and none of Aug 18's, then
published nothing for at least 21 hours, on a dataset whose page states
*Update Frequency: Daily*.

**Inferred (unknowable from outside):** the internal mechanism. The
observations fit a wholesale nightly rebuild that regressed recent days, the
same publish style implied by the mass `:updated_at` re-stamping measured in
ADR 010 (~540k rows re-touched nightly). One inconsistency: content changed
between 04:33 and 05:28 while `rowsUpdatedAt` stayed 01:44, which suggests a
multi-step rebuild or replicas lagging the metadata.

**Context, possibly related:** in Dec 2025 the city split this dataset
(2010–2019 moved out; erm2-nwe9 became "2020 to Present"; corrected in the
claims shipped with this postmortem). The process behind it rewrites the
dataset wholesale; this incident is that process failing partway.

## Contributing factors

- **SLO-1 is blind to source staleness by design:** it measures our
  `_loaded_at`, which is minutes old after any successful run, so detection
  rested entirely on SLO-2. A source-freshness SLO was proposed here and later
  rejected ([ADR 013](../adr/013-no-source-freshness-slo.md)).
- **The first diagnosis anchored on run timing** (dispatch inside the refresh
  window). It was recorded as a hypothesis with a stated falsification
  condition and falsified by the next scheduled run, at a cost of ~5 hours of
  misattribution.
- The monitor's issue dedup worked (one issue, appended comments), which kept
  the investigation in one place.

## What now detects this

**`scripts/check_upstream_stall.py`**, a warning that never gates.

What caught the incident was `scripts/slo/slo2_completeness.sql` in its old
form, which asked whether the city had published a normal volume. It fired
twice; the numbers are on issue #7. That file now asks whether *we* loaded
everything the city published, and passes on a stall like this one by design.
The volume question moved to the stall checker, which judges the newest
complete day against the source's own counts (ADR 015).

## Follow-ups

| Action | Tracked in | Done |
|---|---|---|
| Close #7 when the source backfills and a scheduled run passes; finalize this postmortem | #7 | ✓ finalized 2026-08-20. #7 was closed at 08:25Z, about two hours before the qualifying run finished at 10:25Z. Both conditions held by 10:25, but not when it was closed. Recorded because marking a criterion met before it is met is the failure this column exists to prevent |
| Decide on SLO-3 (source freshness: max `created_date` in Gold within N hours) | [ADR 013](../adr/013-no-source-freshness-slo.md) | ✓ rejected after measurement: the blind spot is covered by a warning, not a gate |
| Revisit SLO-2's window (T-1 vs T-2) if normal days show T-1 chronically incomplete at run time | [ADR 015](../adr/015-slo2-population-is-complete-days.md) | ✓ resolved 2026-08-27, and the framing was wrong: the publish lag measured 23.3 h, 23.5 h, then 49.0 h, so no fixed offset works. SLO-2's population is now every day `int_load_completeness` marks complete |
| Dataset-split claims correction (README, sources.yml, ADR notes) | shipped with this postmortem | ✓ |
