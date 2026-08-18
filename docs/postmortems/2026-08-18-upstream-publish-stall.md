# Postmortem: Upstream publish stall left the source ~96% incomplete for Aug 17

**Date of incident:** 2026-08-18
**Date written:** 2026-08-18
**Status:** draft — incident ongoing at time of writing; finalize when the source backfills and a scheduled run passes
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

## Detection

SLO-2 (completeness: yesterday ≥ 40% of the trailing-7-day median), evaluated
by the daily workflow. Detected on the first evaluation after the stall —
including on the tier's very first scheduled day of operation. Every stage of
the pipeline itself was green both times; only the source-facing check saw
the problem. A pipeline without it would have published a Gold layer missing
~96% of the day and reported success.

## Root cause

**Observed:** the source's publish process replaced the dataset at ~01:44 UTC
with a version containing 410 of Aug 17's ≈9,100+ rows and none of Aug 18's,
and published nothing further for at least 21 hours — on a dataset whose own
page states *Update Frequency: Daily*.

**Inferred (unknowable from outside):** the exact internal mechanism. The
observations are consistent with a wholesale nightly rebuild that regressed
recent days — the same publish style implied by the mass `:updated_at`
re-stamping measured in ADR 010 (~540k rows re-touched nightly). One
inconsistency is noted honestly: content visibly changed between 04:33 and
05:28 while `rowsUpdatedAt` stayed 01:44, suggesting a multi-step rebuild or
replica lag behind the metadata.

**Context, possibly related:** the city restructured this dataset in Dec 2025
(split 2010–2019 out; erm2-nwe9 became "2020 to Present" — see the claims
correction shipped alongside this postmortem). The publish pipeline behind it
demonstrably rewrites the dataset wholesale; this incident is that process
failing partway.

## Contributing factors

- **SLO-1 is blind to source staleness by design:** it measures our
  `_loaded_at`, which is minutes old after any successful run. Detection
  rested entirely on SLO-2. A source-freshness commitment (max `created_date`
  age in Gold) would name this failure mode directly.
- **The first diagnosis anchored on run timing** (dispatch inside the refresh
  window) — plausible, recorded as a hypothesis with a stated falsification
  condition, and falsified by the next scheduled run five hours later. The
  process worked; the anchor cost ~5 hours of misattribution.
- The monitor's issue-dedup behaved correctly (one issue, appended comments),
  keeping the investigation trail in a single place.

## What now detects this

`scripts/slo/slo2_completeness.sql`, executed by
`.github/workflows/daily-run.yml` — proven by this incident, twice, with the
measured numbers preserved on issue #7 and the breach-run artifacts retained
14 days.

## Follow-ups

| Action | Tracked in | Done |
|---|---|---|
| Close #7 when the source backfills and a scheduled run passes; finalize this postmortem (status → reviewed, add recovery timeline) | #7 | |
| Decide on SLO-3 (source freshness: max `created_date` in Gold within N hours) — closes the SLO-1 blind spot named above | new issue on decision | |
| Revisit SLO-2's window (T-1 vs T-2) only if several *normal* post-recovery days show T-1 chronically incomplete at 10:00 UTC — threshold changes require accumulated evidence, not one incident | #7 | |
| Dataset-split claims correction (README, sources.yml, ADR notes) | shipped in the same PR as this postmortem | ✓ |
