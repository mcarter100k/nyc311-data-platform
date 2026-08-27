# Postmortem: Upstream publish stall left the source ~96% incomplete for Aug 17

**Date of incident:** 2026-08-18
**Date written:** 2026-08-18
**Date finalized:** 2026-08-20
**Status:** reviewed — source backfilled, and the first scheduled run under the redesigned control passed
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
| 2026-08-19 10:22 | Scheduled run **failed red** — SLO-2 (still the old median-based definition) measured `rows_yesterday=319` against a median of 10,449.5. Recovery was not yet visible to the gate at this point, and this run is the reason the redesign had not yet been exercised |
| 2026-08-19 (later) | Source resumed publishing. The missing days filled in: Aug 17 **410 → 10,473**, Aug 18 **0 → 10,833**. No action was taken by this project; recovery was entirely upstream, and the trailing 7-day fetch window absorbed both days automatically on the next run |
| 2026-08-20 03:22 | Redesigned SLO-2 (source reconciliation) and the non-gating upstream-stall warning merged to `main` ([#24](https://github.com/mcarter100k/nyc311-data-platform/pull/24), 03:22:50Z) — after the failed run above, which is why 2026-08-20 is the first run to exercise them |
| 2026-08-20 10:24 | First scheduled run under the redesigned control. **Green.** SLO-1 `age_hours=0` (threshold 26); SLO-2 `rows_loaded_yesterday=372` vs `rows_published_by_source=372` — a 100% reconciliation; `dbt build` PASS=124 ERROR=0. The upstream-stall warning fired on the volume cliff (`rows_yesterday=372`, `median_prior_7d=10494.5`, floor 0.40) and filed issue [#40](https://github.com/mcarter100k/nyc311-data-platform/issues/40) without reddening the run |

## Did the control work

> **Correction, 2026-08-27.** The section below concluded that it did, on the
> strength of a `372 / 372` reconciliation. That evidence was weaker than it
> reads. Aug 19 eventually held **10,701** rows; 372 was the ~2-hour stub the
> source's publish lag leaves in T-1 at 10:00 UTC. The control therefore
> certified **3.5%** of that day and reported it as a pass — and on a day where
> the capture returned zero, `slo2_completeness.sql`'s `WHEN source = 0 THEN
> true` branch would have passed against nothing at all. The reasoning below
> about *which question to ask* stands and is unchanged; the claim that this run
> demonstrated the control working does not. SLO-2's population was rebuilt on
> 2026-08-27 — see [ADR 015](../adr/015-slo2-population-is-complete-days.md).
>
> The same correction applies to the upstream-stall warning noted in the
> recovery timeline: it did not fire because Aug 19 was anomalous. It fired
> because the stub day is what T-1 always looks like, and it went on firing
> every day thereafter.

This is the part worth being precise about, because the redesign was made on
one incident's evidence and this was its first real exercise.

Aug 19 published **372** rows against a trailing median of **10,494.5** — a day
shaped exactly like the incident. Under the SLO-2 that existed during the
incident (yesterday ≥ 40% of the trailing median), that day computes to **3.5%
of median and breaches**, filing a `daily-run-breach` issue and turning the run
red. Under SLO-2 as reconciliation, it computes 372/372 and **passes**.

Both readings are of the same day, and the second one is correct: the pipeline
loaded every row the city published. The first would have reported a pipeline
failure that did not happen.

What the incident actually taught, then, was not "raise the threshold" but
*"the question was wrong."* Volume-against-history answers **did the city
publish normally**, which is not something this pipeline can control or fix.
Reconciliation answers **did we load what the city published**, which is. The
first question still matters to anyone reading the dashboards, so it survives —
as a warning that files a tracked issue and leaves the run green. That split is
now the standing rule, recorded in [ADR 013](../adr/013-no-source-freshness-slo.md):
**gate on what we control, warn on what we don't.**

Worth stating plainly: a green run with an open `upstream-stall` issue is the
designed outcome here, not a compromise. The alternative — a red build for a
fault in someone else's publishing schedule, recurring daily — trains the
operator to ignore red builds, and that costs more than the visibility it buys.

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

**`scripts/check_upstream_stall.py`** — and naming the right file matters,
because it is not the one that caught the incident.

What detected this in August was `scripts/slo/slo2_completeness.sql` in its
*then* form, which asked whether the city had published a normal volume. It
fired twice, and the measured numbers are preserved on issue #7 with the
breach-run artifacts retained 14 days.

That file still exists, but it no longer detects this failure mode and is not
supposed to: it now asks whether *we* loaded everything the city published, and
answers 372/372 — a pass — on exactly this incident class. The volume question
moved wholesale to `check_upstream_stall.py`, which warns without gating.

So the detector changed identity. Anyone reading this postmortem for "what
would catch it next time" needs the stall checker, not the SLO file that
carries the incident's history. See **Did the control work** above for the first
scheduled exercise of the new arrangement, which produced a green run and a
tracked `upstream-stall` issue on the same day — the outcome the split was
designed for.

## Follow-ups

| Action | Tracked in | Done |
|---|---|---|
| Close #7 when the source backfills and a scheduled run passes; finalize this postmortem (status → reviewed, add recovery timeline) | #7 | ✓ postmortem finalized 2026-08-20. **#7 was closed early**: at 08:25:07Z, ~2h before the qualifying run finished at 10:25:40Z. Both conditions did hold by 10:25, but not at the moment of closing — recorded rather than quietly corrected, because a criterion that gets marked met before it is met is exactly the failure this column exists to prevent |
| Decide on SLO-3 (source freshness: max `created_date` in Gold within N hours) — closes the SLO-1 blind spot named above | [ADR 013](../adr/013-no-source-freshness-slo.md) | ✓ rejected after measurement — the blind spot is closed by a warning, not a gate |
| Revisit SLO-2's window (T-1 vs T-2) only if several *normal* post-recovery days show T-1 chronically incomplete at 10:00 UTC — threshold changes require accumulated evidence, not one incident | [BACKLOG](../BACKLOG.md), [ADR 015](../adr/015-slo2-population-is-complete-days.md) | ✓ resolved 2026-08-27 — and the framing was wrong. A third measurement put the publish lag at **49.0 h** against the earlier 23.3 h and 23.5 h, so the lag is not a constant and T-2 is a stub on some days too. The window is no longer an offset from the clock at all: SLO-2's population is every day `int_load_completeness` marks complete |
| Dataset-split claims correction (README, sources.yml, ADR notes) | shipped in the same PR as this postmortem | ✓ |
