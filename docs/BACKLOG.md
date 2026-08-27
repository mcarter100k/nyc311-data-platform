# Backlog

Known issues and follow-ups that are real but not urgent. Each entry states the
evidence, the risk, and the options — so a future session can act without
re-deriving the analysis. Items graduate to a PR or an ADR; they do not sit
here as vague intentions.

---

## ~~Borough standardization is duplicated across Silver and dbt, and the copies have drifted~~ — RESOLVED 2026-08-20

**Found:** 2026-08-19, while explaining the layer boundaries.

**Evidence.** Both layers standardize borough independently:

| Layer | Where | Variants known |
|---|---|---|
| Silver | `local/silver_transformations.py` | 24 — includes `KINGS COUNTY`, `NY`, `QUEENS COUNTY`, `BRONX COUNTY`, `RICHMOND` |
| dbt | `int_service_requests_cleaned.sql` Step 1 | 19 — **missing all five of the above** |

Silver runs first and **overwrites** the `borough` column with its normalized
value, so what dbt receives is only the six canonical outputs (verified against
the live database: `BROOKLYN`, `QUEENS`, `BRONX`, `MANHATTAN`, `STATEN ISLAND`,
`UNSPECIFIED`). The dbt CASE therefore never sees a raw variant — it is a
pass-through in practice.

**Risk.** Not a live defect: the two-pass arrangement is currently harmless
because Silver's list is the stronger one and it runs first. The exposure is
that dbt's copy is a *weaker* backup than the primary it is backing up. If
Silver's normalization were ever removed, disabled, or the dbt project pointed
at an un-normalized source, `RICHMOND` would silently become `UNSPECIFIED`
rather than `STATEN ISLAND`, and volume would shift into the unattributed
bucket without any test firing.

**Secondary observation.** Silver overwrites `borough` in place, so the city's
original spelling is unrecoverable downstream. dbt does the opposite — it adds
`borough_clean` alongside the original. The same "never destroy the source
value" principle is applied inconsistently across the two systems; the Silver
side is the one that loses information.

**Options.**
1. Bring dbt's list to parity with Silver's and keep the double pass as
   deliberate defense in depth (document it as such at both sites).
2. Delete dbt's pass-through, let Silver own standardization outright, and add
   an `accepted_values` test on the Silver source asserting only the six
   canonical values ever arrive — which converts a silent assumption into a
   tested contract.
3. Have Silver write `borough_clean` alongside the raw `borough` instead of
   overwriting, so the original survives into Bronze→Silver→Gold.

**Resolved 2026-08-20 — none of the three options above.** Investigation found
the problem was larger than recorded: the mapping existed in **four** places,
not two. the PySpark module held two independent copies (a `KNOWN_BOROUGH_VARIANTS`
set AND a hardcoded `.isin()` chain), plus `local_runner.py`'s `BOROUGH_MAP`,
plus the dbt `CASE`. The Python copies agreed on 24 variants; dbt knew only 19.
(The PySpark module has since been removed with the Databricks path.)

Rather than syncing the copies — which decays — the mapping became **data**:
`config/borough_variants.csv`, one file with one row per spelling, read by all
four consumers. Both dbt projects load it as a seed via `seed-paths: ["../config"]`
and join to it; both Python transforms read the CSV directly and build their
mapping from it. `config/borough_variants.yml` puts a `unique` test on
`variant` (guarding the join against fan-out) and `accepted_values` on
`canonical`, so the mapping itself is now tested — nothing validated any of
the four lists before.

Verified behaviour-preserving: borough distribution identical across all six
values (61,329 rows, no fan-out), and the five variants dbt previously did not
know (`RICHMOND`, `KINGS COUNTY`, `NY`, `QUEENS COUNTY`, `BRONX COUNTY`) now
resolve correctly instead of collapsing to UNSPECIFIED.

---

## ~~dbt 1.12 deprecation warnings will become errors on the next major~~ — RESOLVED 2026-08-20

**Found:** 2026-08-20, in the dbt-docs build log after Dependabot moved
`dbt-core` to 1.12.x.

Every dbt invocation now emits two deprecation classes:

| Deprecation | Occurrences | What it wants |
|---|---|---|
| `PropertyMovedToConfigDeprecation` | 2 | `freshness:` is a top-level property of `sources[0].tables[0]` in `dbt/models/staging/sources.yml`; it must move under that table's `config:` |
| `MissingArgumentsPropertyInGenericTestDeprecation` | 16 | generic-test parameters (`dbt_utils.unique_combination_of_columns`, `accepted_values`, `relationships`, `dbt_utils.expression_is_true`) must nest under an `arguments:` key rather than sitting top-level |

**Risk.** Warnings only today — nothing fails. They become hard errors at the
next dbt major, which Dependabot will propose automatically, so that upgrade PR
would arrive already red with a cause not obvious from its diff.

**Resolved 2026-08-20 (#34).** The estimate of 18 sites was wrong: the real
count was **29**, because dbt *deduplicates* its warning output — it reports one
occurrence per type per file, not per instance. Enumerated from the files
instead. Both classes cleared across both projects and the shared seed schema;
`dbt parse --show-all-deprecations` now reports zero. Transformed with a
line-based script rather than a yaml round-trip, which would have preserved
semantics and destroyed every comment.

---

## ~~SLO-2 measures T-1, and T-1 may be structurally incomplete at 10:00 UTC~~ — RESOLVED 2026-08-27

**Resolved, and not by any of the three options this item listed.** The item
asked whether to move the window to T-2, keep T-1, or move the schedule. All
three assume the answer is an offset from the clock, and a fourth measurement
falsified that assumption: on **2026-08-27 03:03 UTC** the newest row at the
source was **2026-08-25 02:06 — a 49.0 h lag**, with a publish 1.4 h earlier
that carried nothing new. Against the 23.3 h and 23.5 h readings below, the lag
is **not a constant**, so T-2 would have been a whole day on the 25th and 26th
and a 358-row stub on the 27th.

SLO-2's population is now every day `int_load_completeness` marks complete — the
load decides, the clock does not — and the source-count capture covers the whole
fetch window rather than one day, which also makes a stub day re-reconcilable
once it fills in. The `WHEN source = 0 THEN true` branch, which turned an empty
denominator into a pass, is gone. See
[ADR 015](adr/015-slo2-population-is-complete-days.md).

The **unexplained 04:33 reading** recorded below is still unexplained, and it no
longer blocks anything: the new design does not depend on the lag having any
particular value. It is left standing for the same reason it was written down.

The evidence that produced this item is preserved unchanged below.

**Evidence, 2026-08-20.** The source backfilled after the 2026-08-18 stall, and
the recovery exposes a publishing rhythm:

| `created_date` | during the incident | 2026-08-20 | 2026-08-22 |
|---|---|---|---|
| Aug 17 | 410 | **10,473** | 10,853 |
| Aug 18 | 0 | **10,833** | 10,967 |
| Aug 19 | — | **372** ← stub | 10,632 (filled in) |
| Aug 21 | — | — | **382** ← same stub shape, two days later |

**Why it matters.** The daily run measures at 10:00 UTC and SLO-2 asks about
T-1. If T-1 is structurally ~4% complete at that hour on *every* normal day,
the gate is measuring the publishing calendar rather than our completeness.

**Mitigating fact:** SLO-2 was since redefined as a *source reconciliation*
(#24) — it compares what we loaded against what the source published for that
day, so a stub day yields 372/372 and **passes**. The gate is no longer fooled.
What remains is that a structurally-incomplete T-1 makes the number
uninformative rather than wrong.

### The publish model, measured twice (2026-08-20 and 2026-08-22)

An earlier version of this item said the dataset "publishes once daily at
roughly 02:20 UTC carrying data up to that moment." **That is wrong**, and the
correction matters because the two models predict opposite things about T-1.
Direct probes of the source:

| Probed | Last publish (`rowsUpdatedAt`) | Newest row (`max(created_date)`) | Lag |
|---|---|---|---|
| 2026-08-20 08:26 | 2026-08-20 01:46 | 2026-08-19 02:26 | **23.3 h** |
| 2026-08-22 06:27 | 2026-08-22 01:37 | 2026-08-21 02:05 | **23.5 h** |

Each publish lands at ~01:40 UTC carrying data only up to roughly *the previous*
morning. So the publish is daily, but its contents run about a day behind — T-1
at 10:00 UTC holds only the ~2 hours before that cutoff, and T-0 is empty.

Two independent T-1 measurements at run time agree: **Aug 19 = 372 rows**,
**Aug 21 = 382 rows**, against a trailing median near 10,500. Both are about
2.2 h of a ~10 k day. Every older day reads complete (Aug 13–20 all sit between
9,249 and 10,967).

### The observation that does not fit

This postmortem's own timeline records that at **04:33 on 2026-08-18, T-1
(Aug 17) held 9,119 rows — 87 % of its eventual 10,473.** Under the 23.5 h lag
model, T-1 at 04:33 should have held roughly 400 rows, not 9,119.

The two cannot both describe the same publishing behaviour. Either the source's
cadence changed across the incident, or that reading came from the local
database reflecting an earlier same-day fetch rather than a fresh probe of the
source — its provenance is recorded only as "local verification run". It is not
resolvable from what was retained, and it is left standing here rather than
dropped, because a measurement that contradicts the working model is the most
useful thing on this page.

**What would settle it:** probe `rowsUpdatedAt` and `max(created_date)` at a
fixed hour for several consecutive days. If the ~23.5 h lag holds, T-1 is
structurally a stub and option (a) is right. If the lag closes, the current
behaviour is incident residue and T-1 is fine.

**Options.** (a) Move the window to T-2 and state why. (b) Keep T-1 and accept
that the reconciliation makes it safe. (c) Move the schedule later than the
source's publish — ineffective if the lag is ~23.5 h, since no same-day hour
helps.

**Status (superseded 2026-08-27, see the resolution at the top of this item):**
two consistent post-recovery observations plus a measured mechanism, against one
recorded contradiction. The postmortem's bar was *several* normal days; two is
not several, and the unexplained 04:33 reading is a live reason not to act yet.
Keep accumulating.

**What accumulating actually produced.** A third measurement at 49.0 h, which
did not settle the "is T-1 structurally a stub" question — it dissolved it. The
right lesson is narrower than "wait for more data": the options list was drawn
from a single family of answers, and no amount of further observation inside
that family would have exposed that the family was wrong.

---

## ~~The 2026-08-18 postmortem is still marked draft, and its exit criterion is met~~ — RESOLVED 2026-08-20

Its exit criterion was *"finalize when the source backfills **and** a scheduled
run passes."* Both halves are now met: the source backfilled on 2026-08-19
(Aug 17 → 10,473, Aug 18 → 10,833), and the 2026-08-20 10:24 scheduled run — the
first under the redesigned SLO logic — passed.

**Resolved 2026-08-20.** Status → reviewed, recovery timeline added, issue #7
closed, and a *Did the control work* section added recording the measurement
that matters: Aug 19 published **372** rows against a trailing median of
**10,494.5**. Under the SLO-2 that existed during the incident that day computes
to 3.5% of median and **breaches**; under SLO-2 as reconciliation it computes
**372/372 and passes**. Same day, two readings, and the second is the correct
one — the pipeline loaded every row the city published.

The upstream-stall warning fired on the same run and filed issue #40 while the
run stayed green. That is the designed outcome, not a compromise.

---

## ~~Decide SLO-3, or record why it is not needed~~ — RESOLVED 2026-08-20

The postmortem proposed a third SLO for *source* freshness (max `created_date`
in Gold within N hours), to close the blind spot that SLO-1 measures our own
load stamp and is therefore minutes old after any successful run.

**Resolved 2026-08-20 — rejected, recorded as [ADR 013](adr/013-no-source-freshness-slo.md).**
Measuring the source before deciding settled it. On an ordinary day at 08:26 UTC,
`max(created_date)` was **30.0 h** stale while the dataset's own publish stamp
(`rowsUpdatedAt`) was **6.7 h** old — the file was published this morning
carrying nothing newer than yesterday morning. A gate must pick one of those two
columns, and each fails differently: `max(created_date)` would work but merely
duplicates the volume cliff `check_upstream_stall.py` already catches (Aug 18
held 0 rows against a ~10,000 median), while the publish stamp reports whether
the file was *touched*, not whether it gained data — during the stall Aug 17 sat
at 410 rows and later backfilled to 10,473, so a publish-stamp gate would have
read healthy through the whole incident.

The metric that works is redundant; the metric that is not redundant does not
work. Source staleness stays a warning. The standing principle: **gate on what we
control, warn on what we don't.**

**One gap left open knowingly:** the warning's floor is 40% of the trailing
median, so a *partial* stall — the city publishing half a normal day — passes
both SLOs and the warning. Not observed; recorded in the ADR so it is met as a
known limit rather than a surprise.

---

## fct_complaint_recurrence needs history — address normalisation RESOLVED 2026-08-20

**History.** The model emits `days_to_next_same_complaint` with no baked-in
window, so it already supports any threshold — but seven days of loaded history
supports only a 3-day rate. At 30+ days of accumulated runs it becomes a real
30-day metric with **no code change**, because the window is a var and the
censoring is exposed via `observation_days`. Nothing to do but let it run.

**Address normalisation — RESOLVED 2026-08-20.** Measured before choosing, which
changed the answer. Of 33,469 distinct address strings, only 232 collapse under
normalisation at all (0.69%) — and **226 of those 232 are internal whitespace**
(`WEST   86 STREET` vs `WEST 86 STREET`), not suffix variants. Whitespace is 97%
of the available gain and changes the key for **4.71% of tickets**; suffix
folding (`STREET`→`ST`, `AVENUE`→`AVE`, …) buys **six more strings** and was
deliberately skipped — an abbreviation table is a maintenance surface that six
strings does not pay for.

`address_key` now collapses internal whitespace. The headline recurrence figures
did **not** move at this data size, which is worth stating: the fix is correct
and the effect was nil, and reporting it as an improvement would be dishonest.

One bug worth recording: the first attempt used `'\\s+'`, which SQL reads as a
literal backslash — it matched nothing, raised nothing, and left 475 rows
unnormalised while the build stayed green. Replaced with the POSIX class
`[[:space:]]+`, which no layer can mangle, and guarded by
`assert_address_key_is_normalised` because a step that can silently do nothing
needs an assertion on its output.

Geocoding to a BBL/BIN remains the real fix and a separate concern.

---

## ~~Silver-quarantined rows stay in Gold forever~~ — RESOLVED 2026-08-23

**Found 2026-08-22**, by the first end-to-end smoke test run after a live fetch
moved the window.

**Mechanism (certain).** Quarantine happens in Silver, in pandas
(`drop_quarantined`), *before* dbt sees anything. A row that Silver rejects is
therefore absent from `silver.service_requests`, absent from
`stg_service_requests`, and absent from `int_service_requests_cleaned` — so the
reconciliation `post_hook` on `fct_service_requests`, which deletes rows
"present in staging, absent from int", is **structurally unable to see it**.
The hook catches rows the *dbt* quality filter drops; nothing catches rows the
*Silver* quarantine drops.

If the row was loaded by an earlier run, Gold keeps that earlier version
indefinitely.

**Evidence.** Two rows today:

| unique_key | in Gold | in the current raw file |
|---|---|---|
| 70093182 | `In Progress`, `closed_date=NULL` | closed 2026-08-17 09:55:00, created 09:55:14 |
| 70095979 | `In Progress`, `closed_date=NULL` | closed 2026-08-17 19:11:00, created 19:11:04 |

Both acquired a `closed_date` a few seconds *before* their `created_date` — the
clock-inversion the quarantine exists to catch. Silver now rejects them; Gold
still serves the pre-error version. `reconcile.py` reports this on two rungs
(`gold within the fetch window = silver`, and `closed-request count`, where the
same two rows make Gold undercount closures by exactly 2).

**Why it matters.** It is small now and it only grows. Gold is the serving
layer, so it currently publishes two records that the pipeline's own quality
rules have rejected. `reconcile.py` stays red until this is resolved, which is
correct — the discrepancy is real — but it means a red reconcile no longer
distinguishes "new problem" from "this known one".

**Options.**
1. **Persist the quarantine and delete from it.** Silver already computes
   `select_quarantine(df_derived)` — a function that is unit-tested but *not
   imported by the pipeline*, so quarantined rows currently vanish leaving only
   a count in `fct_data_quality`. Write them to `silver.quarantine`, expose a
   staging model, and extend the fct `post_hook` to delete those keys. Safe: it
   only ever deletes rows affirmatively identified as invalid.
2. **Widen the post_hook** to delete any Gold row inside Silver's window that is
   absent from Silver. Simpler, but riskier — a short or failed fetch would
   shrink the window and delete legitimate history.
3. **Accept and document**, treating Gold as eventually-consistent with Silver's
   quality rules.

Option 1 is the recommended one, and it has a second payoff: quarantined rows
become inspectable instead of discarded, which is what the word "quarantine"
implies and what the current code does not do.

**Resolved 2026-08-23 — option 1.** `select_quarantine()` is now imported and
its output written to `silver.quarantine`, exposed as `stg_quarantine`, and
deleted from the fact table by a second `post_hook`. The table is **replaced**
 every run, never appended: it means "rejected by the CURRENT fetch", and a growing
history would be actively wrong — a row the city later corrects must stop being
listed, or the hook would delete a valid row from Gold on every subsequent run.

`reconcile.py` is green again; the two stranded rows are gone from Gold.

Two things this exposed that are worth keeping in mind:

- The first attempt wrote **two `post_hook =` keys** in one `config()` call.
  That is a duplicate keyword: the second silently overrides the first, which
  would have deleted the *original* reconciliation while looking correct. It
  must be a list.
- The singular dbt test `assert_no_quarantined_rows_in_gold` is **vacuous under
  `--full-refresh`** — a full rebuild never contains quarantined rows, because
  they are absent from staging. It only has teeth incrementally, which is also
  the only mode where the bug exists. The behavioral fixture's `r9` covers the
  real lifecycle: valid in phase 1, Silver-rejected in phase 2, gone from Gold
  after the incremental build.

---

## The fetch window cannot update a ticket once its created_date ages out

**Mechanism (certain).** The daily fetch pulls a trailing 7-day window on
`created_date`. A request created on day 1 that closes on day 20 is never
re-fetched, because day 1 left the window on day 8. Our copy keeps it open
forever, so resolution metrics are biased toward fast-closing work.

**Evidence (so far: none).** Sampled 40 tickets held locally as open with
`created_date <= 2026-08-14`; the source currently reports **0 of them closed**.
The bias has not materialised at this history depth — those tickets are simply
still open.

**Why it will.** Slow categories close over weeks: Street Light and Parks &
Trees show near-zero same-week resolution. Those are precisely the tickets that
will close after leaving the window, so the bias grows with time and lands
hardest on the categories already reported as slowest.

**Options.** (a) Add a second small fetch pass on `:updated_at` restricted to
tickets we hold as open. (b) Accept it and document the ceiling on
`resolution_days`. (c) Widen the created window. (a) is the honest fix and is
cheap: the set of locally-open tickets is small and queryable.

---

## Census denominator — the next analytical unlock

Every geographic comparison in the platform is a raw count, and raw counts
measure population as much as they measure conditions. Joining ACS population
by ZIP or community district converts them to **rates**, which is the only
honest way to compare neighbourhoods and the entry point to the equity question
(*where are conditions bad but reporting low?*).

It would also be the **first external source** the pipeline ingests — a real
test of whether the medallion layering absorbs a second dataset cleanly, or
whether the ingest path is quietly single-source-shaped.

---

## The dbt/local dual tree is duplication held together by a checker

Two dbt projects mirror each other, kept aligned by `check_model_drift.py`. The
measured divergence is small — of 25 mirrored file pairs, 11 differ only in
comments, and the genuinely dialect-specific SQL is roughly 100 lines.

**Options.** (a) Unify into one project with two targets, using adapter dispatch
for the dialect gap — kills the duplication permanently, but makes both versions
less readable, which matters if the Snowflake code is the showcase. (b) Keep the
split and accept the tax, which is real: every model change costs two edits and
a baseline regeneration, and a merge conflict in the generated baseline has
already corrupted it once. (c) Drop Snowflake entirely.

Deliberately unresolved: the answer depends on whether `dbt/` is a *deployment
target* or a *reference artifact*, and that is a portfolio decision rather than
a technical one.

---

## The cached daily-run database carries pre-existing dangling location_ids

**Found:** 2026-08-25, fixing referential decay in `dim_location`.

`dim_location` now accumulates (`materialized: incremental`, keyed on
`location_id`) instead of being rebuilt each run from Silver's rolling 7-day
window, and `fct_service_requests.location_id` has its `relationships` test
back. That combination is **preventive, not curative**, and the difference
matters for the next scheduled run.

**Evidence.** Reproduced locally against a 54,408-row build by moving the
Silver window forward three days and rebuilding:

| | dim_location members | orphaned fact rows |
|---|---|---|
| Before the window moved | 540 | 0 |
| After, old `table` materialization | 433 | 189 |
| After, new `incremental` materialization | 540 | 0 |
| Old materialization, then the fix applied, window unchanged | 433 | **189** |

The last row is the point. `daily-run.yml` restores the previous run's DuckDB
from the Actions cache, so the deployed database starts with a `dim_location`
that has **already** lost members — 88 dangling fact rows at last measurement,
growing ~15–20/day. The accumulation only preserves what the dimension holds on
the day it is switched on; it cannot recover members already dropped. Their
attributes are unrecoverable from the platform: the fact table stores
`location_id` but not `borough`/`community_board`/`incident_zip`, and Silver,
Bronze, and the raw JSON all carry only the current window.

**Risk.** The restored `relationships` test runs inside `dbt build` during the
daily run, so the first scheduled run against the cached database will **fail**
on those pre-existing rows, and `daily-run.yml` will open a
`daily-run-breach` issue. The test is right and the data is wrong; this is a
one-time debt, not a regression.

**Remediation (verified locally).** Replay one Silver window wide enough to
cover the fact table's date range — the accumulating dimension then absorbs the
missing members permanently. Reproduced end to end: from the 189-orphan
database above, one build over the restored full window took `dim_location`
from 433 members back to 540 and orphans to 0, with `fct_service_requests`
unchanged at 54,408 rows (no fact row deleted). Moving the window forward again
afterwards kept orphans at 0.

**Options.**
1. Run `local_runner.py` once with a `--rows`/`--live` window spanning the
   cached fact's history, then re-save the cache. Exact, no new code, uses the
   accumulation as designed. Bounded by how far back Socrata's window reach
   goes versus how much history Gold has accumulated.
2. Drop the Actions cache and let the next run rebuild from scratch. Simplest,
   but discards accumulated Gold history — the thing the incremental fact
   exists to keep.
3. Land the model change now and the `relationships` test in a follow-up, after
   option 1 has run. Keeps the daily run green through the transition at the
   cost of leaving the guard off for another day.

**Not an option:** deleting the orphaned fact rows. They are valid requests that
merely aged out of the fetch window, and Gold's accumulated history is
deliberate.
