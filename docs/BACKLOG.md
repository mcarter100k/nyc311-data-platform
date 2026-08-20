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
| `PropertyMovedToConfigDeprecation` | 2 | `freshness:` is a top-level property of `sources[0].tables[0]` in `models/staging/sources.yml`; it must move under that table's `config:` |
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

## SLO-2 measures T-1, and T-1 may be structurally incomplete at 10:00 UTC

**Evidence, 2026-08-20.** The source backfilled after the 2026-08-18 stall, and
the recovery exposes a publishing rhythm:

| `created_date` | during the incident | now |
|---|---|---|
| Aug 17 | 410 | **10,473** |
| Aug 18 | 0 | **10,833** |
| Aug 19 | — | **372** ← same shape, one day later |

`max(created_date)` in the source is `2026-08-19T02:26`. The dataset appears to
publish once daily at roughly 02:20 UTC carrying data up to that moment, so the
most recent day is always a stub until the *following* publish fills it.

**Why it matters.** The daily run measures at 10:00 UTC and SLO-2 asks about
T-1. If T-1 is structurally ~4% complete at that hour on *every* normal day,
the gate is measuring the publishing calendar rather than our completeness.

This is exactly the condition the postmortem's third follow-up said to wait for:
*"revisit only if several normal post-recovery days show T-1 chronically
incomplete at 10:00 UTC — threshold changes require accumulated evidence, not
one incident."* Two post-recovery days now show it. A third would settle it.

**Mitigating fact:** SLO-2 was since redefined as a *source reconciliation*
(#24) — it compares what we loaded against what the source published for that
day, so a stub day yields 372/372 and **passes**. The gate is no longer fooled.
What remains is that a structurally-incomplete T-1 makes the number
uninformative rather than wrong.

**Options.** (a) Move the window to T-2 and state why. (b) Keep T-1 and accept
that the reconciliation makes it safe. (c) Move the schedule later than the
source's publish. Needs one more day of observation before choosing.

---

## The 2026-08-18 postmortem is still marked draft, and its exit criterion is met

`docs/postmortems/2026-08-18-upstream-publish-stall.md` says *"draft — incident
ongoing at time of writing; finalize when the source backfills and a scheduled
run passes."* The source **has** backfilled (Aug 17 → 10,473, Aug 18 → 10,833).

**To do:** status → reviewed, add the recovery timeline, close issue #7, and
record that the control which shipped (SLO-2 as source reconciliation plus the
non-gating upstream-stall check) was validated by this recovery.

**Note:** the first scheduled run under the new SLO logic has not happened yet —
the last daily run (2026-08-19 10:22) used the old median-based SLO-2. Today's
10:00 UTC run is the first real exercise of it, and should be cited in the
finalized postmortem.

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
