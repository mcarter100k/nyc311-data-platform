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
| Silver | `local/local_runner.py` `BOROUGH_MAP` (and `databricks/notebooks/silver_transformations.py` `KNOWN_BOROUGH_VARIANTS`) | 24 — includes `KINGS COUNTY`, `NY`, `QUEENS COUNTY`, `BRONX COUNTY`, `RICHMOND` |
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
not two. `silver_transformations.py` held two independent copies (the
`KNOWN_BOROUGH_VARIANTS` set AND a hardcoded `.isin()` chain inside
`standardize_borough`), plus `local_runner.py`'s `BOROUGH_MAP`, plus the dbt
`CASE`. Databricks and pandas agreed on 24 variants; dbt knew only 19.

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

## dbt 1.12 deprecation warnings will become errors on the next major

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

**Note.** Both projects are affected: `local/` mirrors the same yml files, so
the fix is one PR touching both trees (`check_model_drift.py` compares them).
Roughly 18 small yml edits — mechanical but wide.
