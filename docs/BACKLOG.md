# Backlog

Known issues and follow-ups that are real but not urgent. Each entry states the
evidence, the risk, and the options — so a future session can act without
re-deriving the analysis. Items graduate to a PR or an ADR; they do not sit
here as vague intentions.

---

## Borough standardization is duplicated across Silver and dbt, and the copies have drifted

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

Option 2 plus 3 is the smaller long-term surface; option 1 is the smaller diff.
Either way the fix belongs in one PR touching both projects, since
`check_model_drift.py` treats the two trees as mirrors.
