# Backlog

Known open work. Each item gives the problem, the evidence and a proposed fix,
so a future maintainer can act without re-deriving it. An item leaves this file
when it is done; git history and the ADRs keep the record.

---

## Requests still open after 37 days never update

**Problem.** The daily run re-pulls only requests created in the last 37 days
([ADR 010](adr/010-scheduled-operation.md)), and a row in Gold changes only
when it is re-pulled. A request still open when it leaves the window keeps that
status in Gold, even after the city closes it. Gold's 30-day metrics are not
affected: 37 days is the 30-day closure window plus the 7 days the source takes
to settle, so every 30-day outcome is final before a request leaves. What is
wrong is the current state of slow requests: `status`, `closed_date` and
`resolution_days` for anything that closes after day 37.

**Evidence.** On the cohorts created 2026-08-24 to 08-28, 10.5% of requests were
not closed within 30 days, so a share of them will close after day 37 and never
reach Gold. The same mechanism with a 7-day window was measured directly: three
samples of rows Gold held as open found 22 of 40, 30 of 40 and 25 of 30 already
closed at the source.

**Proposed fix.** Each run, re-fetch the requests Gold holds as open and older
than the window, by `unique_key` (batched `$where unique_key in (...)`). That
set is small, and the query does not touch `:updated_at`, which the source
re-stamps on ~540k rows a night. Until then, read a row's status as "as of day
37 at the latest".

---

## Refresh the rows stored under the 7-day window

**Problem.** Gold accumulates across runs and holds requests back to
2026-08-12 (498,458 rows in the 2026-09-29 database). Rows that left the window
while it was 7 days wide keep the status they had about a week after creation,
so their 30-day closure rates read low (68.4% instead of 89.5% on the same
cohorts). The 37-day window refreshes only requests created in the last 37
days; older cohorts never re-enter it.

**Proposed fix.** Run `daily-run.yml` once by hand with the `window_days`
input wide enough to reach 2026-08-12 (48 days on 2026-09-29; roughly 500k
rows, under the 800k cap). The run upserts every re-fetched row and deletes
none. Dimensions take their members from the window, so the run also re-seats
any member the fact table references but a dimension has lost (none were
missing in the 2026-09-29 database). After that, the daily 37-day window keeps
new cohorts current.

---

## fct_complaint_recurrence sees only the fetch window

**Problem.** The model is a table rebuilt every run from Silver's window
(`int_service_requests_cleaned`), not from the accumulated fact table. With the
37-day window, `observation_days` is at most about 36, so a 30-day recurrence
rate covers only closures from the first few days of the window: a small sample
that moves every day and does not grow as history accumulates.

**Evidence.** The model's source is `int_service_requests_cleaned`. In the
2026-09-29 database, built with the 7-day window over 48 days of Gold history,
`max(observation_days)` was 5.

**Proposed fix.** Carry the normalised address key into `fct_service_requests`
(it has no address column today), then read closures and later complaints from
the fact table. Keep `observation_days` and the complete-day horizon as they
are.

---

## Census denominator

**Problem.** Every geographic comparison in Gold is a raw count, and raw counts
measure population as much as conditions. Neighbourhoods cannot be compared
fairly, and the equity question (*where are conditions bad but reporting
low?*) cannot be asked.

**Proposed fix.** Join ACS population by ZIP or community district and publish
rates. It would also be the first dataset besides NYC 311 that the pipeline
ingests, a real test of whether the ingest path assumes there is only one.

---

## The dbt/ and local/ projects are duplicated

**Problem.** `dbt/` (Snowflake) and `local/` (DuckDB) are two copies of one dbt
project. Every model change is two edits, plus a baseline update
(`scripts/check_model_drift.py --update`) when the edit touches a line where
the copies differ.

**Evidence.** Of 37 mirrored files, 10 differ, in dialect lines and the project
names (80 diff lines recorded in `scripts/model_drift_baseline.json`). `scripts/check_model_drift.py`
fails CI when the copies drift apart.

**Proposed fix.** One of: (a) one project with two targets, using adapter
dispatch for the dialect lines, which ends the duplication but makes the
Snowflake SQL harder to read; (b) keep the split and its cost; (c) drop the
Snowflake project. The choice depends on whether `dbt/` is a deployment target
or a reference, which is the owner's call.
