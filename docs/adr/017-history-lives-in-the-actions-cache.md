# ADR 017: Gold's history lives in the GitHub Actions cache

**Status:** Accepted
**Date:** 2026-10-08
**Relates to:** [ADR 008](008-prototype-scope.md) (prototype scope),
[ADR 010](010-scheduled-operation.md) (scheduled operation),
[ADR 012](012-github-repo-as-code.md) (repository settings as code)

## Context

Each daily run fetches the last 37 days. Gold holds much more than that, every
request since 2026-08-11, because each run builds on the previous run's
database instead of starting empty. That database is carried from run to run in
one place: the GitHub Actions cache.

How it works in `daily-run.yml`:

- **Restore.** The run restores the newest cache entry whose key starts with
  `nyc311-duckdb-`. With no match, it starts from an empty file.
- **Save.** Only a successful run saves its database, under a new key
  (`nyc311-duckdb-<run id>`). A failed run cannot replace good history with a
  partial build.
- **Copy.** Every run also uploads the database as an artifact, kept 14 days.

GitHub removes a cache entry nobody has read for 7 days, and drops the
least-recently-used entries once a repository's caches pass 10 GB. On
2026-10-08 there were 11 entries totalling 1.6 GB, each read daily, so neither
rule touches the newest one while the daily run is running.

**The risk is that losing it is silent.** On a cache miss the run starts empty,
loads 37 days, and passes both SLOs: SLO-1 sees minutes-old data and SLO-2
reconciles every complete day it loaded. A from-empty 37-day build on
2026-09-29 printed "All SLOs met". Gold would shrink from about 520,000 rows to
about 385,000 and every gate would stay green.

Two ways it could happen:

1. **The daily run stops for over 7 days**, and the cache entry expires. GitHub
   disables scheduled workflows in a public repository after 60 days without
   repository activity, so a quiet repository reaches this on its own: 60 days
   after the last commit, then 7 more. The heartbeat cannot warn about it,
   because the heartbeat is scheduled too and is disabled at the same moment.
2. **The cache is cleared** by hand or by GitHub.

## Decision

Keep the history in the cache, and make losing it loud.

- `scripts/check_history.py` runs around the pipeline step. Before it, it
  records what the restored database held, or that nothing was restored.
  After it, it warns when nothing was restored, when Gold lost more than 1% of
  its rows, or when Gold's earliest request moved later. Like the upstream-stall
  check, it never fails the run: the fetch window loaded correctly, and failing
  would not bring the history back.
- A warning files or updates one issue labeled `history-loss`. The label is
  declared in `terraform/github`, and a test fails if any workflow uses a label
  Terraform does not declare.

Storing the database outside GitHub (object storage) would remove the risk, but
it needs a cloud account and credentials in CI. That is the cost ADR 008 chose
not to pay for a prototype.

## Recovery

The city's dataset keeps every request, so lost history can be fetched again.

- **Up to about 75 days back:** run `daily-run.yml` by hand with the
  `window_days` input set to the number of days to restore. The 800,000-row cap
  limits one run to roughly 75 days. The fact table upserts every re-fetched
  row, so this both restores and refreshes.
- **Older than that:** the window always ends today, so the daily runner
  cannot fetch an older range on its own. The newest artifact (14 days) holds a
  full copy of the database for inspection or a manual rebuild.

## Consequences

- Gold's history is only as durable as the daily run is regular. Any commit
  within 60 days keeps the schedule alive.
- A deliberate reset, such as deleting the cache by hand, also files a
  `history-loss` issue. That is intended: it should never go unnoticed.
- The 1% tolerance absorbs the few rows quarantine post-hooks delete in a run.
  A lost cache drops far more: from about 520,000 rows to about 385,000 on
  2026-09-30.
