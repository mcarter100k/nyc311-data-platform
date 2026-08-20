# ADR 013: No source-freshness SLO — gate on what we control, warn on what we don't

**Status:** Accepted
**Date:** 2026-08-20
**Relates to:** [ADR 010](010-scheduled-operation.md) (scheduled operation and the SLO gates), [docs/SLO.md](../SLO.md), [2026-08-18 postmortem](../postmortems/2026-08-18-upstream-publish-stall.md)

## Context

The 2026-08-18 postmortem proposed a third SLO for **source** freshness — assert
that `max(created_date)` in Gold is within N hours — to close a named blind
spot: SLO-1 measures our own load stamp, so it reads healthy minutes after any
successful run even if the run loaded nothing new.

The blind spot is real. The question this ADR settles is whether a *gate* is the
right instrument for it, given that SLO-2 was redefined ten days earlier
(#24) specifically so that a city publishing outage could no longer redden our
pipeline's reliability signal.

## What the measurement showed

Measured against the live source on 2026-08-20 at 08:26 UTC, on an ordinary day
with no incident in progress:

| Signal | Value | Age |
|---|---|---|
| `max(created_date)` in the source | 2026-08-19 02:26 | **30.0 h** |
| `rowsUpdatedAt` (dataset publish stamp) | 2026-08-20 01:46 | **6.7 h** |

The two are decoupled by roughly 23 hours *on a normal day*. The dataset was
published this morning and carried nothing newer than yesterday morning. Any
freshness gate has to pick one of these two columns, and each fails for a
different reason.

**`max(created_date)`, as the postmortem proposed.** The normal-day value is
~30 h, so a threshold must sit above it; during the August stall it would have
climbed to ~54 h and then ~78 h, so it *would* discriminate. But it is
**redundant**: `check_upstream_stall.py` already detects the identical event.
That check counts rows dated T-1, and a source that stops publishing produces
exactly the cliff it looks for — Aug 18 held 0 rows against a ~10,000 median.
A second detector of an already-detected event adds no information, only a
second threshold to tune. And making it a gate would re-redden the run on a city
fault, which is the precise behaviour the SLO-2 redefinition removed.

**`rowsUpdatedAt`, the publish stamp.** Fresher and superficially the cleaner
signal, but it reports whether the file was *touched*, not whether it gained
data — and the incident record shows why that is fatal. During the stall, Aug 17
sat at 410 rows and later backfilled to 10,473. The dataset was being written
throughout; it was simply underfilled. A publish-stamp gate would have read
healthy for the entire incident it was designed to catch.

So the metric that works is redundant, and the metric that is not redundant does
not work.

## Decision

**No SLO-3.** Source staleness stays a non-gating warning in
`check_upstream_stall.py`, which files a labeled `upstream-stall` issue while the
run stays green.

The principle, stated so it does not have to be re-derived: **gate on what we
control, warn on what we don't.** SLO-1 (did our pipeline run) and SLO-2 (did we
load everything the city published) both fail for reasons we can fix. The city's
publishing schedule is not one of those reasons. A red build nobody can act on
trains the operator to ignore red builds, and that costs more than the blind spot
it closes.

Note that this decision does not depend on the 30-hour figure. Only one clean
observation of normal-day staleness exists, and it may move. Both arguments above
— redundancy with the volume warning, and gating on a fault we cannot remedy —
hold at any value, which is why the decision is safe to make now rather than
after more days of observation.

## Consequences

The postmortem's blind spot is closed by a warning rather than a gate. That is a
weaker instrument, deliberately, and it leaves one gap worth recording: the
volume warning's floor is 40% of the trailing median, so a **partial** stall — the
city publishing, say, half a normal day — passes both SLOs and the warning, and
nothing surfaces it. Not observed to date. Recorded here so the next person meets
it as a known limit rather than a surprise.

If a future incident shows partial stalls happening in practice, the fix is to
tighten or re-shape the *warning*, not to promote it to a gate — the reasoning
above does not change with the threshold.
