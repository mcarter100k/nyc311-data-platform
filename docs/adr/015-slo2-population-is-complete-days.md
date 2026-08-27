# ADR 015: SLO-2's population is complete days, chosen by the data — not an offset from the clock

**Status:** Accepted
**Date:** 2026-08-27
**Relates to:** [ADR 010](010-scheduled-operation.md) (scheduled operation and the SLO gates),
[ADR 013](013-no-source-freshness-slo.md) (gate on what we control, warn on what we don't),
[docs/SLO.md](../SLO.md), [2026-08-18 postmortem](../postmortems/2026-08-18-upstream-publish-stall.md)

## Context

SLO-2 has asked the right *question* since [#24](https://github.com/mcarter100k/nyc311-data-platform/pull/24):
did we load what the city published? It asked it about the wrong *day*.

The capture (`local_runner.fetch_source_count_yesterday`) asked the source for
**UTC-yesterday** at run time, ~10:00 UTC, and the query reconciled
`current_date - 1`. But the source publishes on a lag: a publish landing ~01:40
carries data only to ~02:05 of the previous day. UTC-yesterday is therefore
never a whole day. It is the first couple of hours of one, or nothing.

What that produced, measured:

| Measurement | Value |
|---|---|
| 2026-08-19 at the source, once settled | 10,701 rows |
| The 2026-08-20 run's headline "100% reconciliation" | 372 / 372 |
| Share of that day actually certified | **3.5%** |
| 2026-08-26, six identical probes at 03:00 UTC | `[0, 0, 0, 0, 0, 0]` |
| `slo2_completeness.sql:32` | `WHEN (SELECT n FROM source) = 0 THEN true` |

So on a good day the gate certified a ~2-hour sliver, on a bad day it passed
**vacuously on a zero denominator**, and in neither case did it ever revisit a
day once that day filled in. `check_upstream_stall.py` could not compensate: it
compared our own counts day-over-day and never looked at the source at all.

The same defect wrecked the warning. Comparing that same stub against a trailing
7-day median of full days scores ~3.4% against a 40% floor, so the stall warning
fired on **100% of healthy runs** — issue #40 was commented every day from
2026-08-20. ADR 013's own argument applies: a signal that cannot stay quiet
trains the operator to ignore it.

A prior attempt ([#50](https://github.com/mcarter100k/nyc311-data-platform/pull/50))
made the capture take the maximum of five probes and docs/SLO.md claimed *"this
closes the exposure."* That sentence was false and has been removed. Max-of-N
helps only when *some* replica holds the day; when the source has not published
the day at all, every probe correctly returns 0.

## The obvious fix, and why it is wrong

Move the window from T-1 to T-2. It is one character and it would have passed
review.

It is wrong because **the lag is not a constant**. Measured within a single week
against the live dataset:

| When | Newest row at source | Lag | Last publish |
|---|---|---|---|
| 2026-08-25 | — | 23.3 h | — |
| 2026-08-26 | — | 23.5 h | — |
| 2026-08-27 03:03 UTC | 2026-08-25 02:06 | **49.0 h** | 2026-08-27 01:36 (1.4 h earlier) |

On 2026-08-27 the source published 1.4 hours before the measurement and carried
nothing new — the same shape as the 2026-08-18 stall. T-2 would have been a
whole day on the 25th and 26th and a 358-row stub on the 27th. Any fixed offset
is a stub on some days; T-2 merely relocates the defect and buys a review pass.

## Decision

**SLO-2's population is every day the LOAD shows as complete, and the load
decides — the clock does not.**

1. **Completeness comes from the existing primitive.** `int_load_completeness`
   (added in [#56](https://github.com/mcarter100k/nyc311-data-platform/pull/56))
   already marks a day complete when its newest request lands within
   `complete_day_tail_minutes` of midnight. It was built for the recurrence
   horizon; a second consumer is a point in its favour, and re-deriving the
   concept would guarantee the two drift. Crucially it is **clock coverage, not
   a row count** — row-count thresholds were rejected there because the source
   is not read-consistent (identical fetches returned 65,936 and 54,446), and
   that reasoning is unchanged here.

2. **The capture covers the whole window, not one day.** The gate cannot know
   at fetch time which day it will pick, so `fetch_source_counts_window` asks
   for per-day counts across the entire fetch window in ONE grouped Socrata
   request (no extra round trips) and writes one row per day into
   `silver.source_counts`, idempotent per date.

3. **Every complete day in the window is assessed, not only the newest.** This
   is what makes a day re-reconcilable: the daily run re-pulls and re-counts the
   whole window, so a day first loaded as a stub is reconciled *properly* once
   the source fills it in. It also removes the skip hazard — under a
   newest-day-only rule, a day the source finishes late while the horizon moves
   past it would never be assessed at all.

4. **Zero is never a pass.** The `WHEN source = 0 THEN true` branch is deleted.
   Within the new population a zero source count is a *contradiction*: the load
   says the source published that day through to midnight. Either the capture is
   wrong or the source retracted the day; both mean the gate cannot vouch for it.
   Days the source genuinely has nothing for are recorded as explicit `0` by the
   capture and are simply not complete, so they never enter the population.
   "The source says none" and "we never asked" stay distinguishable — the latter
   still fails closed.

5. **No complete day in the window FAILS.** See the trade-off below.

6. **The stall warning gets the same population**, and looks at the SOURCE.
   Staleness: the newest complete day more than 2 days behind today (UTC).
   Volume: that day's source count below 40% of the median source count of the
   other complete days.

## A separate defect fixed in the same pass: HTTP faults were never retried

In both fetch paths `resp.raise_for_status()` sat **outside** the
`for attempt in (1, 2)` loop. `requests` raises only on connection-level
faults; it returns 429 and 5xx as ordinary `Response` objects. So the two
likeliest transient faults against a public, rate-limited API got **zero**
retries, while a dropped socket got one — the protection was inverted.

Both paths now share `_get_with_retry`: three attempts with exponential backoff
(1s, 2s), retrying on a connection error or on `{429, 500, 502, 503, 504}`, and
raising **immediately** on any other non-2xx, because repeating a 404 or a
malformed-query 400 only buries the real status. The fail-loud contract of
ADR 010 is unchanged — exhausted retries raise, a cap breach raises, a zero-row
fetch raises.

This supersedes the sentence in [ADR 010](010-scheduled-operation.md) reading
*"network failure (after exactly one retry) fail the run"*: the retry count is
now three attempts, and HTTP-level transient faults are inside the policy where
they were previously outside it.

## The one genuinely contested call

Failing SLO-2 when the window holds **no** complete day is in tension with
ADR 013's *gate on what we control*: seven-plus days of the city publishing
nothing is not our fault.

It gates anyway, for two reasons. First, the alternative is a gate reporting
success while measuring nothing, which is the defect this ADR exists to remove;
`check_slos.py` already applies exactly this rule to itself ("zero evaluated
SLOs is a breach of the gate itself, not a pass"). Second, the remedy *is* ours:
the fetch window is a parameter, and `--live --days N` widens it until complete
days are in scope. A seven-day total publish outage is also not routine noise —
it means the platform's data is unusable, which is worth a red build.

This is the call most worth revisiting if it proves wrong in operation.

## Consequences

**The gate now certifies something.** Measured on a 14-day live load,
2026-08-27, against the same database before and after:

| | complete days assessed | rows reconciled | verdict |
|---|---|---|---|
| Before | 1 (`current_date - 1`) | 358 loaded vs no captured count | **BREACH** (fails closed) |
| After | 12 | ~126,000 | **PASS**, worst day 10,521 / 10,546 = 0.9976 |

All twelve complete days landed between 0.9976 and 0.9998, so the 0.98 floor has
roughly 1.8 percentage points of headroom over the worst day observed. The
shortfall is the documented quarantine and dedup, as intended.

**The warning can now be quiet.** It fires today — the source genuinely is
3 days behind — and it names why, rather than reporting a volume cliff that was
only ever the publish lag.

**A latent timezone bug went with it.** The old query compared the session's
`current_date` against a `target_date` captured in UTC. On the UTC runner they
agree; on a laptop west of Greenwich they do not, and SLO-2 failed closed for
that reason alone. Neither query anchors on a session date any more, and the
stall check takes `current_timestamp AT TIME ZONE 'UTC'`.

**What this does NOT solve, stated rather than hidden.** A day the city never
publishes in full *within the fetch window* is never reconciled, and once it
ages out of the window it is unrecoverable by the daily run — the window is
7 days by default and the source has been observed 2 days behind. That gap is
narrower than before (the old design revisited nothing at all, ever) but it is
not closed, and closing it needs a backfill path, not a gate change. The
`--days` parameter is the manual remedy and the groundwork for one.

**Cost.** SLO-2 is now a broader gate: twelve days can redden the run where one
stub could not. That is the point — but it means a thin fetch (Socrata served
65,936 and 54,446 rows for identical queries once) can now redden a run that
previously could not notice. Not observed on the 14-day load measured here,
where every day reconciled above 99.7%; recorded so the next person meets it as
a known exposure rather than a surprise.
