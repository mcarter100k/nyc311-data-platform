# ADR 016: NYC 311 data settles after 7 days — the replica spread is a recency lag, not noise

**Status:** Accepted
**Date:** 2026-08-27
**Relates to:** [ADR 013](013-no-source-freshness-slo.md) (gate on what we control),
[ADR 015](015-slo2-population-is-complete-days.md) (SLO-2's population is complete days),
[docs/SLO.md](../SLO.md), [2026-08-18 postmortem](../postmortems/2026-08-18-upstream-publish-stall.md)

## Context

Three places in this repo record the same observation and draw the same
conclusion from it: `int_load_completeness` (both trees), its schema docs, and
ADR 015. The observation is that two identical fetches of the same 7-day window
returned **65,936 and 54,446** rows minutes apart. The conclusion is that a
completeness rule must use clock coverage rather than a row count.

The conclusion is right. The stated reason — that the source's counts are
**noisy** — is wrong, and being wrong about the mechanism cost real margin
somewhere else: SLO-2's 0.98 floor was justified against a loss budget that
omitted the largest term in it.

## What the measurement showed

Probing the same grouped count query 20 times per day, 2026-08-27 (20 probes
makes an even two-way split miss with probability < 1e-5):

| day | age | distinct counts over 20 probes | spread | relative |
|---|---|---|---|---|
| 2026-08-26 | 1d | `0`, `416` | 416 | 100.000% |
| 2026-08-25 | 2d | `358`, `10,785` | 10,427 | 96.681% |
| 2026-08-24 | 3d | `11,515`, `11,627` | 112 | 0.963% |
| 2026-08-23 | 4d | `10,739`, `10,789` | 50 | 0.463% |
| 2026-08-22 | 5d | `10,043`, `10,047` | 4 | 0.040% |
| 2026-08-21 | 6d | `11,519`, `11,521` | 2 | 0.017% |
| 2026-08-20 | 7d | `11,061` | **0** | 0.000% |
| 2026-08-19 | 8d | `10,701` | **0** | 0.000% |
| 2026-08-18 | 9d | `10,974` | **0** | 0.000% |
| 2026-08-17 | 10d | `10,857` | **0** | 0.000% |

Three properties, each of which the noise framing denies:

1. **It is monotone in age.** Noise does not shrink by four orders of magnitude
   as a row gets older. A replication lag does.
2. **It has a direction.** Across 30 probes of a 10-day window, the lagging
   replica's count was `<=` the leading replica's on **10 days out of 10**, with
   zero violations. The stale replica is *behind*, never *ahead*.
3. **There are exactly two states, routed per request.** Those same 30 probes
   produced exactly two distinct per-day vectors — window totals 87,767 and
   98,778 — split 13/17. The `65,936`/`54,446` pair recorded elsewhere in this
   repo is the same phenomenon read a day earlier: two replicas, one behind.

Roughly **95%** of that headline 17% spread is a single 2-day-old day (10,427 of
the 11,011-row gap between the two window totals measured here). It was never a
17% uncertainty about the week. It was near-total uncertainty about two days and
near-zero about the rest.

## Decision

**NYC 311 data settles after 7 days. Treat any day 7 days old or older as
final, and treat every younger day as still filling in.**

1. **`max` is the estimator for a source count, and now for a stated reason.**
   `fetch_source_counts_window` already took the per-day maximum over probes,
   justified as "the most complete view available". With the direction
   established, the justification is stronger and different in kind: the
   quantity being estimated — what the city has published for that day — only
   ever grows, and the lagging replica is a *lower bound* on it. The mean or the
   last probe would estimate "what some replica happened to hold", which is not
   a fact about the city.

2. **Eleven probes, from the measured split rather than a round number.** A
   day's captured count is wrong exactly when every probe lands on the stale
   replica, at probability `P(stale)^N`. The stale share was 52/98 = 0.53 pooled
   across 98 requests and 13/20 = 0.65 in the worst single run.

   | N | at P(stale)=0.65 | at P(stale)=0.53 |
   |---|---|---|
   | 5 — the previous value | 0.11603 | 0.04182 |
   | 10 | 0.01346 | 0.00175 |
   | **11 — chosen** | **0.00875** | **0.00093** |
   | 12 | 0.00569 | 0.00049 |

   Eleven is the *smallest* N holding the miss rate under 1% at the worst
   observed split; five mis-captured roughly one day in nine. Going higher buys
   little and spends round trips against a public API for margin already in
   hand. Measured cost over a 7-day window: **5.34 s at N=5 against 11.69 s at
   N=11**, ~6 s added to a run whose Gold build alone is minutes.

3. **The capture records its own evidence.** `silver.source_counts` gains
   `probe_count`, `source_count_min` and `probes_disagreed`. A denominator that
   cannot be audited has to be trusted, and the settling spread for a day is
   exactly `source_count - source_count_min`. Stage 3 performs the column
   migration itself (`ADD COLUMN IF NOT EXISTS`), because the daily run persists
   its DuckDB file and `CREATE TABLE IF NOT EXISTS` is a no-op against it.

   A real capture, 2026-08-27, N=11 — the horizon reproduced from the table's
   own columns rather than from a one-off script:

   ```
   2026-08-20  max= 11061  min= 11061  probes=11  disagreed=False   (7d, settled)
   2026-08-21  max= 11521  min= 11519  probes=11  disagreed=True    (6d, spread 2)
   2026-08-22  max= 10047  min= 10043  probes=11  disagreed=True    (5d, spread 4)
   2026-08-23  max= 10789  min= 10739  probes=11  disagreed=True    (4d, spread 50)
   2026-08-24  max= 11627  min= 11515  probes=11  disagreed=True    (3d, spread 112)
   2026-08-25  max= 10785  min=   358  probes=11  disagreed=True    (2d, spread 10,427)
   2026-08-26  max=   416  min=     0  probes=11  disagreed=True    (1d, spread 416)
   ```

4. **This makes SLO-2 stricter, deliberately.** The denominator is the largest
   count any probe saw; the numerator is whatever replica served the load.
   Raising the denominator can only lower the ratio. Reconciling against the
   best estimate of what was published, rather than against the convenient
   number, is the correct direction and it costs margin — see below.

5. **Nothing gates on the horizon itself.** The 7-day figure is documentation
   and a budgeting input. No query, threshold, or filter reads "7 days"; the
   fetch window's `LIVE_DAYS = 7` predates this measurement and coincides with
   it by accident, not by derivation.

## What this changes about SLO-2's floor — and what it does not

The floor stays at **0.98**. The justification for it does not.

`slo2_completeness.sql` explained the 2% as quarantine and dedup, worth up to
0.24%, and read the headroom as ~1.76 points. Settling skew is worth up to
**0.96%** — four times larger — and was not mentioned. It lands directly in the
ratio because the numerator and the denominator are drawn from the same
inconsistent pool: the load takes whichever replica answered, while the capture
deliberately takes the maximum.

| term | worst measured |
|---|---|
| quarantine + dedup, on days old enough to have stopped moving | 10,521 / 10,546 = 0.9976 |
| settling skew at 3 days | 11,515 / 11,627 = 0.99037 |
| **combined** | **0.99037 × 0.99763 = 0.9880** |

A **1.20% budget against a 2.00% floor: 0.80 points of margin, not 1.76.** The
floor holds — 1.20 < 2.00 — and moving it would mean fitting a threshold to one
observation window, which is the error this ADR is correcting elsewhere. But it
is roughly half consumed, and the term consuming it is the one nobody had
measured.

This is not hypothetical. Run against the database left by the 03:31 UTC
2026-08-27 daily run — whose load was served by the *stale* replica — the gate
itself reports:

```
✓ slo2_completeness.sql: slo=SLO-2 completeness  complete_days_assessed=12
  newest_complete_day=2026-08-24  worst_day=2026-08-24
  worst_day_rows_loaded=11513  worst_day_rows_published=11627
  tolerance_floor=0.98  pass=True
```

`11,513 / 11,627 = 0.9902`, and the worst day is the *youngest* assessed day
rather than the oldest.

**This supersedes ADR 015's consequence table**, which recorded "worst day
10,521 / 10,546 = 0.9976" and "roughly 1.8 percentage points of headroom" for
what it describes as the same 2026-08-27 load. Re-running the unchanged query
against that database returns 0.9902 and 1.02 points, so the table is not a
reading anyone can reproduce today. The likeliest explanation is the five-probe
capture: at N = 5 a day's denominator missed the fresh replica up to 11.6% of
the time, and a run that missed it reconciled 11,515 against 11,515 and reported
~1.0, hiding the 3-day-old day behind an older one. That is a hypothesis about
how the number arose, not a measurement — what *is* measured is that the figure
is wrong now.

**Raising N from 5 to 11 makes this MORE visible, not less**, which is the
point. The denominator is now the fresh value on essentially every run, so the
gate can no longer be flattered by its own sampling; whenever the *load* is the
stale one — 43% of requests as measured — the 3-day-old day reports ~0.9902
rather than ~1.0. Expect SLO-2's worst day to sit near 0.99 routinely and to
vary between runs on unchanged data. That is the honest number, and it is the
number the floor must be justified against.

## A premise this ADR checked and refuted

The natural defence of the floor is that clock coverage already excludes the
catastrophic 1-to-2-day window: on the stale replica a 2-day-old day holds only
its first two hours, so it never reads as complete. **That is only true of the
stale replica.** Measured 2026-08-27, `max(created_date)` per day per replica:

| day | age | stale replica | fresh replica |
|---|---|---|---|
| 2026-08-25 | 2d | `02:06:15`, 358 rows — 1,314 min short, **not complete** | `23:59:45`, 10,785 rows — 1 min short, **COMPLETE** |
| 2026-08-24 | 3d | `23:59:36`, 11,515 rows — **COMPLETE** | `23:59:36`, 11,627 rows — **COMPLETE** |

So a **2-day-old day does enter SLO-2's population**, whenever the load happens
to be served by the leading replica — which was 17 requests in 30. The exclusion
is an accident of routing, not a property of the rule. A full `--live` run at
05:35 UTC on 2026-08-27, served by the leading replica, ends the argument:

```
✓ slo2_completeness.sql: complete_days_assessed=6  newest_complete_day=2026-08-25
```

The newest day the gate assessed was **2 days old**.

It does not widen the budget, for a reason worth stating rather than assuming: a
2-day-old day is only *complete* when the load holds it to midnight, which means
the load was served by the fresh replica, and the capture's maximum then comes
from that same state. Numerator and denominator agree and the ratio is ~1. The
same run confirms it, and shows the whole mechanism in one table — note that
`settling_spread` is read straight out of `silver.source_counts`:

| load_day | age | loaded | published | settling_spread | ratio |
|---|---|---|---|---|---|
| 2026-08-20 | 7d | 11,054 | 11,061 | 0 | 0.99937 |
| 2026-08-21 | 6d | 11,506 | 11,521 | 2 | 0.99870 |
| 2026-08-22 | 5d | 10,043 | 10,047 | 4 | 0.99960 |
| 2026-08-23 | 4d | 10,784 | 10,789 | 50 | 0.99954 |
| 2026-08-24 | 3d | 11,625 | 11,627 | 112 | 0.99983 |
| 2026-08-25 | 2d | 10,772 | 10,785 | 10,427 | 0.99879 |

The 3-day-old day reconciles at 0.99983 *here* — the load was fresh, so it
matched the fresh denominator — where the 03:31 run reconciled the same day at
0.9902 because its load was stale. **The exposure is a routing coincidence, not
a property of the day**: it materialises when a stale load meets a fresh
denominator, and it is largest at 3 days, the youngest age at which a *stale*
load still reaches midnight. The premise is wrong; the conclusion it was
defending survives for a different reason.

One related hazard was checked and **not observed**: `fetch_live_records` pages
with `$offset`, and each request is routed independently, so pages could in
principle mix replica states and produce a day that reaches midnight while
holding a fraction of its rows. Across 14 replications of the live walk
(2 pages each) every trial's per-day counts matched one replica state exactly —
totals were 55,235 or 66,246 and never a hybrid. Recorded as unproven-safe
rather than as safe.

## Consequences

**The repo tells one story.** `int_load_completeness` in both trees, its schema
docs, and docs/SLO.md now describe the spread as a settling lag. The conclusion
they drew — clock coverage, not row counts — is unchanged and better supported:
a count threshold is not merely noisy, it is *systematically biased low on the
youngest days*, which are the days a completeness rule exists to judge. And
clock coverage is immune for a specific reason, that the source publishes each
day as a **time prefix**: a replica holds the day through 23:5x or stops hours
short, with no observed intermediate state in which a materially incomplete day
could pass a coverage-to-midnight test.

**A day now has a deadline.** The fetch window is 7 days and a day settles at 7
days, so a day is reconcilable for exactly as long as it is still moving, with
no slack. Widening `--live --days` is the remedy if that proves too tight; ADR
015 already records the unreconcilable-day gap this sits next to.

**One observation window, stated plainly.** Every number here comes from a
single measurement session on 2026-08-27 against a source whose publishing
behaviour this project does not control and has already seen change once (the
2026-08-18 stall; a lag measured at 23.3h, 23.5h, then 49.0h within one week).
The 7-day horizon is **an observation, not a guarantee**. If the city changes
its backfill practice the horizon moves, and the first thing to move with it is
SLO-2's 0.80 points of margin. Nothing gates on the number today, which is what
makes it safe to write down; re-measure before anything starts to.
