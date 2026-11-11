# What the data shows

What the pipeline's tables say about how New York City handles 311 complaints, how far each finding can be trusted, and how to reproduce it. The [README](../README.md) has the two-line version.

Measured on the 127,255 requests created in the twelve complete days **13–24 Aug 2026**.

---

## "Closed" usually does not mean "fixed"

Of 89,506 closures, between **35% and 44%** describe the city doing something. The rest closed as *no violation found*, *nothing there*, duplicate, or handed off. It is a range because 8.5% of closures carry text no rule can classify, and per category the width is itself the finding:

| Category | Actioned | Uncertainty (points) |
|---|---|---|
| Illegal Parking | 42–43% | 0.7 |
| Noise | 40–41% | 1.5 |
| **Homeless Services** | **17–20%** | 3.6 |
| Street Condition | 28–46% | 17.9 |
| Water & Sewer | 17–40% | 22.3 |

## A closure the city couldn't complete is the one that comes back

After an *Access Failed* closure, the same complaint returns to the same address within 3 days 13.8% of the time. That ranks first in **all eight** specifications tried (windows of 2–5 days, with and without *chronic* locations: addresses that file the same complaint again and again), 1.1–5.8 points ahead of the runner-up.

3-day recurrence, every decoded closure type with at least 200 closures:

| Closed as | Recurred within 3 days |
|---|---|
| **Access Failed** | **13.8%** |
| Resolved on Scene | 10.5% |
| Duplicate | 10.4% |
| Enforcement Action | 10.4% |
| Referred Elsewhere | 10.2% |
| No Violation Found | 9.7% |
| No Condition Found | 6.7% |
| Work Performed | 5.7% |

## Methodology, and what does not survive it

Absolute recurrence rates do not travel: *Work Performed* ranges from 4.2% to 15.8% on the same data depending only on the window and whether chronic locations are included. Only the Access Failed *ranking* is stable, and only among closure types whose text could be decoded. A repeat is evidence, not proof: the fix may have failed, or the condition may be legal and keep being reported. Both guards are columns on [`fct_complaint_recurrence`](../dbt/models/marts/fct_complaint_recurrence.sql).

Volume: weekdays average 10,955 requests a day and weekends 9,903, while noise complaints more than double at weekends (1,730 → 3,589 a day).

## What was corrected

A claim that "nothing there" closures recur least was withdrawn: it came from 7 days of data and reversed on 12. A weekday/weekend volume comparison was reported backwards (both totals had been divided by the same number of days). Both are registered in the claim checker so they cannot return. And the daily run first fetched only 7 days, so Gold never saw a request close after day 7: for requests created 24–28 Aug, the 30-day closure rate read 68.4% instead of 89.5%. The window is now 37 days, and one wide run on 30 Sep refreshed the rows stored before the change.

## Reproduce it

Run the pipeline first (see [Run it](../README.md#run-it)). `pip install duckdb` gives the Python library, not the `duckdb` command-line tool, so query from Python:

```python
import duckdb
con = duckdb.connect("local/data/nyc311_local.duckdb", read_only=True)

# Share of closed requests the city acted on, and the share it could not decode.
con.sql("""
    select count(*) filter (where is_actioned) * 1.0 / count(*)                 as pct_actioned_of_closed,
           count(*) filter (where closure_type = 'Undecodable') * 1.0 / count(*) as pct_undecodable
    from gold.fct_service_requests
    where is_resolved
""").show()

# Closure rates stay NULL until 30 complete days follow a day (is_denominator_closed).
con.sql("""
    select is_denominator_closed, count(*) as groups,
           count(pct_closed_within_window) as groups_publishing_a_rate
    from gold.fct_daily_volume group by 1
""").show()

# 3-day recurrence, excluding chronic locations and closures too recent to judge.
con.sql("""
    select closure_type,
           avg(case when days_to_next_same_complaint <= 3 then 1.0 else 0.0 end) as recurred_3d
    from gold.fct_complaint_recurrence
    where not is_chronic_location and observation_days >= 3
    group by 1 order by 2 desc
""").show()
```

Expect different numbers: `--live` fetches the 37 days ending today, not 13–24 Aug. On a fresh `--live` database, closure rates publish only for the oldest five or so days of the window; the rest stay NULL. Only the Access Failed ranking has held across specifications. In the recurrence query, leave out `Unspecified` (no resolution text) and `Undecodable` (text no rule matches): they are an absence of information, not something the city did, and `Unspecified` can top the ranking.
