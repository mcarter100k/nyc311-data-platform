{% docs __overview__ %}

# NYC 311 Data Platform

New York City logs about 10,000 non-emergency complaints a day (noise, potholes, illegal parking) through its 311 line. This site documents the tables the pipeline builds from them: every table, every column, the tests on each, and how they connect.

Source code, findings and how it runs: [github.com/mcarter100k/nyc311-data-platform](https://github.com/mcarter100k/nyc311-data-platform)

### Where to start

- Click the **blue lineage icon** (bottom right) to see how data flows from the raw source to the final tables.
- Open **`fct_service_requests`**, the central table: one row per complaint.

### The tables

Data moves through three layers: **staging** renames and types the cleaned source, **intermediate** applies the business rules, and **marts** are the tables analysts query, arranged as a star schema.

| Table | What one row is |
|---|---|
| `fct_service_requests` | One complaint, with its status, timings and closure type |
| `fct_daily_volume` | Complaint counts for one day, borough and category |
| `fct_complaint_recurrence` | One closed complaint: did the same complaint come back to the same address? |
| `fct_data_quality` | One data-quality check from one run |
| `dim_agency` | One version of a city agency; old names are kept, so old complaints keep the name they had |
| `dim_date` | One calendar day, 2010–2030, with holiday flags |
| `dim_location` | One borough, community board and ZIP code |

### What runs where

This site is generated from the Snowflake version of the project, which is designed and validated but not deployed. The same models, differing only in SQL dialect, run every day on DuckDB; CI fails if the two copies drift apart.

{% enddocs %}
