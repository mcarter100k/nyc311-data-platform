# ADR 014: Transform before load — Bronze is the raw file, not a warehouse table

**Status:** Accepted
**Date:** 2026-08-21
**Amends:** [ADR 004](004-medallion-vs-elt.md) (medallion vs ELT — its Bronze/Silver tooling is superseded here)
**Relates to:** [ADR 008](008-prototype-scope.md) (what is spec vs what runs)

## Context

The Bronze→Silver step was neither ETL nor ELT, and the ambiguity was
structural rather than cosmetic. Stage 2 wrote every raw row into
`bronze.service_requests`, and stage 3 immediately pulled the same rows back
out again:

```python
df_bronze = con.execute("SELECT * FROM bronze.service_requests").df()
```

Data went into the warehouse, out of it, and back in — a full materialisation
and re-read that bought nothing. The transform ran in pandas either way.

The shape is a fossil. ADR 004 assigned Silver to Databricks/PySpark, where
out-of-warehouse compute is the entire point and a materialised Bronze in
object storage is the natural handoff. The Databricks path was removed
(2026-08-20) and the engine became pandas, but the arrangement it was built
for stayed behind. ADR 004 still describes Bronze as Databricks-written,
append-only and immutable, none of which has been true since.

## Decision

**Transform before load.** The raw file is Bronze. The only service-request
data written into the warehouse is the cleaned Silver table.

Bronze is registered as a **view** over the raw JSON rather than deleted
outright:

```sql
CREATE OR REPLACE VIEW bronze.service_requests AS
SELECT *, '<mtime>' AS _ingest_timestamp, '<name>' AS _source_file
FROM read_json_auto('<path>')
```

Two reasons for the view rather than nothing at all. Architecturally it keeps
the layer nameable without copying it — the medallion's Bronze contract is
"raw, exactly as received", and a view over the received bytes satisfies that
more literally than a transformed copy does. Practically, raw stays
SQL-queryable, which is how fields Gold drops (`council_district`, `bbl`,
`police_precinct`) remain reachable without re-fetching from the API.

`_ingest_timestamp` is taken from the raw file's mtime, not `now()`, so the
stamp describes the data rather than the run that happened to look at it.
Re-running stage 2 no longer rewrites when Bronze claims its rows arrived.

## Consequences

**The pipeline is now honestly ETL at that boundary.** Extract writes a file;
transform runs in pandas against that file; load writes one clean table. The
Silver→Gold half remains ELT via dbt, and that split is now deliberate rather
than accidental.

**Smaller database.** 37 MB → 26 MB, the entire difference being the Bronze
copy that no longer exists.

**Two things this deliberately does NOT fix**, stated because it would be easy
to assume otherwise:

1. **The memory ceiling stands.** `pd.DataFrame(json.load(fh))` still holds
   every row in memory. Removing the round-trip removed a redundant write and
   read, not the ceiling. Only moving the Silver transform into SQL would do
   that, and that is the ELT direction this ADR does not take.
2. **Bronze is still not immutable.** The raw file is overwritten on every
   run, so it is a rolling 7-day window, exactly as the materialised table was.
   ADR 004's "append-only, immutable once written, replay from any checkpoint"
   remains false locally. Making it true is a separate change.

**A new coupling.** The view holds an absolute path to the raw file. Move or
delete that file and `bronze.service_requests` fails at query time rather than
at pipeline time. The reconciliation script's `raw file = bronze` check is what
would catch it, and it now compares the file against a view of itself — a
weaker check than before, which is an accepted cost.

**Amendment, 2026-08-22 — the exported artifact.** The consequence above was
recorded but its most important instance was missed. `daily-run.yml` uploads
the DuckDB file as evidence, described in the workflow as *"needed for the
postmortem"*. A database downloaded on its own now has a Bronze layer that
raises `IO Error: No files found` on first query, because the view points at a
path that exists only on the runner. Verified by moving the raw file aside and
querying the copy: Gold and Silver returned 61,528 rows each; Bronze failed.

Two changes follow. The workflow now uploads the raw JSON alongside the
database, so the bundle is self-contained. And because the stored path is still
the runner's, anyone querying a downloaded bundle should read the file directly
rather than through the view:

```sql
SELECT * FROM read_json_auto('nyc311_raw.json');   -- Bronze, portably
```

The view is a convenience for the machine that produced it. The raw file is the
layer, and it travels.

**Idempotency has a sharp edge worth recording.** `DROP TABLE IF EXISTS`
does *not* tolerate the object already being a view: `IF EXISTS` suppresses
"not found", not "wrong type". The first conversion run passed and every run
after it raised `CatalogException` until the drop was made type-aware. Found
by running the stage twice, which is the only way this class of bug surfaces.
