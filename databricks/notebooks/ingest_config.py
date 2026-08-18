"""
Pure request-building logic for the Socrata ingest notebook (01_ingest_raw.py).

No Databricks or Spark APIs — unit-testable with plain Python, following the
same notebook/module split as silver_transformations.py.

The incremental watermark deliberately keys on Socrata's system field
`:updated_at` (row last modified), NOT `created_date` (when the complaint was
filed). 311 requests mutate after creation — status flips to Closed days or
weeks later, closed_date gets set. A created_date predicate fetches each row
exactly once, on its creation day, and never sees those updates; every
downstream update path (the Silver MERGE's whenMatchedUpdate, the fct
incremental merge) would be dead code and resolution metrics would only ever
count same-day closures. `:updated_at` fetches both new and touched rows;
the Silver MERGE on unique_key absorbs the overlap idempotently (rerunning
produces the same result).
"""

SOCRATA_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 50_000  # Socrata hard maximum per request


def build_page_params(load_type: str, run_date: str, page: int) -> dict:
    """Query parameters for one page of the Socrata fetch.

    load_type: 'incremental' fetches rows created OR updated on/after run_date
               via the :updated_at system field; 'full' fetches everything.
    run_date:  YYYY-MM-DD execution date.
    page:      zero-based page number; offset = page * PAGE_SIZE.
    """
    params = {
        "$limit": PAGE_SIZE,
        "$order": ":id",  # stable ordering prevents missed rows across pages
        "$offset": page * PAGE_SIZE,
    }
    if load_type == "incremental":
        params["$where"] = f":updated_at >= '{run_date}T00:00:00'"
    return params


def batch_output_path(output_path: str, batch_index: int) -> str:
    """Sub-directory for one flushed batch of a full load."""
    return f"{output_path}/batch_{batch_index:05d}"


def final_output_path(load_type: str, output_path: str, batch_index: int) -> str:
    """Where the final in-memory batch is written.

    Full loads MUST land the final partial batch in its own batch_* sub-
    directory: a mode=overwrite write to output_path itself deletes every
    previously flushed batch directory underneath it, silently discarding all
    but the last ~500k records. Incremental loads never flush, so the single
    write to output_path root is safe there.
    """
    if load_type == "full":
        return batch_output_path(output_path, batch_index)
    return output_path
