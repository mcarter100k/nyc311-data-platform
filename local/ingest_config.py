"""Socrata query parameters for the --live fetch, kept free of I/O so they are unit-testable.

The fetch filters on created_date, not :updated_at, because :updated_at is
mass re-stamped nightly (ADR 010). Re-pulling the whole created window every
run still picks up status changes for rows inside it.
"""

SOCRATA_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 50_000  # Socrata's maximum rows per request


def build_page_params(run_date: str, page: int) -> dict:
    """Parameters for one page of rows created on or after `run_date` (YYYY-MM-DD)."""
    return {
        "$limit": PAGE_SIZE,
        "$order": ":id",  # a stable sort, so offset paging neither skips nor repeats rows
        "$offset": page * PAGE_SIZE,
        "$where": f"created_date >= '{run_date}T00:00:00'",
    }
