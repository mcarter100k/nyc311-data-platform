"""Socrata query parameters for the --live fetch, kept free of I/O so they are unit-testable.

The fetch filters on created_date, not :updated_at, because :updated_at is
mass re-stamped nightly (ADR 010). Re-pulling the whole created window every
run still picks up status changes for rows inside it.

The window is fetched one day per query. Sorting a multi-week range by :id is
slow at the source (a 37-day page timed out at 300 s on 2026-09-30, while one
day took 1-4 s), and a 60 s read timeout is the pipeline's limit.
"""

from datetime import date, timedelta

SOCRATA_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 50_000  # Socrata's maximum rows per request


def build_page_params(day: str, page: int, *, open_ended: bool = False) -> dict:
    """Parameters for one page of rows created on `day` (YYYY-MM-DD).

    With open_ended, the page covers `day` and everything after it, so the
    window's last slice also takes rows stamped later than today.
    """
    where = f"created_date >= '{day}T00:00:00'"
    if not open_ended:
        next_day = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
        where += f" and created_date < '{next_day}T00:00:00'"
    return {
        "$limit": PAGE_SIZE,
        "$order": ":id",  # a stable sort, so offset paging neither skips nor repeats rows
        "$offset": page * PAGE_SIZE,
        "$where": where,
    }
