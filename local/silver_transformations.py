"""
Silver transformation logic for the local pipeline.

`local_runner.py` handles all I/O (reading Bronze from DuckDB, writing Silver
and the DQ log). This module holds the transformation logic, and nothing here
touches a database — which is the entire point: the code that decides what a
row *means* is the code most worth unit-testing, and it cannot be tested while
it is interleaved with connection handling.

Every function takes a DataFrame and returns a DataFrame (or a plain value),
so a test can hand it three hand-built rows and assert exact output.

Provenance: these rules previously existed twice — here in pandas, inline in
`local_runner.stage3_silver`, and again in a PySpark module for a Databricks
deployment that was specified but never provisioned. The PySpark copy was
unit-tested while this one, which actually runs every day, was not. The
Databricks path was removed and its tests ported here, onto the code that runs.

The borough mapping is NOT defined in this file. It is loaded from
`config/borough_variants.csv`, the single source of truth shared with both dbt
projects (which load it as a seed).
"""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

BOROUGH_VARIANTS_CSV = Path(__file__).resolve().parent.parent / "config" / "borough_variants.csv"

CANONICAL_BOROUGHS = {
    "BROOKLYN", "MANHATTAN", "QUEENS", "BRONX", "STATEN ISLAND", "UNSPECIFIED",
}


def load_borough_map(path: Path | None = None) -> dict:
    """{raw spelling -> canonical borough} from the shared CSV."""
    with open(path or BOROUGH_VARIANTS_CSV, newline="") as fh:
        return {r["variant"]: r["canonical"] for r in csv.DictReader(fh)}


BOROUGH_MAP = load_borough_map()

# Every recognized input spelling — the denominator for the
# unrecognized_borough data quality check.
KNOWN_BOROUGH_VARIANTS = set(BOROUGH_MAP)


def standardize_borough_value(val) -> str:
    """One raw borough string -> its canonical form.

    Null, empty, and unrecognized values all collapse to UNSPECIFIED rather
    than to null: a null borough would break the NOT NULL contract on
    dim_location, and 'unrecognized' is information worth keeping distinct
    from 'missing' only at the DQ-metric level, not in the dimension.
    """
    if pd.isna(val) or str(val).strip() == "":
        return "UNSPECIFIED"
    return BOROUGH_MAP.get(str(val).upper().strip(), "UNSPECIFIED")


def standardize_borough(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse every borough spelling variant to the five canonical names."""
    out = df.copy()
    out["borough"] = out.get("borough", pd.Series(dtype=str)).apply(standardize_borough_value)
    return out


def deduplicate_on_unique_key(df: pd.DataFrame) -> pd.DataFrame:
    """One row per unique_key: newest ingest wins, later fetch breaks the tie.

    API pagination overlaps at page boundaries, so the same unique_key can
    arrive twice in one run. The later copy is the fresher read of a row that
    may have changed between page requests, so it is the one to keep.

    Why the tiebreaker is load-bearing rather than defensive. The previous
    version sorted on `_ingest_timestamp` alone and called that deterministic.
    It is not, for two compounding reasons:

      1. A run stamps ONE timestamp across the whole frame (stage 3 assigns
         `df["_ingest_timestamp"] = <mtime>`), so in production every row ties
         and the sort key carries no information at all.
      2. `sort_values` defaults to quicksort, which is NOT stable, so tied rows
         come out in an unspecified order.

    Measured: 1,000 keys duplicated across two pages, run against five
    shuffles of the same input, produced five different survivor sets — and
    even at fixed order the survivors were a 997/3 mix of the two pages rather
    than either one cleanly.

    `_fetch_position` makes the ordering total, and `kind="mergesort"` makes it
    stable, so the result depends only on the input rows and not on their
    arrival order or on pandas' internals. Output is returned in fetch order.
    """
    if "unique_key" not in df.columns:
        return df.reset_index(drop=True)

    ordered = df.reset_index(drop=True)
    by, desc = ["_fetch_position"], [False]
    if "_ingest_timestamp" in ordered.columns:
        by, desc = ["_ingest_timestamp", "_fetch_position"], [False, False]

    return (
        ordered.assign(_fetch_position=ordered.index)
               .sort_values(by, ascending=desc, kind="mergesort")
               .drop_duplicates(subset=["unique_key"], keep="first")
               .sort_values("_fetch_position")
               .drop(columns="_fetch_position")
               .reset_index(drop=True)
    )


def parse_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce the three date columns, NAIVE — never utc=True.

    Socrata sends naive NYC-local timestamps and the warehouse contract stores
    them as TIMESTAMP_NTZ. Labelling them UTC here shifted every value by the
    machine's offset, which surfaced downstream as wrong calendar dates for
    after-midnight rows.
    """
    out = df.copy()
    for col in ("created_date", "closed_date", "resolution_action_updated_date"):
        out[col] = pd.to_datetime(out[col], errors="coerce") if col in out.columns else pd.NaT
    return out


def compute_resolution_days(df: pd.DataFrame) -> pd.DataFrame:
    """Days from created to closed, plus is_resolved.

    NULL for open requests, not 0 — a zero would be indistinguishable from a
    same-day close and would drag every average toward zero. Negative values
    are produced here rather than suppressed: they are a real data-entry
    signal, and select_quarantine decides what happens to them.
    """
    out = df.copy()
    has_both = out["closed_date"].notna() & out["created_date"].notna()
    out["resolution_days"] = pd.NA
    if has_both.any():
        out.loc[has_both, "resolution_days"] = (
            (out.loc[has_both, "closed_date"] - out.loc[has_both, "created_date"])
            .dt.days.astype("Int64")
        )
    out["is_resolved"] = out.get("status", pd.Series(dtype=str)) == "Closed"
    return out


def quarantine_mask(df: pd.DataFrame) -> pd.Series:
    """True where resolution_days is negative (closed before created).

    to_numeric rather than astype(float): the column is nullable Int64 and
    astype raises on pd.NA, while to_numeric maps NA to NaN, which compares
    False — so open requests are never quarantined.
    """
    return df["resolution_days"].notna() & (
        pd.to_numeric(df["resolution_days"], errors="coerce") < 0
    )


def select_quarantine(df: pd.DataFrame) -> pd.DataFrame:
    """The rows that fail the closed-before-created check."""
    return df[quarantine_mask(df)].reset_index(drop=True)


def drop_quarantined(df: pd.DataFrame) -> pd.DataFrame:
    """Everything that survives the quality filter."""
    return df[~quarantine_mask(df)].reset_index(drop=True)


def failure_rate(failed: int, checked: int) -> float:
    """failed / checked, rounded to 6dp. Zero checked is 0.0, not a crash."""
    return round(failed / checked, 6) if checked else 0.0


def compute_dq_metrics(
    df_bronze: pd.DataFrame,
    df_deduped: pd.DataFrame,
    run_date: str,
) -> list:
    """The five data quality checks for one Silver run.

    TWO populations, and each check states which one it is measured against.
    Getting this wrong is not a rounding error — it changes what the metric
    MEANS, and it did: see the note at the end of this docstring.

      df_bronze   every row as fetched, before deduplication and before the
                  quality filter. |df_bronze| is the run's input size.
      df_deduped  one row per unique_key, with the derived columns already
                  computed (borough standardized, resolution_days present),
                  and BEFORE quarantining. This is the population every
                  quality RULE is evaluated over.

    The post-quarantine frame — the rows that survive and get written to
    silver.service_requests — is deliberately NOT a parameter. No check may be
    measured against it, because it is defined as "the rows that passed", so
    using it as a denominator produces a rate whose denominator excludes its
    own numerator.

    Check by check:

      null_rate_unique_key    / df_bronze. Must be pre-dedup: dedup collapses
                                null unique_keys, so a null measured afterwards
                                would be invisible.
      null_rate_created_date  / df_bronze. Same population as the check above
                                so the two null rates are comparable.
      duplicate_rate          / df_bronze. Numerator is |bronze| - |deduped| —
                                the rows deduplication actually removed —
                                over the rows deduplication actually saw.
      invalid_resolution_days / df_deduped. The quarantine rule is applied to
                                the deduped frame, so the deduped frame is what
                                it was checked against. Its failures are a
                                SUBSET of this denominator, which is the point.
      unrecognized_borough    / df_deduped. Borough standardization runs on the
                                same frame; a row quarantined for a bad date
                                still had a borough that was or was not
                                recognized, so it belongs in this denominator.

    Why one deduped frame and not two. This function used to take `df_deduped`
    and `df_derived` separately, taking counts from the first and masks from
    the second. At the only call site they are the same population, so the
    split bought nothing and cost a great deal: local_runner passed the
    POST-quarantine frame as `df_deduped`, which made `duplicate_rate` report
    duplicates + quarantined rows (in practice: quarantined rows, since the
    fetch contains no duplicate unique_keys) and gave the other two checks a
    denominator that excluded their own numerator. One parameter makes that
    class of mistake unrepresentable — the count and the mask now provably
    come from the same rows.

    Returns rows shaped for SILVER.data_quality_log; the caller writes them.
    """
    n_bronze = len(df_bronze)
    n_deduped = len(df_deduped)
    n_null_uk = int(df_bronze["unique_key"].isna().sum()) if "unique_key" in df_bronze else n_bronze
    n_null_cd = int(df_bronze["created_date"].isna().sum()) if "created_date" in df_bronze else 0
    # Rows removed by deduplication. Equivalently |bronze| - |distinct
    # unique_key|, since that is precisely what deduplicate_on_unique_key does.
    n_dupes = n_bronze - n_deduped
    n_invalid = int(quarantine_mask(df_deduped).sum())
    n_unrecognized = (
        int((df_deduped["borough"] == "UNSPECIFIED").sum()) if "borough" in df_deduped else 0
    )

    def row(name, failed, checked):
        return {
            "run_date": run_date,
            "check_name": name,
            "records_checked": checked,
            "records_failed": failed,
            "failure_rate": failure_rate(failed, checked),
            "pipeline_stage": "silver",
        }

    return [
        row("null_rate_unique_key", n_null_uk, n_bronze),
        row("null_rate_created_date", n_null_cd, n_bronze),
        row("duplicate_rate", n_dupes, n_bronze),
        # Both of these are checked-against df_deduped, the frame their masks
        # were computed on — never the post-quarantine frame.
        row("invalid_resolution_days", n_invalid, n_deduped),
        row("unrecognized_borough", n_unrecognized, n_deduped),
    ]
