"""
Silver transformation logic for the local pipeline, with no database access.

`local_runner.py` does the I/O (reading the raw file, writing Silver and the DQ
log). Every function here takes a DataFrame and returns a DataFrame or a plain
value, so unit tests can hand it a few rows and assert exact output.

The borough mapping is loaded from `config/borough_variants.csv`, which both
dbt projects also load as a seed.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd

BOROUGH_VARIANTS_CSV = Path(__file__).resolve().parent.parent / "config" / "borough_variants.csv"


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
    dim_location. unrecognized_borough_mask keeps the distinction for the DQ
    metric.
    """
    if pd.isna(val) or str(val).strip() == "":
        return "UNSPECIFIED"
    return BOROUGH_MAP.get(str(val).upper().strip(), "UNSPECIFIED")


def standardize_borough(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse every borough spelling variant to the five canonical names.

    The raw value is kept in `_borough_raw`, because standardizing maps missing,
    unrecognized and the source's literal 'Unspecified' to the same string, and
    `unrecognized_borough_mask` must tell them apart. local_runner drops it
    before writing Silver.
    """
    out = df.copy()
    raw = out["borough"] if "borough" in out.columns else pd.Series(dtype=str, index=out.index)
    out["_borough_raw"] = raw
    out["borough"] = raw.apply(standardize_borough_value)
    return out


def unrecognized_borough_mask(df: pd.DataFrame) -> pd.Series:
    """True where a borough value WAS supplied and no variant matched it.

    Not `borough == 'UNSPECIFIED'`, which mixes three different cases:

        borough is null / blank      the source said nothing
        borough is 'Unspecified'     a RECOGNIZED variant in the CSV
        borough is something else    no variant matched: the actual failure
    """
    if "_borough_raw" not in df.columns:
        return pd.Series(False, index=df.index)

    raw = df["_borough_raw"]
    as_text = raw.astype("string").str.strip()
    supplied = as_text.notna() & (as_text != "")
    recognized = as_text.str.upper().isin(KNOWN_BOROUGH_VARIANTS)
    return (supplied & ~recognized).fillna(False)


def deduplicate_on_unique_key(df: pd.DataFrame) -> pd.DataFrame:
    """One row per unique_key: newest ingest wins, later fetch breaks the tie.

    Pages can overlap, so a key can arrive twice in one run; the later copy is
    the fresher read. A run stamps one _ingest_timestamp on every row, so
    _fetch_position breaks ties, and a stable mergesort makes the result
    independent of input order. Output is in fetch order.
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

    Socrata sends naive NYC-local timestamps and the warehouse stores them as
    TIMESTAMP_NTZ; labelling them UTC would shift every value by the machine's
    offset.
    """
    out = df.copy()
    for col in ("created_date", "closed_date", "resolution_action_updated_date"):
        out[col] = pd.to_datetime(out[col], errors="coerce") if col in out.columns else pd.NaT
    return out


def compute_resolution_days(df: pd.DataFrame) -> pd.DataFrame:
    """Add resolution_days (whole 24-hour periods, created to closed) for the quarantine.

    Not stored in Silver: Gold's calendar-day resolution_days is the one
    published definition. NULL for open requests; negative values are kept,
    because they are what quarantine_mask looks for.
    """
    out = df.copy()
    has_both = out["closed_date"].notna() & out["created_date"].notna()
    out["resolution_days"] = pd.NA
    if has_both.any():
        out.loc[has_both, "resolution_days"] = (
            (out.loc[has_both, "closed_date"] - out.loc[has_both, "created_date"])
            .dt.days.astype("Int64")
        )
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
    """The five data quality checks for one Silver run, as data_quality_log rows.

      df_bronze   every row as fetched, before dedup and quarantine.
      df_deduped  one row per unique_key with derived columns, BEFORE
                  quarantine: the population every quality rule runs over.

    The post-quarantine frame is not a parameter: as a denominator it would
    exclude the very rows a check counts as failures.

      null_rate_unique_key    / df_bronze   (dedup would hide null keys)
      null_rate_created_date  / df_bronze
      duplicate_rate          / df_bronze   (failed = |bronze| - |deduped|)
      invalid_resolution_days / df_deduped
      unrecognized_borough    / df_deduped  (numerator: unrecognized_borough_mask)
    """
    n_bronze = len(df_bronze)
    n_deduped = len(df_deduped)
    n_null_uk = int(df_bronze["unique_key"].isna().sum()) if "unique_key" in df_bronze else n_bronze
    n_null_cd = int(df_bronze["created_date"].isna().sum()) if "created_date" in df_bronze else 0
    n_dupes = n_bronze - n_deduped
    n_invalid = int(quarantine_mask(df_deduped).sum())
    n_unrecognized = int(unrecognized_borough_mask(df_deduped).sum())

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
        row("invalid_resolution_days", n_invalid, n_deduped),
        row("unrecognized_borough", n_unrecognized, n_deduped),
    ]
