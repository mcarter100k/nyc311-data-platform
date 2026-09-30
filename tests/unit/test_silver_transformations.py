"""
Unit tests for local/silver_transformations.py.

Each test builds an in-memory DataFrame with known inputs and asserts exact
output. No database, no network.
"""

import csv
import os
import sys

import pytest

pytest.importorskip("pandas", reason="pandas not installed — skipping Silver unit tests")

import pandas as pd  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "local"))

from silver_transformations import (  # noqa: E402
    BOROUGH_MAP,
    KNOWN_BOROUGH_VARIANTS,
    compute_dq_metrics,
    compute_resolution_days,
    deduplicate_on_unique_key,
    drop_quarantined,
    failure_rate,
    parse_timestamps,
    select_quarantine,
    standardize_borough,
    unrecognized_borough_mask,
)


# ── 1. Borough standardization ────────────────────────────────────────────────

def test_borough_standardization():
    """Every raw variant maps to the correct canonical name; nothing maps to null."""
    cases = [
        ("BROOKLYN", "BROOKLYN"), ("Bklyn", "BROOKLYN"), ("bk", "BROOKLYN"),
        ("KINGS", "BROOKLYN"), ("Kings County", "BROOKLYN"),
        ("MANHATTAN", "MANHATTAN"), ("MN", "MANHATTAN"), ("New York", "MANHATTAN"),
        ("new york city", "MANHATTAN"), ("NY", "MANHATTAN"),
        ("QUEENS", "QUEENS"), ("Qns", "QUEENS"), ("Queens County", "QUEENS"),
        ("BRONX", "BRONX"), ("The Bronx", "BRONX"), ("BX", "BRONX"),
        ("STATEN ISLAND", "STATEN ISLAND"), ("SI", "STATEN ISLAND"),
        ("Richmond", "STATEN ISLAND"),
        ("UNSPECIFIED", "UNSPECIFIED"),
        ("FAKE_BOROUGH", "UNSPECIFIED"),   # unrecognized -> UNSPECIFIED, never null
        (None, "UNSPECIFIED"),             # null -> UNSPECIFIED
        ("   ", "UNSPECIFIED"),            # whitespace -> UNSPECIFIED
    ]
    df = pd.DataFrame({"id": range(len(cases)), "borough": [c[0] for c in cases]})
    out = standardize_borough(df)

    for i, (raw, expected) in enumerate(cases):
        assert out.loc[i, "borough"] == expected, (
            f"borough={raw!r} -> {out.loc[i, 'borough']!r}, expected {expected!r}"
        )
    assert out["borough"].notna().all(), (
        "A null borough breaks the NOT NULL contract on dim_location."
    )


def test_unrecognized_borough_is_not_the_unspecified_bucket():
    """The DQ numerator counts decoder failures only.

    Missing, unrecognized and the source's literal 'Unspecified' all become
    'UNSPECIFIED'; only a supplied value that matches no variant is a failure.
    """
    df = pd.DataFrame({
        "borough": [
            "Brooklyn",       # recognized variant
            "Unspecified",    # RECOGNIZED: the source's own explicit sentinel
            None,             # missing, not a decode failure
            "   ",            # blank, not a decode failure
            "Woodside",       # the only real decode failure here
        ],
        "unique_key": ["a", "b", "c", "d", "e"],
        "resolution_days": pd.array([1, 1, 1, 1, 1], dtype="Int64"),
    })
    derived = standardize_borough(df)

    assert list(unrecognized_borough_mask(derived)) == [False, False, False, False, True], (
        "Only a SUPPLIED borough that matches no variant is a decode failure."
    )
    assert (derived["borough"] == "UNSPECIFIED").sum() == 4, (
        "Four of the five still collapse to UNSPECIFIED — which is exactly why "
        "the mask cannot be derived from the standardized column."
    )

    rows = compute_dq_metrics(df, derived, run_date="2024-01-01")
    boro = {r["check_name"]: r for r in rows}["unrecognized_borough"]
    assert (boro["records_failed"], boro["records_checked"]) == (1, 5)


def test_borough_map_comes_from_the_shared_csv():
    """The map equals the CSV both dbt projects seed from, row for row."""
    with open(os.path.join(ROOT, "config", "borough_variants.csv"), newline="") as fh:
        expected = {r["variant"]: r["canonical"] for r in csv.DictReader(fh)}
    assert BOROUGH_MAP == expected, (
        "BOROUGH_MAP differs from config/borough_variants.csv; a local copy "
        "drifts from the dbt seed."
    )
    assert KNOWN_BOROUGH_VARIANTS == set(BOROUGH_MAP)


# ── 2. Resolution days ────────────────────────────────────────────────────────

def test_resolution_days_calculation():
    """All created/closed combinations, including the ones that are easy to get wrong."""
    df = pd.DataFrame({
        "id":           ["r1", "r2", "r3", "r4", "r5"],
        "created_date": ["2024-01-01", "2024-01-01", "2024-01-10", None,         "2024-01-10"],
        "closed_date":  ["2024-01-05", "2024-01-01", None,         "2024-01-05", "2024-01-05"],
        "status":       ["Closed", "Closed", "Open", "Closed", "Closed"],
    })
    out = compute_resolution_days(parse_timestamps(df))
    got = dict(zip(out["id"], out["resolution_days"], strict=True))

    assert got["r1"] == 4
    assert got["r2"] == 0, "Same-day close must be 0, not null — null would hide it from SLA metrics."
    assert pd.isna(got["r3"]), "Open request must be null, not 0 — 0 would read as instant resolution."
    assert pd.isna(got["r4"]), "No created_date means the interval is uncomputable."
    assert got["r5"] == -5, "Closed-before-created must surface as negative, not be suppressed here."


# ── 3. Deduplication ──────────────────────────────────────────────────────────

def test_deduplication():
    """One row per unique_key survives, and it is the most recently ingested."""
    df = pd.DataFrame({
        "unique_key":        ["a", "a", "b", "c", "c", "c"],
        "_ingest_timestamp": ["2024-01-01T00:00", "2024-01-02T00:00",
                              "2024-01-01T00:00",
                              "2024-01-03T00:00", "2024-01-01T00:00", "2024-01-02T00:00"],
        "status":            ["Open", "Closed", "Open", "Closed", "Open", "Open"],
    })
    out = deduplicate_on_unique_key(df)

    assert len(out) == 3, f"Expected 3 unique keys, got {len(out)}"
    assert out["unique_key"].is_unique
    got = dict(zip(out["unique_key"], out["_ingest_timestamp"], strict=True))
    assert got["a"] == "2024-01-02T00:00", "Must keep the LATEST ingest, not an arbitrary row."
    assert got["c"] == "2024-01-03T00:00"


def test_deduplication_under_production_conditions():
    """Stage 3 stamps one timestamp on every row, so every duplicate is a tie.

    The later fetch is the fresher read and must win for every key.
    """
    n = 500
    df = pd.DataFrame({
        "unique_key":        [f"k{i % n}" for i in range(2 * n)],
        "page":              ["first"] * n + ["second"] * n,
        "_ingest_timestamp": ["2026-08-22T00:00:00"] * (2 * n),
    })

    out = deduplicate_on_unique_key(df)

    assert len(out) == n, f"Expected {n} unique keys, got {len(out)}"
    survivors = out["page"].value_counts().to_dict()
    assert survivors == {"second": n}, (
        f"Every survivor must come from the later page; got {survivors}. A mix "
        f"means ties are being broken arbitrarily rather than by fetch order."
    )
    assert deduplicate_on_unique_key(df).equals(out), (
        "Identical input must produce identical output."
    )


# ── 4. Quarantine ─────────────────────────────────────────────────────────────

def test_quarantine_selects_only_negative_resolution_days():
    """Catches data-entry errors; never catches open requests."""
    df = pd.DataFrame({
        "unique_key":      ["ok", "same_day", "open", "bad"],
        "resolution_days": pd.array([4, 0, None, -5], dtype="Int64"),
    })
    bad = select_quarantine(df)
    kept = drop_quarantined(df)

    assert list(bad["unique_key"]) == ["bad"], (
        "Only the negative row is a data-entry error. An open request has NULL "
        "resolution_days, which must never compare as negative."
    )
    assert sorted(kept["unique_key"]) == ["ok", "open", "same_day"]
    assert len(bad) + len(kept) == len(df), "Quarantine must partition, not drop."


# ── 5. Data quality metrics ───────────────────────────────────────────────────

def test_data_quality_metrics():
    """Exact counts for the five checks that feed fct_data_quality.

    The call site (which frame local_runner passes) is covered by
    tests/local/test_stage3_dq_metrics.py.
    """
    bronze = pd.DataFrame({
        "unique_key":   ["a", "a", None, "d"],           # 1 null, 1 duplicate
        "created_date": ["2024-01-01", "2024-01-01", "2024-01-01", None],  # 1 null
    })
    # Post-dedup (4 -> 3), PRE-quarantine, derived columns present.
    deduped = pd.DataFrame({
        "unique_key":      ["a", None, "d"],
        "resolution_days": pd.array([-1, 3, None], dtype="Int64"),         # 1 invalid
        "borough":         ["BROOKLYN", "UNSPECIFIED", "QUEENS"],
        # The RAW spellings behind those canonical names. 'Woodside' matches no
        # variant and is the one decode failure; 'UNSPECIFIED' is a recognized
        # variant and must NOT be counted — see
        # test_unrecognized_borough_is_not_the_unspecified_bucket.
        "_borough_raw":    ["Brooklyn", "Woodside", "Queens"],              # 1 unrecognized
    })
    rows = compute_dq_metrics(bronze, deduped, run_date="2024-01-01")
    by = {r["check_name"]: r for r in rows}

    assert len(rows) == 5, "fct_data_quality's accepted_values test expects exactly these five checks."

    r = by["null_rate_unique_key"]
    assert (r["records_checked"], r["records_failed"]) == (4, 1), (
        "Null rates are measured on BRONZE: a null unique_key cannot survive "
        "dedup and would be invisible if measured afterwards."
    )
    assert r["failure_rate"] == 0.25          # literal, not failure_rate(1, 4)
    assert r["pipeline_stage"] == "silver"
    assert r["run_date"] == "2024-01-01"

    assert by["null_rate_created_date"]["records_failed"] == 1
    assert by["duplicate_rate"]["records_checked"] == 4, "Measured against bronze."
    assert by["duplicate_rate"]["records_failed"] == 1, "4 bronze rows -> 3 deduped = 1 duplicate."
    assert by["invalid_resolution_days"]["records_failed"] == 1
    assert by["invalid_resolution_days"]["records_checked"] == 3, (
        "Measured against the deduped, PRE-quarantine frame — the population "
        "the rule was applied to. A post-quarantine denominator would be 2, "
        "which excludes the very row the numerator counts."
    )
    assert by["unrecognized_borough"]["records_failed"] == 1
    assert by["unrecognized_borough"]["records_checked"] == 3

    # Structural invariant, independent of these fixture numbers: no check may
    # report more failures than it checked, and the two deduped-population
    # checks must share one denominator.
    for check in rows:
        assert check["records_failed"] <= check["records_checked"], (
            f"{check['check_name']} failed {check['records_failed']} of "
            f"{check['records_checked']} — a denominator that excludes its own "
            f"numerator is the defect this assertion exists to catch."
        )


def test_failure_rate_handles_zero_checked():
    """An empty run reports 0.0, not a ZeroDivisionError."""
    assert failure_rate(0, 0) == 0.0
    assert failure_rate(1, 3) == 0.333333
