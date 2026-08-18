"""
Unit tests for silver_transformations.py.

Each test builds an in-memory Spark DataFrame with known inputs and makes exact
assertions about the output. No cloud credentials, Delta tables, or Databricks
APIs are used — the SparkSession runs in local mode against inline data only.

Plain-English summary of what is being tested and why:

  test_borough_standardization
    The Silver notebook receives borough values as free-text strings entered by
    311 call-center operators. The same borough appears as "Brooklyn", "BKLYN",
    "BK", etc. Gold reporting breaks if these aren't collapsed to a consistent
    canonical name. This test checks every known variant for all five boroughs,
    plus the UNSPECIFIED sentinel and an unrecognized value to verify the
    otherwise-branch returns UNSPECIFIED rather than null.

  test_resolution_days_calculation
    Resolution time (days from created → closed) is the primary SLA metric in
    the Gold layer. Edge cases are common: same-day closes should be 0 not null;
    open tickets should be null not 0; records where closed_date precedes
    created_date must produce a negative integer (not be silently dropped here —
    they're quarantined later by the notebook). This test verifies all four cases.

  test_deduplication
    The Bronze table receives one API page per run. Page boundaries occasionally
    overlap, so the same unique_key can appear twice with different _ingest_
    timestamps. dropDuplicates picks an arbitrary row; the Window-based function
    picks the most recent one deterministically. This test verifies that: (a) only
    one row per unique_key survives, and (b) the row with the largest timestamp wins.

  test_data_quality_metrics
    The DQ log must contain accurate failure counts for five checks so that the
    fct_data_quality Gold model can compute rolling 7-day breach flags. This test
    creates three DataFrames with precisely known defects and asserts each returned
    Row carries the correct records_checked, records_failed, and failure_rate.
"""

from datetime import date, datetime

import pytest

from silver_transformations import (
    compute_dq_metrics,
    compute_resolution_days,
    compute_unique_key_checksum,
    deduplicate_on_unique_key,
    failure_rate,
    standardize_borough,
)


# ── 1. Borough standardization ────────────────────────────────────────────────

def test_borough_standardization(spark):
    """Every raw borough variant maps to the correct canonical name.

    Canonical output values: BROOKLYN, MANHATTAN, QUEENS, BRONX, STATEN ISLAND,
    UNSPECIFIED. No other values should ever appear after Silver processing.
    """
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([
        StructField("id",      StringType(), True),
        StructField("borough", StringType(), True),
    ])

    # (id, raw_borough) → expected standardized value
    cases = [
        # BROOKLYN variants
        ("b1", "BROOKLYN",       "BROOKLYN"),
        ("b2", "Bklyn",          "BROOKLYN"),
        ("b3", "brooklyn",       "BROOKLYN"),
        ("b4", "BK",             "BROOKLYN"),
        ("b5", "KINGS",          "QUEENS"),
        ("b6", "Kings County",   "BROOKLYN"),
        # MANHATTAN variants
        ("m1", "MANHATTAN",      "MANHATTAN"),
        ("m2", "MN",             "MANHATTAN"),
        ("m3", "New York",       "MANHATTAN"),
        ("m4", "new york city",  "MANHATTAN"),
        # QUEENS variants
        ("q1", "QUEENS",         "QUEENS"),
        ("q2", "Qns",            "QUEENS"),
        ("q3", "Queens County",  "QUEENS"),
        # BRONX variants
        ("x1", "BRONX",          "BRONX"),
        ("x2", "The Bronx",      "BRONX"),
        ("x3", "BX",             "BRONX"),
        # STATEN ISLAND variants
        ("s1", "STATEN ISLAND",  "STATEN ISLAND"),
        ("s2", "SI",             "STATEN ISLAND"),
        ("s3", "Richmond",       "STATEN ISLAND"),
        # Sentinel — already standardized
        ("u1", "UNSPECIFIED",    "UNSPECIFIED"),
        # Unrecognized value — must map to UNSPECIFIED, not null
        ("z1", "FAKE_BOROUGH",   "UNSPECIFIED"),
        # Null value — falls through all WHEN clauses → UNSPECIFIED
        ("z2", None,             "UNSPECIFIED"),
    ]

    raw_data    = [(id_, raw) for id_, raw, _ in cases]
    expected    = {id_: want for id_, _, want in cases}

    df_raw      = spark.createDataFrame(raw_data, schema=schema)
    df_result   = standardize_borough(df_raw)
    result_rows = {r["id"]: r["borough"] for r in df_result.collect()}

    for id_, expected_value in expected.items():
        actual = result_rows[id_]
        assert actual == expected_value, (
            f"id={id_!r}: raw={raw_data[[r[0] for r in raw_data].index(id_)][1]!r} "
            f"→ got {actual!r}, expected {expected_value!r}"
        )

    # The unrecognized value must produce UNSPECIFIED, never null
    assert result_rows["z1"] is not None, (
        "Unrecognized borough mapped to null instead of UNSPECIFIED. "
        "Null borough breaks the dim_location NOT NULL constraint in Gold."
    )
    assert result_rows["z2"] is not None, (
        "Null raw borough mapped to null output instead of UNSPECIFIED."
    )


# ── 2. Resolution days calculation ────────────────────────────────────────────

def test_resolution_days_calculation(spark):
    """resolution_days is computed correctly for all created/closed date combinations.

    Verified cases:
      - Normal close (days > 0)
      - Same-day close (days = 0, not null)
      - Open request (null closed_date → null resolution_days, not 0)
      - Null created_date (cannot compute → null, even if closed_date present)
      - Negative case (closed_date < created_date → negative integer, not filtered here)
    """
    from pyspark.sql.types import DateType, StringType, StructField, StructType

    schema = StructType([
        StructField("id",           StringType(), True),
        StructField("created_date", DateType(),   True),
        StructField("closed_date",  DateType(),   True),
        StructField("status",       StringType(), True),
    ])

    data = [
        # (id, created,            closed,             status,   expected_days, expected_resolved)
        ("r1", date(2024, 1, 1),  date(2024, 1, 5),  "Closed"),  # 4 days
        ("r2", date(2024, 1, 1),  date(2024, 1, 1),  "Closed"),  # 0 days — same-day close
        ("r3", date(2024, 1, 10), None,               "Open"),    # null — open request
        ("r4", None,              date(2024, 1, 5),   "Closed"),  # null — missing created
        ("r5", date(2024, 1, 10), date(2024, 1, 5),  "Closed"),  # -5 — negative (data error)
    ]

    expected_days     = {"r1": 4, "r2": 0, "r3": None, "r4": None, "r5": -5}
    expected_resolved = {"r1": True, "r2": True, "r3": False, "r4": True, "r5": True}

    df_input  = spark.createDataFrame(data, schema=schema)
    df_result = compute_resolution_days(df_input)
    rows      = {r["id"]: r for r in df_result.collect()}

    for id_ in expected_days:
        actual_days = rows[id_]["resolution_days"]
        assert actual_days == expected_days[id_], (
            f"id={id_}: resolution_days={actual_days}, expected={expected_days[id_]}"
        )
        actual_resolved = rows[id_]["is_resolved"]
        assert actual_resolved == expected_resolved[id_], (
            f"id={id_}: is_resolved={actual_resolved}, expected={expected_resolved[id_]}"
        )

    # Same-day close must be 0 not null — null would hide resolved SLA metrics
    assert rows["r2"]["resolution_days"] == 0, (
        "Same-day close produced null resolution_days instead of 0. "
        "Null hides same-day resolutions from SLA reporting."
    )

    # Open request must be null not 0 — 0 would incorrectly imply same-day resolution
    assert rows["r3"]["resolution_days"] is None, (
        "Open request (null closed_date) produced 0 instead of null resolution_days. "
        "0 days would misrepresent open tickets as same-day resolves."
    )


# ── 3. Deduplication ─────────────────────────────────────────────────────────

def test_deduplication(spark):
    """Only one row per unique_key survives; the most recent _ingest_timestamp wins.

    Verifies:
      - A duplicated unique_key collapses to exactly one row
      - The row with the larger _ingest_timestamp is retained
      - A non-duplicated unique_key passes through unchanged
      - A null unique_key row is preserved (it is quarantined downstream, not here)
    """
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    schema = StructType([
        StructField("unique_key",        StringType(),   True),
        StructField("_ingest_timestamp", TimestampType(), True),
        StructField("payload",           StringType(),   True),
    ])

    ts = lambda s: datetime.fromisoformat(s)

    data = [
        # Two rows for key1 — older should be discarded
        ("key1", ts("2024-01-01T10:00:00"), "old_payload"),
        ("key1", ts("2024-01-01T11:00:00"), "new_payload"),  # winner
        # Single row for key2 — must survive unchanged
        ("key2", ts("2024-01-01T09:00:00"), "only_row"),
        # Null unique_key — quarantined downstream, but must survive dedup step
        (None,   ts("2024-01-01T08:00:00"), "null_key_row"),
    ]

    df_input  = spark.createDataFrame(data, schema=schema)
    df_result = deduplicate_on_unique_key(df_input)
    rows      = df_result.collect()

    # Total row count
    assert len(rows) == 3, (
        f"Expected 3 rows after dedup (key1, key2, null), got {len(rows)}. "
        f"Rows: {rows}"
    )

    rows_by_key = {}
    for r in rows:
        rows_by_key[r["unique_key"]] = r

    # key1: most recent timestamp must win
    assert rows_by_key["key1"]["payload"] == "new_payload", (
        f"key1 retained the older row (payload={rows_by_key['key1']['payload']!r}). "
        f"deduplicate_on_unique_key must keep the most recent _ingest_timestamp."
    )

    # key2: single row, untouched
    assert rows_by_key["key2"]["payload"] == "only_row"

    # null key: present — quarantine happens in the notebook, not this function
    assert None in rows_by_key, (
        "Null unique_key row was dropped during deduplication. "
        "Null-key records should survive dedup and be quarantined later."
    )

    # No _rn column leaked into the output schema
    output_columns = df_result.columns
    assert "_rn" not in output_columns, (
        "_rn rank column was not dropped from the output DataFrame."
    )


# ── 4. Data quality metrics ───────────────────────────────────────────────────

def test_data_quality_metrics(spark):
    """compute_dq_metrics returns accurate failure counts for each of the five checks.

    Test setup — three DataFrames with precisely known defects:

      df_bronze  (4 rows):
        - Row with null unique_key     → counts toward null_rate_unique_key
        - Row with null created_date   → counts toward null_rate_created_date
        - Two rows sharing unique_key "k3" (one duplicate) → duplicate_rate

      df_deduped (3 rows after collapsing the k3 duplicate):
        - Row with borough "FAKE_BOROUGH" → counts toward unrecognized_borough

      df_derived (3 rows, post-transformation):
        - Row with resolution_days = -5 → counts toward invalid_resolution_days

    Expected DQ row values are derived from those counts and checked exactly.
    """
    from pyspark.sql.types import (
        IntegerType, StringType, StructField, StructType,
    )

    bronze_schema = StructType([
        StructField("unique_key",    StringType(),  True),
        StructField("created_date",  StringType(),  True),
        StructField("borough",       StringType(),  True),
    ])

    deduped_schema = StructType([
        StructField("unique_key",    StringType(),  True),
        StructField("borough",       StringType(),  True),
    ])

    derived_schema = StructType([
        StructField("unique_key",      StringType(),  True),
        StructField("resolution_days", IntegerType(), True),
    ])

    # df_bronze: 4 rows — 1 null key, 1 null date, 1 real duplicate
    df_bronze = spark.createDataFrame(
        [
            (None,  "2024-01-01", "BROOKLYN"),       # null unique_key
            ("k2",  None,        "FAKE_BOROUGH"),    # null created_date
            ("k3",  "2024-01-01", "QUEENS"),         # first occurrence of k3
            ("k3",  "2024-01-01", "QUEENS"),         # duplicate — will be dropped
        ],
        schema=bronze_schema,
    )

    # df_deduped: 3 rows (k3 collapsed), retaining raw borough values
    # FAKE_BOROUGH is unrecognized and will trip the unrecognized_borough check
    df_deduped = spark.createDataFrame(
        [
            (None,  "BROOKLYN"),
            ("k2",  "FAKE_BOROUGH"),    # unrecognized borough
            ("k3",  "QUEENS"),
        ],
        schema=deduped_schema,
    )

    # df_derived: 3 rows with resolution_days computed
    # k2 has resolution_days = -5 — closed_date before created_date (data entry error)
    df_derived = spark.createDataFrame(
        [
            (None,  None),   # no resolution (null key, will be quarantined)
            ("k2",  -5),     # negative resolution_days — invalid
            ("k3",  3),      # valid 3-day resolution
        ],
        schema=derived_schema,
    )

    rows = compute_dq_metrics(df_bronze, df_deduped, df_derived, "2024-01-01")
    by_check = {r.check_name: r for r in rows}

    # Five checks must always be present — if one is missing the DQ log is incomplete
    expected_checks = {
        "null_rate_unique_key",
        "null_rate_created_date",
        "duplicate_rate",
        "invalid_resolution_days",
        "unrecognized_borough",
    }
    assert set(by_check.keys()) == expected_checks, (
        f"Returned checks: {set(by_check.keys())}. Expected: {expected_checks}"
    )

    # null_rate_unique_key: 1 out of 4 bronze rows has null unique_key
    r = by_check["null_rate_unique_key"]
    assert r.records_checked == 4
    assert r.records_failed  == 1
    assert r.failure_rate    == failure_rate(1, 4)
    assert r.pipeline_stage  == "silver"
    assert r.run_date        == "2024-01-01"

    # null_rate_created_date: 1 out of 4 bronze rows has null created_date
    r = by_check["null_rate_created_date"]
    assert r.records_checked == 4
    assert r.records_failed  == 1
    assert r.failure_rate    == failure_rate(1, 4)

    # duplicate_rate: 4 bronze rows → 3 after dedup → 1 dropped
    r = by_check["duplicate_rate"]
    assert r.records_checked == 4
    assert r.records_failed  == 1
    assert r.failure_rate    == failure_rate(1, 4)

    # invalid_resolution_days: 1 out of 3 deduped rows has resolution_days < 0
    r = by_check["invalid_resolution_days"]
    assert r.records_checked == 3
    assert r.records_failed  == 1
    assert r.failure_rate    == failure_rate(1, 3)

    # unrecognized_borough: 1 out of 3 deduped rows has an unrecognized borough
    r = by_check["unrecognized_borough"]
    assert r.records_checked == 3
    assert r.records_failed  == 1
    assert r.failure_rate    == failure_rate(1, 3)


# ── 5. Unique-key checksum ────────────────────────────────────────────────────

def test_unique_key_checksum_is_order_independent(spark):
    """compute_unique_key_checksum produces identical (checksum, key_count) tuples
    regardless of the order rows appear in the DataFrame.

    Verified properties:
      - Same keys in 3 different orderings → identical checksum and key_count
      - Different key set → different checksum
      - Null keys are excluded from the hash; key_count reflects only non-null keys
      - Empty DataFrame → ("", 0)

    The order-independence property is what prevents false DATA CONTRACT VIOLATION
    alerts on legitimate replays: Spark makes no row-ordering guarantee, so without
    sort_array two runs of identical data can produce different hashes.
    """
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField("unique_key", StringType(), True)])

    keys = ["ZZZ-001", "AAA-003", "MMM-002"]

    ordering_1 = spark.createDataFrame([(k,) for k in keys],           schema=schema)
    ordering_2 = spark.createDataFrame([(k,) for k in reversed(keys)], schema=schema)
    ordering_3 = spark.createDataFrame([(k,) for k in sorted(keys)],   schema=schema)

    cs1, n1 = compute_unique_key_checksum(ordering_1)
    cs2, n2 = compute_unique_key_checksum(ordering_2)
    cs3, n3 = compute_unique_key_checksum(ordering_3)

    assert cs1 == cs2 == cs3, (
        "Checksum differs across row orderings — sort_array may be missing. "
        f"ordering_1={cs1[:16]!r}, ordering_2={cs2[:16]!r}, ordering_3={cs3[:16]!r}"
    )
    assert n1 == n2 == n3 == len(keys), (
        f"key_count differs across orderings: {n1}, {n2}, {n3}"
    )
    assert len(cs1) == 64, f"SHA-256 hex digest should be 64 chars, got {len(cs1)}"

    # Different key set must produce a different checksum
    different = spark.createDataFrame([("XYZ-999",), ("ABC-000",)], schema=schema)
    cs_diff, _ = compute_unique_key_checksum(different)
    assert cs_diff != cs1, "Different key sets produced the same checksum."

    # Null keys must be excluded from the hash; adding nulls must not change
    # the checksum or the count
    with_nulls = spark.createDataFrame(
        [(k,) for k in keys] + [(None,), (None,)], schema=schema
    )
    cs_nulls, n_nulls = compute_unique_key_checksum(with_nulls)
    assert cs_nulls == cs1, (
        "Null keys changed the checksum — they should be filtered before hashing."
    )
    assert n_nulls == len(keys), (
        f"key_count includes null rows: got {n_nulls}, expected {len(keys)}"
    )

    # Empty DataFrame must return ("", 0) — not raise or return None
    empty_df = spark.createDataFrame([], schema=schema)
    cs_empty, n_empty = compute_unique_key_checksum(empty_df)
    assert cs_empty == "", f"Empty DataFrame checksum should be '' not {cs_empty!r}"
    assert n_empty == 0,   f"Empty DataFrame key_count should be 0 not {n_empty}"
