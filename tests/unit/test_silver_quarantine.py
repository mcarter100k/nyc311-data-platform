"""
Unit tests for the quarantine path in silver_transformations.py.

The replaceWhere predicate test is a regression guard for a latent crash: the
notebook previously overwrote the quarantine table with
`replaceWhere "run_date = ..."`, but quarantined records carry the Bronze
audit column `_run_date`, not `run_date`. The write therefore threw an
unresolved-column error on the FIRST run that quarantined anything — the bug
only fired when the path it guarded was finally exercised.
"""

import re

from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType,
)

from silver_transformations import (
    quarantine_partition_predicate,
    select_quarantine,
)


QUARANTINE_INPUT_SCHEMA = StructType([
    StructField("unique_key",      StringType(),  True),
    StructField("resolution_days", IntegerType(), True),
    StructField("_run_date",       StringType(),  True),
])


def _quarantine_frame(spark):
    data = [
        (None,  3,    "2024-06-01"),   # null key → quarantine
        ("k1",  -5,   "2024-06-01"),   # negative resolution → quarantine
        ("k2",  4,    "2024-06-01"),   # clean → not quarantined
        ("k3",  None, "2024-06-01"),   # open request → not quarantined
    ]
    return spark.createDataFrame(data, schema=QUARANTINE_INPUT_SCHEMA)


def test_select_quarantine_picks_exactly_the_critical_failures(spark):
    """Null-key and negative-resolution rows are quarantined with the right
    reasons; clean and open rows pass through untouched."""
    result = select_quarantine(_quarantine_frame(spark)).collect()
    reasons = {r["unique_key"]: r["quarantine_reason"] for r in result}
    assert len(result) == 2, f"Expected 2 quarantined rows, got {result}"
    assert reasons[None] == "null_unique_key"
    assert reasons["k1"] == "negative_resolution_days"


def test_replace_where_predicate_references_a_column_the_rows_carry(spark):
    """The overwrite predicate may only reference columns that exist on the
    quarantine DataFrame — otherwise the first real quarantine write crashes
    the Silver job. Fails against the historical `run_date = ...` predicate."""
    df_quarantine = select_quarantine(_quarantine_frame(spark))
    predicate = quarantine_partition_predicate("2024-06-01")

    referenced = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", predicate.split("=")[0])
    assert referenced, f"Could not parse a column name from {predicate!r}"
    column = referenced[0]

    assert column in df_quarantine.columns, (
        f"replaceWhere predicate {predicate!r} references {column!r}, which is "
        f"not a column of the quarantine DataFrame ({df_quarantine.columns}). "
        f"Delta would fail the write with an unresolved-column error."
    )
