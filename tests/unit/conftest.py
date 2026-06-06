"""
Shared fixtures for the Silver unit tests.

Provides a session-scoped SparkSession so all four test modules share one
JVM startup — the most expensive part of running PySpark tests locally.

Also adds databricks/notebooks/ to sys.path so test files can import
silver_transformations without installing it as a package.
"""

import os
import sys

import pytest

# Force Spark workers to use the same Python interpreter as the test driver.
# Without this, PySpark defaults to the system Python (3.9 on macOS) while
# pytest may run under a different interpreter, causing syntax errors in the
# worker when it imports PySpark's own type annotations.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# Make silver_transformations importable without installation
_NOTEBOOKS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "databricks", "notebooks",
)
if _NOTEBOOKS_DIR not in sys.path:
    sys.path.insert(0, _NOTEBOOKS_DIR)

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed — skipping unit tests")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("nyc311-silver-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
