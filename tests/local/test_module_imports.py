"""
Every module in local/ must import. A name ruff reports as unused in one module
can be another module's import (reconcile.py imports from local_runner), and
nothing else in the suite imports reconcile. Lives in this tier because the
imports need pandas, duckdb and requests.
"""

import importlib
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOCAL_DIR = os.path.join(ROOT, "local")

# Listed, not globbed, so a module disappearing fails the next test instead of
# silently shrinking the parametrisation.
LOCAL_MODULES = [
    "dbt_exec",
    "ingest_config",
    "local_runner",
    "reconcile",
    "silver_transformations",
]


def test_module_list_matches_the_directory():
    """The list above must not drift from what is actually on disk."""
    on_disk = {
        f[:-3] for f in os.listdir(LOCAL_DIR)
        if f.endswith(".py") and not f.startswith("_")
    }
    assert on_disk == set(LOCAL_MODULES), (
        f"local/ contains {sorted(on_disk)} but this test parametrises "
        f"{sorted(LOCAL_MODULES)}. Update LOCAL_MODULES so the new module is "
        f"covered — an unlisted module is an untested one."
    )


@pytest.mark.parametrize("module_name", LOCAL_MODULES)
def test_local_module_imports_cleanly(module_name):
    """Importing the module must not raise — see this file's docstring."""
    if LOCAL_DIR not in sys.path:
        sys.path.insert(0, LOCAL_DIR)
    importlib.import_module(module_name)
