"""
Import health for the modules in local/ — the code that actually runs daily.

Why this tier and not the structural one. The check has to be a *real* import
to be worth anything, and importing these modules pulls pandas, duckdb and
requests. The structural tier is deliberately dependency-light (it installs dbt
and pytest, nothing else) so it can stay a seconds-long gate; this tier already
installs local/requirements.txt, so the real thing runs here for free.

Why it exists at all. A cleanup deleted an import from local_runner that ruff
reported as unused. It *was* unused there — but reconcile.py imported it FROM
local_runner as a re-export, so reconcile broke at its import line, and the
entire 93-test suite stayed green, because nothing in it imports reconcile.

A pipeline module that cannot be imported is broken regardless of what it
contains, and that was untested. Re-exports make it worse than it sounds: the
name a linter sees as dead in one module can be another module's only source
for it, and neither file reads as wrong on its own.
"""

import importlib
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOCAL_DIR = os.path.join(ROOT, "local")

# Every module in local/. Listed explicitly rather than globbed so that adding a
# module is a deliberate act that shows up in review, and so a file disappearing
# fails loudly here instead of silently shrinking the parametrisation.
LOCAL_MODULES = [
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
