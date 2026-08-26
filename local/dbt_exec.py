"""
Resolve the dbt console script for a given Python interpreter.

No shebang: this is an importable module that also runs as
`python local/dbt_exec.py`, never as `./local/dbt_exec.py` (ruff EXE001).

Why this module exists
----------------------
`python -m dbt` DOES NOT WORK. dbt-core ships no `dbt/__main__.py`, so the
invocation dies with:

    No module named dbt.__main__; 'dbt' is a package and cannot be directly executed

This is still true on the version this repo pins (dbt-core 1.12.2) — it is not
a legacy-1.7 quirk, and the two comments that used to say "dbt-core 1.7" were
stale. The supported entrypoints are the `dbt` console script and the
`dbt.cli.main` Python API; `-m` is neither.

The trap is that `import dbt.cli.main` SUCCEEDS while `python -m dbt` FAILS, so
an import check is not a valid guard for a `-m` invocation. run_tests.sh made
exactly that mistake and exited 1 before running a single test.

Why it lives here, and why there is only one copy
-------------------------------------------------
This resolution logic was independently duplicated in local/local_runner.py and
tests/local/conftest.py. This repo already carries scripts/check_model_drift.py
and scripts/check_claims.py because duplicated definitions here have caused real
bugs, so a third copy would have been a finding rather than a fix.

It is extracted into local/ for the same reason silver_transformations.py is:
local/ is not a package, so a sibling module is importable by local_runner.py
with no ceremony, and tests reach it with the sys.path.insert idiom already used
by tests/local/test_module_imports.py and tests/local/test_live_fetch.py.

run_tests.sh does not reimplement this in bash — it EXECUTES this file and reads
the path off stdout (see `__main__` below). Bash cannot import Python, and a
bash port would have recreated the duplication this module exists to remove.
One definition, three consumers.
"""

import os
import shutil
import sys


def dbt_executable(python_executable: str | None = None) -> str | None:
    """Absolute path to the `dbt` console script, or None if it is not found.

    Prefers the script installed alongside `python_executable` (defaults to the
    running interpreter) so that an activated virtualenv wins over whatever
    unrelated `dbt` happens to be earlier on PATH. Falls back to PATH.
    """
    python_executable = python_executable or sys.executable
    candidate = os.path.join(os.path.dirname(python_executable), "dbt")
    if os.path.exists(candidate):
        return candidate
    return shutil.which("dbt")


if __name__ == "__main__":
    # CLI shim for run_tests.sh: print the resolved path, or exit 1 with no
    # output when dbt is not installed for this interpreter. Callers branch on
    # the exit code.
    resolved = dbt_executable()
    if not resolved:
        sys.exit(1)
    print(resolved)
