"""
Find the dbt executable next to the running Python, falling back to PATH.

Use it instead of `python -m dbt`, which fails: dbt-core has no __main__.
Used by local_runner.py, tests/local/conftest.py and run_tests.sh (which runs
this file and reads the path from stdout). No shebang: run it as
`python local/dbt_exec.py`.
"""

import os
import shutil
import sys


def dbt_executable(python_executable: str | None = None) -> str | None:
    """Absolute path to the `dbt` console script, or None if it is not found.

    Prefers the script beside `python_executable` (default: the running
    interpreter), so an activated virtualenv wins over another dbt on PATH.
    """
    python_executable = python_executable or sys.executable
    candidate = os.path.join(os.path.dirname(python_executable), "dbt")
    if os.path.exists(candidate):
        return candidate
    return shutil.which("dbt")


if __name__ == "__main__":
    # For run_tests.sh: print the path, or exit 1 with no output.
    resolved = dbt_executable()
    if not resolved:
        sys.exit(1)
    print(resolved)
