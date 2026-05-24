"""
conftest.py
===========
Shared pytest fixtures and environment setup for the entire test suite.

All environment variables must be set BEFORE any airflow module is imported,
which is why they live at module level here (conftest.py is the first file
pytest loads).

Windows / Python 3.14 compatibility notes
------------------------------------------
Airflow officially supports POSIX systems only.  Two issues arise when running
tests locally on Windows with Python 3.14:

1. sqlite:///:memory:  is treated as a *relative* path by Airflow 3.x.
   Fix: use an absolute path to a temp-dir file.

2. Airflow's structlog setup references airflow.utils.log.colored_formatter
   inside logging.config.dictConfig().  On Python 3.14, airflow.utils is not
   yet fully initialised at that point (circular import → AttributeError).
   Fix: AIRFLOW__CORE__COLORED_CONSOLE_LOG=False switches the formatter to
   stdlib logging.Formatter, which has no airflow dependency.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

# ── Fix 1: absolute SQLite path ───────────────────────────────────────────────
_db = Path(tempfile.gettempdir()) / "airflow_unit_tests.db"
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", f"sqlite:///{_db.as_posix()}")

# ── Fix 2: disable colored logging (avoids circular import on Py 3.14/Win) ───
# With the default True, Airflow's structlog config references
# airflow.utils.log.colored_formatter.CustomFormatter at dictConfig() time.
# On Python 3.14 that module isn't fully loaded yet → AttributeError.
# Setting False makes structlog fall back to stdlib logging.Formatter.
os.environ.setdefault("AIRFLOW__CORE__COLORED_CONSOLE_LOG", "False")

# ── General Airflow test config ───────────────────────────────────────────────
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
os.environ.setdefault(
    "AIRFLOW_HOME",
    str(Path(tempfile.gettempdir()) / "airflow_unit_home"),
)


@pytest.fixture
def failure_context() -> dict[str, Any]:
    """Minimal Airflow task context dict for on_failure_callback tests."""
    dag_mock = MagicMock()
    dag_mock.dag_id = "test_dag"

    ti_mock = MagicMock()
    ti_mock.task_id = "test_task"

    return {
        "dag": dag_mock,
        "task_instance": ti_mock,
        "run_id": "manual__2024-03-15T00:00:00+00:00",
        "logical_date": datetime(2024, 3, 15, 0, 0, 0),
        "exception": ValueError("something went wrong"),
    }
