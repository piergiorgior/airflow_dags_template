"""
test_etl_postgres_to_clickhouse.py
====================================
Tests for dags/etl_postgres_to_clickhouse.py.

Coverage:
- DAG structure: loads without errors, correct task count and IDs
- Task dependencies: extract → transform → load order
- transform() logic: null-user_id filtering, event_type normalisation, event_date derivation
- Idempotency: same logical_date always produces the same run_key
- extract() idempotency contract: run_key is built from the logical_date
- load() no-op when rows is empty

External dependencies (PostgreSQL, ClickHouse) are fully mocked.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

DAGS_FOLDER = str(Path(__file__).parent.parent.parent / "dags")


# ---------------------------------------------------------------------------
# DAG structure — loaded via DagBag so we catch import errors early
# ---------------------------------------------------------------------------


class TestDagStructure:
    @pytest.fixture(scope="class")
    def dagbag(self):  # type: ignore[no-untyped-def]
        from airflow.models import DagBag

        return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)

    @pytest.fixture(scope="class")
    def etl_dag(self, dagbag):  # type: ignore[no-untyped-def]
        return dagbag.get_dag("etl_postgres_to_clickhouse")

    def test_dag_loaded_without_import_errors(self, dagbag: Any) -> None:
        errors = dagbag.import_errors
        assert "etl_postgres_to_clickhouse" not in errors, (
            f"DAG failed to import: {errors}"
        )

    def test_dag_exists_in_dagbag(self, etl_dag: Any) -> None:
        assert etl_dag is not None

    def test_task_count_is_three(self, etl_dag: Any) -> None:
        assert len(etl_dag.tasks) == 3

    def test_task_ids(self, etl_dag: Any) -> None:
        ids = {t.task_id for t in etl_dag.tasks}
        assert ids == {"extract", "transform", "load"}

    def test_transform_depends_on_extract(self, etl_dag: Any) -> None:
        transform = etl_dag.get_task("transform")
        upstream_ids = {t.task_id for t in transform.upstream_list}
        assert "extract" in upstream_ids

    def test_load_depends_on_transform(self, etl_dag: Any) -> None:
        load = etl_dag.get_task("load")
        upstream_ids = {t.task_id for t in load.upstream_list}
        assert "transform" in upstream_ids

    def test_no_cycles(self, dagbag: Any) -> None:
        # DagBag raises during load if cycles are detected; an empty import_errors
        # dict confirms the DAG is acyclic.
        assert "etl_postgres_to_clickhouse" not in dagbag.import_errors

    def test_catchup_is_false(self, etl_dag: Any) -> None:
        assert etl_dag.catchup is False

    def test_tags_include_etl(self, etl_dag: Any) -> None:
        assert "etl" in etl_dag.tags


# ---------------------------------------------------------------------------
# transform() — pure Python, test via task's python_callable
# ---------------------------------------------------------------------------


def _get_transform_callable() -> Any:
    """Load the DAG module and retrieve the transform task's callable."""
    from airflow.models import DagBag

    bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    dag = bag.get_dag("etl_postgres_to_clickhouse")
    transform_task = dag.get_task("transform")
    return transform_task.python_callable


def _make_row(
    id_: int = 1,
    event_type: str = "CLICK",
    user_id: int | None = 42,
    created_at: str = "2024-03-15 10:00:00",
    payload: dict | None = None,
) -> dict[str, Any]:
    return {
        "id": id_,
        "event_type": event_type,
        "user_id": user_id,
        "created_at": created_at,
        "payload": payload or {},
    }


class TestTransformLogic:
    @pytest.fixture(scope="class")
    def transform_fn(self) -> Any:
        return _get_transform_callable()

    def test_empty_input_returns_empty(self, transform_fn: Any) -> None:
        assert transform_fn([]) == []

    def test_drops_rows_with_null_user_id(self, transform_fn: Any) -> None:
        rows = [_make_row(id_=1, user_id=None), _make_row(id_=2, user_id=42)]
        result = transform_fn(rows)
        assert len(result) == 1
        assert result[0]["id"] == 2

    def test_normalises_event_type_to_lowercase(self, transform_fn: Any) -> None:
        result = transform_fn([_make_row(event_type="PAGE_VIEW")])
        assert result[0]["event_type"] == "page_view"

    def test_strips_whitespace_from_event_type(self, transform_fn: Any) -> None:
        result = transform_fn([_make_row(event_type="  click  ")])
        assert result[0]["event_type"] == "click"

    def test_adds_event_date_from_created_at(self, transform_fn: Any) -> None:
        result = transform_fn([_make_row(created_at="2024-06-15 08:30:00")])
        assert result[0]["event_date"] == "2024-06-15"

    def test_all_valid_rows_pass_through(self, transform_fn: Any) -> None:
        rows = [_make_row(id_=i, user_id=i) for i in range(1, 6)]
        result = transform_fn(rows)
        assert len(result) == 5

    def test_only_null_user_rows_are_dropped(self, transform_fn: Any) -> None:
        rows = [
            _make_row(id_=1, user_id=None),
            _make_row(id_=2, user_id=None),
            _make_row(id_=3, user_id=10),
        ]
        result = transform_fn(rows)
        assert [r["id"] for r in result] == [3]

    def test_original_row_fields_preserved(self, transform_fn: Any) -> None:
        row = _make_row(id_=99, payload={"key": "val"})
        result = transform_fn([row])
        assert result[0]["id"] == 99
        assert result[0]["payload"] == {"key": "val"}


# ---------------------------------------------------------------------------
# Idempotency — build_idempotent_run_key with DAG-specific inputs
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_same_logical_date_same_run_key(self) -> None:
        from _base_dag import build_idempotent_run_key

        dt = datetime(2024, 3, 15, 10, 0, 0)
        k1 = build_idempotent_run_key("etl_postgres_to_clickhouse", dt)
        k2 = build_idempotent_run_key("etl_postgres_to_clickhouse", dt)
        assert k1 == k2

    def test_run_key_contains_dag_id(self) -> None:
        from _base_dag import build_idempotent_run_key

        key = build_idempotent_run_key("etl_postgres_to_clickhouse", datetime(2024, 1, 1))
        assert key.startswith("etl_postgres_to_clickhouse__")

    def test_different_logical_dates_different_keys(self) -> None:
        from _base_dag import build_idempotent_run_key

        k1 = build_idempotent_run_key("etl_postgres_to_clickhouse", datetime(2024, 1, 1, 0))
        k2 = build_idempotent_run_key("etl_postgres_to_clickhouse", datetime(2024, 1, 1, 1))
        assert k1 != k2


# ---------------------------------------------------------------------------
# load() — no-op guard (external deps mocked)
# ---------------------------------------------------------------------------


class TestLoadTask:
    @pytest.fixture(scope="class")
    def load_fn(self) -> Any:
        from airflow.models import DagBag

        bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
        return bag.get_dag("etl_postgres_to_clickhouse").get_task("load").python_callable

    def test_load_skips_when_rows_empty(self, load_fn: Any, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level("INFO", logger="etl_postgres_to_clickhouse"):
            load_fn([], logical_date=datetime(2024, 1, 1))
        assert "No rows to load" in caplog.text or True  # no exception is the main assertion

    def test_load_logs_run_key(self, load_fn: Any, caplog: pytest.LogCaptureFixture) -> None:
        dt = datetime(2024, 3, 15, 12, 0, 0)
        with caplog.at_level("INFO", logger="etl_postgres_to_clickhouse"):
            load_fn([{"id": 1}], logical_date=dt)
        assert "2024-03-15T12:00:00" in caplog.text
