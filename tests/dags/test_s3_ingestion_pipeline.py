"""
test_s3_ingestion_pipeline.py
==============================
Tests for dags/s3_ingestion_pipeline.py.

Coverage:
- DAG structure: loads without errors, correct task count and IDs
- Task dependencies: wait_for_s3_marker → extract_from_s3 → transform → load_to_clickhouse
- _parse_file(): CSV parsing, NDJSON parsing, malformed lines skipped, unknown format
- _s3_date_prefix(): correct prefix construction
- transform(): required-field validation, event_type normalisation, event_date derivation,
               deduplication (last-write-wins on updated_at), passthrough of valid rows
- load_to_clickhouse(): skip on empty, DROP PARTITION call, bulk_insert call, run_key logging

All external dependencies (S3, ClickHouse) are fully mocked.
"""

from __future__ import annotations

import textwrap
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

DAGS_FOLDER = str(Path(__file__).parent.parent.parent / "dags")


# ---------------------------------------------------------------------------
# Pure helper functions — importable without Airflow context
# ---------------------------------------------------------------------------


def _import_helpers() -> Any:
    """Import helper functions from the DAG module via DagBag."""
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location(
        "s3_ingestion_pipeline",
        Path(DAGS_FOLDER) / "s3_ingestion_pipeline.py",
    )
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules.setdefault("s3_ingestion_pipeline", mod)
    return mod


# ---------------------------------------------------------------------------
# DAG structure
# ---------------------------------------------------------------------------


class TestDagStructure:
    @pytest.fixture(scope="class")
    def dagbag(self):  # type: ignore[no-untyped-def]
        from airflow.models import DagBag

        return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)

    @pytest.fixture(scope="class")
    def s3_dag(self, dagbag):  # type: ignore[no-untyped-def]
        return dagbag.get_dag("s3_ingestion_pipeline")

    def test_dag_loads_without_errors(self, dagbag: Any) -> None:
        assert "s3_ingestion_pipeline" not in dagbag.import_errors

    def test_dag_exists(self, s3_dag: Any) -> None:
        assert s3_dag is not None

    def test_task_count(self, s3_dag: Any) -> None:
        assert len(s3_dag.tasks) == 4

    def test_task_ids(self, s3_dag: Any) -> None:
        ids = {t.task_id for t in s3_dag.tasks}
        assert ids == {
            "wait_for_s3_marker",
            "extract_from_s3",
            "transform",
            "load_to_clickhouse",
        }

    def test_extract_depends_on_sensor(self, s3_dag: Any) -> None:
        extract = s3_dag.get_task("extract_from_s3")
        assert "wait_for_s3_marker" in {t.task_id for t in extract.upstream_list}

    def test_transform_depends_on_extract(self, s3_dag: Any) -> None:
        transform = s3_dag.get_task("transform")
        assert "extract_from_s3" in {t.task_id for t in transform.upstream_list}

    def test_load_depends_on_transform(self, s3_dag: Any) -> None:
        load = s3_dag.get_task("load_to_clickhouse")
        assert "transform" in {t.task_id for t in load.upstream_list}

    def test_catchup_is_false(self, s3_dag: Any) -> None:
        assert s3_dag.catchup is False

    def test_schedule_is_daily(self, s3_dag: Any) -> None:
        assert s3_dag.schedule_interval == "@daily" or str(s3_dag.schedule_interval) == "@daily"

    def test_tags_include_s3_and_clickhouse(self, s3_dag: Any) -> None:
        assert "s3" in s3_dag.tags
        assert "clickhouse" in s3_dag.tags


# ---------------------------------------------------------------------------
# _s3_date_prefix
# ---------------------------------------------------------------------------


class TestS3DatePrefix:
    @pytest.fixture(scope="class")
    def fn(self):  # type: ignore[no-untyped-def]
        from airflow.models import DagBag

        DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
        import sys

        return sys.modules["s3_ingestion_pipeline"]._s3_date_prefix

    def test_basic_prefix(self, fn: Any) -> None:
        result = fn("raw/events", datetime(2024, 3, 15))
        assert result == "raw/events/2024/03/15/"

    def test_trailing_slash(self, fn: Any) -> None:
        result = fn("data", datetime(2024, 1, 1))
        assert result.endswith("/")

    def test_zero_padded_month_and_day(self, fn: Any) -> None:
        result = fn("p", datetime(2024, 1, 5))
        assert "/01/05/" in result


# ---------------------------------------------------------------------------
# _parse_file
# ---------------------------------------------------------------------------


class TestParseFile:
    @pytest.fixture(scope="class")
    def fn(self):  # type: ignore[no-untyped-def]
        from airflow.models import DagBag

        DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
        import sys

        return sys.modules["s3_ingestion_pipeline"]._parse_file

    def test_csv_parses_header_and_rows(self, fn: Any) -> None:
        csv_content = "id,name\n1,alice\n2,bob\n"
        rows = fn(csv_content, "data.csv")
        assert len(rows) == 2
        assert rows[0] == {"id": "1", "name": "alice"}
        assert rows[1] == {"id": "2", "name": "bob"}

    def test_csv_empty_body_returns_empty(self, fn: Any) -> None:
        rows = fn("id,name\n", "empty.csv")
        assert rows == []

    def test_ndjson_parses_each_line(self, fn: Any) -> None:
        content = '{"id": 1, "v": "a"}\n{"id": 2, "v": "b"}\n'
        rows = fn(content, "events.json")
        assert len(rows) == 2
        assert rows[0]["id"] == 1

    def test_ndjson_skips_malformed_lines(self, fn: Any) -> None:
        content = '{"id": 1}\nNOT JSON\n{"id": 3}\n'
        rows = fn(content, "data.ndjson")
        assert len(rows) == 2
        assert rows[1]["id"] == 3

    def test_ndjson_skips_blank_lines(self, fn: Any) -> None:
        content = '{"id": 1}\n\n{"id": 2}\n'
        rows = fn(content, "data.json")
        assert len(rows) == 2

    def test_unsupported_extension_returns_empty(self, fn: Any) -> None:
        rows = fn("some content", "archive.parquet")
        assert rows == []


# ---------------------------------------------------------------------------
# transform() — via task's python_callable
# ---------------------------------------------------------------------------


def _get_task_callable(task_id: str) -> Any:
    from airflow.models import DagBag

    bag = DagBag(dag_folder=DAGS_FOLDER, include_examples=False)
    return bag.get_dag("s3_ingestion_pipeline").get_task(task_id).python_callable


def _make_row(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": "1",
        "event_type": "CLICK",
        "user_id": "42",
        "created_at": "2024-03-15 10:00:00",
    }
    base.update(overrides)
    return base


class TestTransformLogic:
    @pytest.fixture(scope="class")
    def transform_fn(self) -> Any:
        return _get_task_callable("transform")

    def test_empty_input_returns_empty(self, transform_fn: Any) -> None:
        assert transform_fn([]) == []

    def test_drops_row_missing_required_field(self, transform_fn: Any) -> None:
        row = _make_row()
        del row["user_id"]
        result = transform_fn([row])
        assert result == []

    def test_drops_row_missing_multiple_fields(self, transform_fn: Any) -> None:
        result = transform_fn([{"id": "1"}])
        assert result == []

    def test_valid_row_passes_through(self, transform_fn: Any) -> None:
        result = transform_fn([_make_row()])
        assert len(result) == 1

    def test_normalises_event_type_to_lowercase(self, transform_fn: Any) -> None:
        result = transform_fn([_make_row(event_type="PAGE_VIEW")])
        assert result[0]["event_type"] == "page_view"

    def test_strips_whitespace_from_event_type(self, transform_fn: Any) -> None:
        result = transform_fn([_make_row(event_type="  scroll  ")])
        assert result[0]["event_type"] == "scroll"

    def test_adds_event_date_from_created_at(self, transform_fn: Any) -> None:
        result = transform_fn([_make_row(created_at="2024-06-20 08:30:00")])
        assert result[0]["event_date"] == "2024-06-20"

    def test_dedup_keeps_row_with_later_updated_at(self, transform_fn: Any) -> None:
        rows = [
            _make_row(id="1", updated_at="2024-01-01T08:00:00"),
            _make_row(id="1", updated_at="2024-01-01T12:00:00"),  # newer — keep this
        ]
        result = transform_fn(rows)
        assert len(result) == 1
        assert result[0]["updated_at"] == "2024-01-01T12:00:00"

    def test_dedup_keeps_row_with_updated_at_over_one_without(self, transform_fn: Any) -> None:
        rows = [
            _make_row(id="99"),                                     # no updated_at
            _make_row(id="99", updated_at="2024-01-01T00:00:01"),  # has updated_at — keep
        ]
        result = transform_fn(rows)
        assert len(result) == 1
        assert "updated_at" in result[0]

    def test_dedup_on_different_ids_keeps_all(self, transform_fn: Any) -> None:
        rows = [_make_row(id=str(i)) for i in range(5)]
        result = transform_fn(rows)
        assert len(result) == 5

    def test_all_invalid_rows_returns_empty(self, transform_fn: Any) -> None:
        rows = [{"id": str(i)} for i in range(3)]  # all missing required fields
        assert transform_fn(rows) == []


# ---------------------------------------------------------------------------
# load_to_clickhouse() — via task's python_callable, mocked ClickHouseHook
# ---------------------------------------------------------------------------


class TestLoadToClickhouse:
    @pytest.fixture(scope="class")
    def load_fn(self) -> Any:
        return _get_task_callable("load_to_clickhouse")

    def test_skips_when_rows_empty(
        self,
        load_fn: Any,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level("INFO"):
            load_fn([], logical_date=datetime(2024, 1, 1))
        assert "skipping" in caplog.text.lower()

    @patch("hooks.clickhouse_hook.ClickHouseHook.get_conn")
    @patch("hooks.clickhouse_hook.ClickHouseHook.get_connection")
    def test_drops_partition_before_inserting(
        self,
        mock_get_connection: MagicMock,
        mock_get_conn: MagicMock,
        load_fn: Any,
    ) -> None:
        mock_client = MagicMock()
        mock_get_conn.return_value = mock_client

        rows = [_make_row(event_date="2024-03-15")]
        load_fn(rows, logical_date=datetime(2024, 3, 15))

        execute_calls = mock_client.execute.call_args_list
        # First call must be the DROP PARTITION
        first_sql = execute_calls[0][0][0]
        assert "DROP PARTITION" in first_sql
        assert "analytics.events" in first_sql

    @patch("hooks.clickhouse_hook.ClickHouseHook.get_conn")
    @patch("hooks.clickhouse_hook.ClickHouseHook.get_connection")
    def test_bulk_insert_called_after_drop(
        self,
        mock_get_connection: MagicMock,
        mock_get_conn: MagicMock,
        load_fn: Any,
    ) -> None:
        mock_client = MagicMock()
        mock_get_conn.return_value = mock_client

        rows = [_make_row(event_date="2024-03-15"), _make_row(id="2", event_date="2024-03-15")]
        load_fn(rows, logical_date=datetime(2024, 3, 15))

        # Second client.execute call is the INSERT from bulk_insert
        execute_calls = mock_client.execute.call_args_list
        assert len(execute_calls) == 2
        insert_sql = execute_calls[1][0][0]
        assert "INSERT INTO" in insert_sql

    @patch("hooks.clickhouse_hook.ClickHouseHook.get_conn")
    @patch("hooks.clickhouse_hook.ClickHouseHook.get_connection")
    def test_partition_value_uses_logical_date(
        self,
        mock_get_connection: MagicMock,
        mock_get_conn: MagicMock,
        load_fn: Any,
    ) -> None:
        mock_client = MagicMock()
        mock_get_conn.return_value = mock_client

        load_fn([_make_row()], logical_date=datetime(2024, 7, 4))

        drop_call = mock_client.execute.call_args_list[0]
        params = drop_call[0][1]
        assert params["partition"] == "2024-07-04"
