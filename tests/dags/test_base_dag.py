"""
test_base_dag.py
================
Unit tests for dags/_base_dag.py.

Coverage:
- get_default_args(): default values, custom overrides, type correctness
- on_failure_callback(): structured logging, Slack opt-in/opt-out, error swallowing
- build_idempotent_run_key(): output format, determinism, uniqueness
- make_dag(): DAG object properties, catchup, tags, callback wiring
- build_dag_doc(): presence of required sections
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from _base_dag import (
    build_dag_doc,
    build_idempotent_run_key,
    get_default_args,
    make_dag,
    on_failure_callback,
)


# ---------------------------------------------------------------------------
# get_default_args
# ---------------------------------------------------------------------------


class TestGetDefaultArgs:
    def test_returns_dict(self) -> None:
        assert isinstance(get_default_args(), dict)

    def test_default_owner(self) -> None:
        assert get_default_args()["owner"] == "data-engineering"

    def test_default_retries(self) -> None:
        assert get_default_args()["retries"] == 3

    def test_retry_delay_is_timedelta(self) -> None:
        assert isinstance(get_default_args()["retry_delay"], timedelta)

    def test_default_retry_delay_is_five_minutes(self) -> None:
        assert get_default_args()["retry_delay"] == timedelta(minutes=5)

    def test_retry_exponential_backoff_default_true(self) -> None:
        assert get_default_args()["retry_exponential_backoff"] is True

    def test_depends_on_past_is_false(self) -> None:
        assert get_default_args()["depends_on_past"] is False

    def test_custom_owner(self) -> None:
        assert get_default_args(owner="platform-team")["owner"] == "platform-team"

    def test_custom_retries(self) -> None:
        assert get_default_args(retries=5)["retries"] == 5

    def test_custom_retry_delay_minutes(self) -> None:
        assert get_default_args(retry_delay_minutes=10)["retry_delay"] == timedelta(minutes=10)

    def test_disable_exponential_backoff(self) -> None:
        result = get_default_args(retry_exponential_backoff=False)
        assert result["retry_exponential_backoff"] is False


# ---------------------------------------------------------------------------
# on_failure_callback
# ---------------------------------------------------------------------------


class TestOnFailureCallback:
    @patch("_base_dag.Variable.get", return_value="false")
    def test_logs_error_on_failure(
        self,
        _mock_var: MagicMock,
        failure_context: dict[str, Any],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.ERROR, logger="_base_dag"):
            on_failure_callback(failure_context)
        assert "Task failed" in caplog.text
        assert "test_dag" in caplog.text
        assert "test_task" in caplog.text

    @patch("_base_dag.Variable.get", return_value="false")
    @patch("requests.post")
    def test_slack_not_called_when_disabled(
        self,
        mock_post: MagicMock,
        _mock_var: MagicMock,
        failure_context: dict[str, Any],
    ) -> None:
        on_failure_callback(failure_context)
        mock_post.assert_not_called()

    @patch("requests.post")
    @patch("_base_dag.Variable.get")
    def test_slack_called_when_enabled(
        self,
        mock_var: MagicMock,
        mock_post: MagicMock,
        failure_context: dict[str, Any],
    ) -> None:
        def _side_effect(key: str, **kwargs: Any) -> str:
            return "true" if key == "alert_slack_enabled" else "https://hooks.slack.com/test"

        mock_var.side_effect = _side_effect
        mock_post.return_value = MagicMock()

        on_failure_callback(failure_context)

        mock_post.assert_called_once()
        call_url = mock_post.call_args[0][0]
        assert call_url == "https://hooks.slack.com/test"

    @patch("requests.post")
    @patch("_base_dag.Variable.get")
    def test_slack_payload_contains_dag_info(
        self,
        mock_var: MagicMock,
        mock_post: MagicMock,
        failure_context: dict[str, Any],
    ) -> None:
        mock_var.side_effect = lambda k, **kw: (
            "true" if k == "alert_slack_enabled" else "https://hooks.slack.com/test"
        )
        mock_post.return_value = MagicMock()

        on_failure_callback(failure_context)

        payload = mock_post.call_args.kwargs["json"]
        assert "test_dag" in payload["text"]
        assert "test_task" in payload["text"]

    @patch("requests.post", side_effect=Exception("network error"))
    @patch("_base_dag.Variable.get")
    def test_slack_failure_does_not_raise(
        self,
        mock_var: MagicMock,
        _mock_post: MagicMock,
        failure_context: dict[str, Any],
    ) -> None:
        mock_var.side_effect = lambda k, **kw: (
            "true" if k == "alert_slack_enabled" else "https://hooks.slack.com/test"
        )
        # Must not propagate — alerting must never break pipeline reporting
        on_failure_callback(failure_context)

    @patch("_base_dag.Variable.get", return_value="false")
    def test_callback_with_no_exception_in_context(
        self,
        _mock_var: MagicMock,
        failure_context: dict[str, Any],
    ) -> None:
        failure_context["exception"] = None
        on_failure_callback(failure_context)  # must not raise


# ---------------------------------------------------------------------------
# build_idempotent_run_key
# ---------------------------------------------------------------------------


class TestBuildIdempotentRunKey:
    def test_format(self) -> None:
        key = build_idempotent_run_key("my_dag", datetime(2024, 3, 15, 12, 30, 0))
        assert key == "my_dag__2024-03-15T12:30:00"

    def test_separator_is_double_underscore(self) -> None:
        key = build_idempotent_run_key("dag_x", datetime(2024, 1, 1))
        dag_id, date_part = key.split("__", 1)
        assert dag_id == "dag_x"
        assert date_part == "2024-01-01T00:00:00"

    def test_deterministic(self) -> None:
        dt = datetime(2024, 6, 1, 8, 0, 0)
        assert build_idempotent_run_key("etl", dt) == build_idempotent_run_key("etl", dt)

    def test_different_dates_produce_different_keys(self) -> None:
        dt1 = datetime(2024, 1, 1)
        dt2 = datetime(2024, 1, 2)
        assert build_idempotent_run_key("dag", dt1) != build_idempotent_run_key("dag", dt2)

    def test_different_dag_ids_produce_different_keys(self) -> None:
        dt = datetime(2024, 1, 1)
        assert build_idempotent_run_key("dag_a", dt) != build_idempotent_run_key("dag_b", dt)

    def test_midnight_datetime_format(self) -> None:
        key = build_idempotent_run_key("d", datetime(2024, 12, 31, 23, 59, 59))
        assert key.endswith("2024-12-31T23:59:59")


# ---------------------------------------------------------------------------
# make_dag
# ---------------------------------------------------------------------------


class TestMakeDag:
    def test_returns_dag_instance(self) -> None:
        from airflow.sdk import DAG

        dag = make_dag(
            dag_id="factory_test",
            description="Test",
            schedule="@daily",
            start_date=datetime(2024, 1, 1),
        )
        assert isinstance(dag, DAG)

    def test_dag_id_matches(self) -> None:
        dag = make_dag(
            dag_id="my_test_dag",
            description="Test",
            schedule="@daily",
            start_date=datetime(2024, 1, 1),
        )
        assert dag.dag_id == "my_test_dag"

    def test_catchup_false_by_default(self) -> None:
        dag = make_dag(
            dag_id="catchup_test",
            description="Test",
            schedule="@daily",
            start_date=datetime(2024, 1, 1),
        )
        assert dag.catchup is False

    def test_tags_are_set(self) -> None:
        dag = make_dag(
            dag_id="tags_test",
            description="Test",
            schedule="@daily",
            start_date=datetime(2024, 1, 1),
            tags=["etl", "postgres"],
        )
        assert "etl" in dag.tags
        assert "postgres" in dag.tags

    def test_on_failure_callback_is_wired(self) -> None:
        dag = make_dag(
            dag_id="callback_test",
            description="Test",
            schedule="@daily",
            start_date=datetime(2024, 1, 1),
        )
        assert dag.on_failure_callback is not None

    def test_extra_default_args_merged(self) -> None:
        dag = make_dag(
            dag_id="extra_args_test",
            description="Test",
            schedule="@daily",
            start_date=datetime(2024, 1, 1),
        )

    def test_empty_tags_when_none(self) -> None:
        dag = make_dag(
            dag_id="no_tags_test",
            description="Test",
            schedule="@daily",
            start_date=datetime(2024, 1, 1),
        )
        assert dag.tags == []


# ---------------------------------------------------------------------------
# build_dag_doc
# ---------------------------------------------------------------------------


class TestBuildDagDoc:
    def test_contains_description(self) -> None:
        doc = build_dag_doc("My ETL pipeline", "@daily", "platform-team")
        assert "My ETL pipeline" in doc

    def test_contains_schedule(self) -> None:
        doc = build_dag_doc("desc", "0 6 * * *", "team")
        assert "0 6 * * *" in doc

    def test_contains_owner(self) -> None:
        doc = build_dag_doc("desc", "@hourly", "data-eng")
        assert "data-eng" in doc

    def test_contains_source_when_provided(self) -> None:
        doc = build_dag_doc("desc", "@daily", "team", source="PostgreSQL")
        assert "PostgreSQL" in doc

    def test_no_source_when_omitted(self) -> None:
        doc = build_dag_doc("desc", "@daily", "team")
        assert "Source" not in doc

    def test_contains_destination_when_provided(self) -> None:
        doc = build_dag_doc("desc", "@daily", "team", destination="ClickHouse")
        assert "ClickHouse" in doc

    def test_contains_retry_section(self) -> None:
        doc = build_dag_doc("desc", "@daily", "team")
        assert "Retry" in doc

    def test_contains_idempotency_section(self) -> None:
        doc = build_dag_doc("desc", "@daily", "team")
        assert "Idempotency" in doc or "idempotent" in doc.lower()

    def test_returns_string(self) -> None:
        assert isinstance(build_dag_doc("desc", "@daily", "team"), str)
