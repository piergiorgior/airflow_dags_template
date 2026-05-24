"""
test_clickhouse_hook.py
=======================
Unit tests for plugins/hooks/clickhouse_hook.py.

All ClickHouse network calls are mocked — no real ClickHouse instance is needed.

Coverage:
- get_conn(): lazy instantiation, connection parameter mapping, client reuse
- execute(): SQL forwarding, default empty params
- get_records(): return value pass-through
- bulk_insert(): dict rows, tuple rows, empty rows, custom column_names, count return
- close(): disconnect call, guard against double-close
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from hooks.clickhouse_hook import ClickHouseHook


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def hook() -> ClickHouseHook:
    return ClickHouseHook(clickhouse_conn_id="clickhouse_test")


@pytest.fixture
def mock_conn() -> MagicMock:
    conn = MagicMock()
    conn.host = "ch-host"
    conn.port = 9000
    conn.login = "default"
    conn.password = "secret"
    conn.schema = "analytics"
    return conn


@pytest.fixture
def mock_client() -> MagicMock:
    return MagicMock()


# ---------------------------------------------------------------------------
# get_conn
# ---------------------------------------------------------------------------


class TestGetConn:
    @patch("hooks.clickhouse_hook.ClickHouseHook.get_connection")
    @patch("clickhouse_driver.Client")
    def test_creates_client_with_connection_params(
        self,
        mock_client_cls: MagicMock,
        mock_get_connection: MagicMock,
        hook: ClickHouseHook,
        mock_conn: MagicMock,
        mock_client: MagicMock,
    ) -> None:
        mock_get_connection.return_value = mock_conn
        mock_client_cls.return_value = mock_client

        result = hook.get_conn()

        mock_client_cls.assert_called_once_with(
            host="ch-host",
            port=9000,
            user="default",
            password="secret",
            database="analytics",
            settings={"use_numpy": False},
        )
        assert result is mock_client

    @patch("hooks.clickhouse_hook.ClickHouseHook.get_connection")
    @patch("clickhouse_driver.Client")
    def test_reuses_existing_client(
        self,
        mock_client_cls: MagicMock,
        mock_get_connection: MagicMock,
        hook: ClickHouseHook,
        mock_conn: MagicMock,
        mock_client: MagicMock,
    ) -> None:
        mock_get_connection.return_value = mock_conn
        mock_client_cls.return_value = mock_client

        first = hook.get_conn()
        second = hook.get_conn()

        assert first is second
        mock_client_cls.assert_called_once()  # not twice

    @patch("hooks.clickhouse_hook.ClickHouseHook.get_connection")
    @patch("clickhouse_driver.Client")
    def test_defaults_when_conn_fields_are_none(
        self,
        mock_client_cls: MagicMock,
        mock_get_connection: MagicMock,
        hook: ClickHouseHook,
    ) -> None:
        empty_conn = MagicMock()
        empty_conn.host = None
        empty_conn.port = None
        empty_conn.login = None
        empty_conn.password = None
        empty_conn.schema = None
        mock_get_connection.return_value = empty_conn
        mock_client_cls.return_value = MagicMock()

        hook.get_conn()

        mock_client_cls.assert_called_once_with(
            host="localhost",
            port=9000,
            user="default",
            password="",
            database="default",
            settings={"use_numpy": False},
        )


# ---------------------------------------------------------------------------
# execute
# ---------------------------------------------------------------------------


class TestExecute:
    def test_forwards_sql_and_params(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        hook.execute("ALTER TABLE t DELETE WHERE id = %(id)s", {"id": 42})
        mock_client.execute.assert_called_once_with(
            "ALTER TABLE t DELETE WHERE id = %(id)s", {"id": 42}
        )

    def test_uses_empty_dict_when_no_params(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        hook.execute("TRUNCATE TABLE t")
        mock_client.execute.assert_called_once_with("TRUNCATE TABLE t", {})

    def test_returns_none(self, hook: ClickHouseHook, mock_client: MagicMock) -> None:
        hook._client = mock_client
        mock_client.execute.return_value = None
        assert hook.execute("SELECT 1") is None


# ---------------------------------------------------------------------------
# get_records
# ---------------------------------------------------------------------------


class TestGetRecords:
    def test_returns_rows_from_client(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        mock_client.execute.return_value = [(1, "alice"), (2, "bob")]

        rows = hook.get_records("SELECT id, name FROM t WHERE d = %(d)s", {"d": "2024-01-01"})

        assert rows == [(1, "alice"), (2, "bob")]
        mock_client.execute.assert_called_once_with(
            "SELECT id, name FROM t WHERE d = %(d)s", {"d": "2024-01-01"}
        )

    def test_uses_empty_dict_when_no_params(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        mock_client.execute.return_value = []
        hook.get_records("SELECT 1")
        mock_client.execute.assert_called_once_with("SELECT 1", {})

    def test_returns_empty_list_when_no_rows(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        mock_client.execute.return_value = []
        assert hook.get_records("SELECT 1 WHERE 1=0") == []


# ---------------------------------------------------------------------------
# bulk_insert
# ---------------------------------------------------------------------------


class TestBulkInsert:
    def test_empty_rows_returns_zero_without_calling_client(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        count = hook.bulk_insert("analytics.events", [])
        assert count == 0
        mock_client.execute.assert_not_called()

    def test_dict_rows_infers_columns_from_first_row(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        rows = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
        count = hook.bulk_insert("analytics.users", rows)

        assert count == 2
        sql_arg, data_arg = mock_client.execute.call_args[0]
        assert "INSERT INTO analytics.users" in sql_arg
        assert "id" in sql_arg and "name" in sql_arg
        assert data_arg == [(1, "alice"), (2, "bob")]

    def test_dict_rows_respects_custom_column_names(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        rows = [{"id": 1, "name": "alice", "extra": "ignored"}]
        hook.bulk_insert("t", rows, column_names=["id", "name"])

        _, data_arg = mock_client.execute.call_args[0]
        assert data_arg == [(1, "alice")]

    def test_tuple_rows_with_column_names(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        rows = [(1, "alice"), (2, "bob")]
        count = hook.bulk_insert("analytics.users", rows, column_names=["id", "name"])

        assert count == 2
        sql_arg, data_arg = mock_client.execute.call_args[0]
        assert "id, name" in sql_arg
        assert data_arg == [(1, "alice"), (2, "bob")]

    def test_tuple_rows_without_column_names_raises(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        with pytest.raises(ValueError, match="column_names is required"):
            hook.bulk_insert("t", [(1, "alice")])

    def test_returns_row_count(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        rows = [{"id": i} for i in range(10)]
        assert hook.bulk_insert("t", rows) == 10

    def test_sql_contains_values_keyword(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        hook.bulk_insert("db.tbl", [{"x": 1}])
        sql_arg = mock_client.execute.call_args[0][0]
        assert "VALUES" in sql_arg


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------


class TestClose:
    def test_disconnects_client(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        hook.close()
        mock_client.disconnect.assert_called_once()
        assert hook._client is None

    def test_close_when_no_client_is_noop(self, hook: ClickHouseHook) -> None:
        hook._client = None
        hook.close()  # must not raise

    def test_double_close_is_safe(
        self, hook: ClickHouseHook, mock_client: MagicMock
    ) -> None:
        hook._client = mock_client
        hook.close()
        hook.close()  # second call — _client is now None, must not raise
        mock_client.disconnect.assert_called_once()
