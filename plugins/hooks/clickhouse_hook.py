"""
clickhouse_hook.py
==================
Custom Airflow Hook for ClickHouse using the clickhouse-driver native TCP protocol.

Install the driver:
    pip install clickhouse-driver

Airflow connection fields used (Connection type: "Generic" or custom "ClickHouse"):
    host     → ClickHouse server hostname   (default: localhost)
    port     → Native TCP port              (default: 9000)
    login    → Username                     (default: default)
    password → Password                     (default: empty string)
    schema   → Database name               (default: default)

Usage in a DAG task::

    from hooks.clickhouse_hook import ClickHouseHook

    hook = ClickHouseHook(clickhouse_conn_id="clickhouse_analytics")
    rows = hook.get_records(
        "SELECT id, name FROM events WHERE event_date = %(d)s",
        {"d": "2024-01-01"},
    )
    hook.bulk_insert("analytics.events", rows, column_names=["id", "name"])
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Sequence

from airflow.sdk.bases.hook import BaseHook

if TYPE_CHECKING:
    from clickhouse_driver import Client

log = logging.getLogger(__name__)


class ClickHouseHook(BaseHook):
    """
    Airflow Hook for ClickHouse via the clickhouse-driver native protocol.

    The hook is lazy: a connection is created on first use and re-used across
    multiple calls within the same task invocation. Call ``close()`` explicitly
    if you need to release the connection before the task ends.

    Args:
        clickhouse_conn_id: Airflow connection ID for ClickHouse credentials.
    """

    conn_name_attr = "clickhouse_conn_id"
    default_conn_name = "clickhouse_default"
    conn_type = "clickhouse"
    hook_name = "ClickHouse"

    def __init__(self, clickhouse_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.clickhouse_conn_id = clickhouse_conn_id
        self._client: Client | None = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def get_conn(self) -> Client:
        """
        Return a clickhouse-driver Client, creating one on first call.

        Credentials are read from the Airflow connection store so they never
        appear in DAG source code.

        Returns:
            An authenticated ``clickhouse_driver.Client`` instance.
        """
        if self._client is not None:
            return self._client

        from clickhouse_driver import Client  # noqa: PLC0415 — deferred to avoid hard dep at import time

        conn = self.get_connection(self.clickhouse_conn_id)
        self._client = Client(
            host=conn.host or "localhost",
            port=int(conn.port or 9000),
            user=conn.login or "default",
            password=conn.password or "",
            database=conn.schema or "default",
            settings={"use_numpy": False},
        )
        log.debug(
            "ClickHouse connection opened | host=%s port=%s database=%s",
            conn.host,
            conn.port,
            conn.schema,
        )
        return self._client

    def close(self) -> None:
        """
        Disconnect from ClickHouse and release the client.

        Safe to call even if the connection was never opened.
        """
        if self._client is not None:
            self._client.disconnect()
            self._client = None
            log.debug("ClickHouse connection closed.")

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        """
        Execute a SQL statement that does not return rows.

        Use for DDL (CREATE, DROP), DML mutations (ALTER TABLE … DELETE),
        or fire-and-forget INSERT statements.

        Args:
            sql:    SQL string with optional ``%(key)s`` placeholders.
            params: Bind parameters matching the placeholders in ``sql``.
        """
        client = self.get_conn()
        log.debug("Executing | sql=%.200s", sql)
        client.execute(sql, params or {})

    def get_records(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> list[tuple[Any, ...]]:
        """
        Execute a SELECT and return all rows as a list of tuples.

        Args:
            sql:    SELECT statement with optional ``%(key)s`` placeholders.
            params: Bind parameters.

        Returns:
            List of row tuples in column order.

        Example::

            rows = hook.get_records(
                "SELECT id, ts FROM events WHERE date = %(d)s",
                {"d": "2024-01-01"},
            )
        """
        client = self.get_conn()
        log.debug("Fetching records | sql=%.200s", sql)
        return client.execute(sql, params or {})

    def bulk_insert(
        self,
        table: str,
        rows: list[dict[str, Any]] | list[tuple[Any, ...]],
        column_names: Sequence[str] | None = None,
    ) -> int:
        """
        Insert a batch of rows using clickhouse-driver's native columnar protocol.

        This is significantly faster than individual INSERTs because the driver
        batches the data into a single network round-trip using the ClickHouse
        native binary format.

        When ``rows`` is a list of dicts, ``column_names`` is inferred from the
        first dict's keys. Pass ``column_names`` explicitly to control column
        order or exclude keys.

        Args:
            table:        Fully qualified table name, e.g. ``"analytics.events"``.
            rows:         Rows to insert — list of dicts or list of tuples.
            column_names: Required when ``rows`` are tuples; optional for dicts.

        Returns:
            Number of rows inserted (0 when ``rows`` is empty).

        Raises:
            ValueError: When ``rows`` are tuples and ``column_names`` is not provided.

        Example::

            hook.bulk_insert(
                "analytics.events",
                [{"id": 1, "event_type": "click"}, {"id": 2, "event_type": "view"}],
            )
        """
        if not rows:
            log.info("bulk_insert called with empty rows — skipping.")
            return 0

        if isinstance(rows[0], dict):
            cols: Sequence[str] = column_names or list(rows[0].keys())
            data: list[tuple[Any, ...]] = [tuple(r[c] for c in cols) for r in rows]  # type: ignore[index]
        else:
            if not column_names:
                raise ValueError(
                    "column_names is required when rows are tuples, "
                    "so the hook knows which columns to map them to."
                )
            cols = column_names
            data = list(rows)  # type: ignore[arg-type]

        col_clause = ", ".join(cols)
        sql = f"INSERT INTO {table} ({col_clause}) VALUES"

        client = self.get_conn()
        client.execute(sql, data)
        log.info("bulk_insert complete | table=%s rows=%d", table, len(data))
        return len(data)
