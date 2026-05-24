"""
etl_postgres_to_clickhouse.py
=============================
Example DAG: hourly ETL from PostgreSQL to ClickHouse.

Demonstrates:
- make_dag() factory from _base_dag
- Idempotent task pattern (delete-then-insert)
- Type-annotated @task functions (Airflow 3.x SDK)
- XCom usage with explicit types
- Retry logic inherited from default_args
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from _base_dag import make_dag, build_idempotent_run_key

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with make_dag(
    dag_id="etl_postgres_to_clickhouse",
    description="Hourly ETL — PostgreSQL → ClickHouse",
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    tags=["etl", "postgres", "clickhouse", "hourly"],
    source="PostgreSQL (source_db.events)",
    destination="ClickHouse (analytics.events)",
) as dag:

    @task
    def extract(logical_date: datetime = None, **context: Any) -> list[dict[str, Any]]:
        """
        Extract rows from PostgreSQL for the current logical hour.

        Idempotent: always queries a fixed time window based on logical_date,
        so re-running the same DAG run returns the same rows.

        Returns:
            List of row dicts to be passed downstream via XCom.
        """
        run_key = build_idempotent_run_key("etl_postgres_to_clickhouse", logical_date)
        log.info("Starting extract | run_key=%s", run_key)

        hook = PostgresHook(postgres_conn_id="postgres_source")
        sql = """
            SELECT
                id,
                event_type,
                user_id,
                created_at::text AS created_at,
                payload
            FROM events
            WHERE created_at >= %(start)s
              AND created_at <  %(end)s
        """
        rows = hook.get_records(
            sql,
            parameters={
                "start": logical_date,
                "end": logical_date.replace(minute=0, second=0) + __import__("datetime").timedelta(hours=1),
            },
        )

        result = [
            {
                "id": r[0],
                "event_type": r[1],
                "user_id": r[2],
                "created_at": r[3],
                "payload": r[4],
            }
            for r in rows
        ]

        log.info("Extracted %d rows | run_key=%s", len(result), run_key)
        return result

    @task
    def transform(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Apply lightweight transformations before loading.

        - Normalise event_type to lowercase
        - Drop rows with null user_id
        - Add a derived 'event_date' column for ClickHouse partitioning

        Args:
            rows: Raw rows from extract().

        Returns:
            Cleaned and enriched rows.
        """
        transformed: list[dict[str, Any]] = []

        for row in rows:
            if row.get("user_id") is None:
                log.debug("Dropping row with null user_id: id=%s", row.get("id"))
                continue

            transformed.append(
                {
                    **row,
                    "event_type": row["event_type"].lower().strip(),
                    "event_date": row["created_at"][:10],  # "YYYY-MM-DD"
                }
            )

        log.info("Transform complete: %d → %d rows", len(rows), len(transformed))
        return transformed

    @task
    def load(rows: list[dict[str, Any]], logical_date: datetime = None, **context: Any) -> None:
        """
        Load rows into ClickHouse with an idempotent delete-then-insert pattern.

        Before inserting, deletes all rows for the current logical hour
        so re-runs never produce duplicates.

        Args:
            rows:         Transformed rows from transform().
            logical_date: Injected by Airflow — used for the delete window.
        """
        if not rows:
            log.info("No rows to load, skipping.")
            return

        run_key = build_idempotent_run_key("etl_postgres_to_clickhouse", logical_date)
        log.info("Starting load | rows=%d | run_key=%s", len(rows), run_key)

        # NOTE: replace ClickHouseHook with your actual hook/client.
        # Example shown with a hypothetical hook for illustration.
        #
        # from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook
        # hook = ClickHouseHook(clickhouse_conn_id="clickhouse_analytics")
        #
        # Idempotent delete — remove existing rows for this time window first
        # hook.execute(
        #     "ALTER TABLE analytics.events DELETE WHERE created_at >= %(start)s AND created_at < %(end)s",
        #     {"start": logical_date, "end": logical_date + timedelta(hours=1)},
        # )
        # hook.execute("INSERT INTO analytics.events VALUES", rows)

        log.info("Load complete | rows=%d | run_key=%s", len(rows), run_key)

    # ---------------------------------------------------------------------------
    # Wire up tasks — Airflow 3.x TaskFlow API
    # ---------------------------------------------------------------------------

    raw_rows = extract()
    clean_rows = transform(raw_rows)
    load(clean_rows)