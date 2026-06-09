"""
s3_ingestion_pipeline.py
========================
Daily S3 → ClickHouse ingestion pipeline.

Pipeline:
    S3KeySensor  →  extract_from_s3  →  transform  →  load_to_clickhouse

    1. S3KeySensor — waits for a ``_SUCCESS`` marker file in the date-partitioned
       S3 prefix.  The marker convention (written by upstream Spark / Glue jobs)
       guarantees that all data files for that date are fully landed before the
       ingestion starts.

    2. extract_from_s3 — lists every data file in the date prefix and reads them
       in-process.  Supports CSV and newline-delimited JSON (NDJSON).  Returns a
       flat list of raw row dicts via XCom.

    3. transform — validates required fields, normalises types, deduplicates on
       the primary key keeping the row with the latest ``updated_at`` value, and
       drops malformed rows with a warning.

    4. load_to_clickhouse — idempotent DROP PARTITION + bulk_insert via
       ClickHouseHook.  Dropping the partition before inserting means re-running
       the same logical date never produces duplicate rows in ClickHouse.

S3 prefix convention:
    s3://{S3_BUCKET}/{S3_PREFIX}/{YYYY}/{MM}/{DD}/
    e.g.  s3://my-data-lake/raw/events/2024/03/15/

Airflow Variables:
    s3_bucket    — S3 bucket name (e.g. "my-data-lake")
    s3_prefix    — Key prefix inside the bucket, no leading/trailing slash
                   (e.g. "raw/events")

Airflow Connections:
    aws_default           — AWS credentials for S3 access (key + secret or IAM role)
    clickhouse_analytics  — ClickHouse connection used by ClickHouseHook

Dependencies:
    pip install apache-airflow-providers-amazon clickhouse-driver
"""

from __future__ import annotations

import csv
import io
import json
import logging
from datetime import datetime
from typing import Any

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sdk import task
from airflow.sdk import Variable

from _base_dag import make_dag, build_idempotent_run_key
from hooks.clickhouse_hook import ClickHouseHook

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TARGET_TABLE: str = "analytics.events"
PARTITION_COLUMN: str = "event_date"   # Date column used for ClickHouse partitioning
PRIMARY_KEY: str = "id"                # Used for deduplication in transform()

REQUIRED_FIELDS: frozenset[str] = frozenset({"id", "event_type", "user_id", "created_at"})

# Files that do not contain data (markers, metadata, Spark logs)
_SKIP_SUFFIXES: tuple[str, ...] = ("_SUCCESS", "_metadata", ".crc", ".tmp")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _s3_date_prefix(base_prefix: str, logical_date: datetime) -> str:
    """
    Build the date-partitioned S3 key prefix for a given logical date.

    Example:
        _s3_date_prefix("raw/events", datetime(2024, 3, 15))
        → "raw/events/2024/03/15/"
    """
    return f"{base_prefix}/{logical_date:%Y/%m/%d}/"


def _parse_file(content: str, key: str) -> list[dict[str, Any]]:
    """
    Parse a single S3 file's content into a list of row dicts.

    Supported formats (detected from the file extension):
        .csv   — comma-separated, first row is the header
        .json  — newline-delimited JSON (NDJSON), one object per line

    Args:
        content: Raw file content as a UTF-8 string.
        key:     S3 object key, used only for format detection and log messages.

    Returns:
        List of row dicts.  Returns an empty list for unsupported extensions.
    """
    key_lower = key.lower()

    if key_lower.endswith(".csv"):
        reader = csv.DictReader(io.StringIO(content))
        return list(reader)

    if key_lower.endswith(".json") or key_lower.endswith(".ndjson"):
        rows: list[dict[str, Any]] = []
        for line_no, line in enumerate(content.splitlines(), start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                log.warning("Skipping malformed JSON | key=%s line=%d error=%s", key, line_no, exc)
        return rows

    log.warning("Unsupported file format — skipping | key=%s", key)
    return []


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------


with make_dag(
    dag_id="s3_ingestion_pipeline",
    description="Daily S3 → ClickHouse ingestion (CSV / NDJSON)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    tags=["ingestion", "s3", "clickhouse", "daily"],
    source="S3 (raw/events)",
    destination="ClickHouse (analytics.events)",
) as dag:

    # ------------------------------------------------------------------
    # 1. Wait for the upstream _SUCCESS marker
    # ------------------------------------------------------------------

    wait_for_s3 = S3KeySensor(
        task_id="wait_for_s3_marker",
        # Jinja template: resolved at runtime from Variables
        bucket_name="{{ var.value.s3_bucket }}",
        bucket_key=(
            "{{ var.value.s3_prefix }}"
            "/{{ logical_date.strftime('%Y/%m/%d') }}"
            "/_SUCCESS"
        ),
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=3600,   # wait up to 1 hour for upstream to land files
        mode="poke",
    )

    # ------------------------------------------------------------------
    # 2. List and read all data files from S3
    # ------------------------------------------------------------------

    @task
    def extract_from_s3(logical_date: datetime = None, **context: Any) -> list[dict[str, Any]]:
        """
        List every data file in the date-partitioned S3 prefix and parse them.

        Skips ``_SUCCESS``, ``.crc``, and other non-data files.  Reads each
        file in-process (suitable for files up to a few hundred MB total;
        for larger volumes use a Glue/Spark job and reference paths in XCom).

        Returns:
            Flat list of raw row dicts from all files in the prefix.
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # noqa: PLC0415

        bucket = Variable.get("s3_bucket")
        prefix = Variable.get("s3_prefix")
        date_prefix = _s3_date_prefix(prefix, logical_date)
        run_key = build_idempotent_run_key("s3_ingestion_pipeline", logical_date)

        log.info("Listing S3 files | bucket=%s prefix=%s run_key=%s", bucket, date_prefix, run_key)

        hook = S3Hook(aws_conn_id="aws_default")
        keys: list[str] = hook.list_keys(bucket_name=bucket, prefix=date_prefix) or []

        data_keys = [
            k for k in keys
            if not any(k.endswith(suffix) for suffix in _SKIP_SUFFIXES)
        ]
        log.info("Found %d data file(s) | prefix=%s", len(data_keys), date_prefix)

        all_rows: list[dict[str, Any]] = []
        for key in data_keys:
            content = hook.read_key(key=key, bucket_name=bucket)
            rows = _parse_file(content, key)
            log.info("Parsed %d rows | key=%s", len(rows), key)
            all_rows.extend(rows)

        log.info("extract_from_s3 complete | total_rows=%d | run_key=%s", len(all_rows), run_key)
        return all_rows

    # ------------------------------------------------------------------
    # 3. Validate, normalise, deduplicate
    # ------------------------------------------------------------------

    @task
    def transform(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Validate, normalise, and deduplicate raw rows before loading.

        Steps (in order):
            1. Drop rows missing any REQUIRED_FIELDS.
            2. Normalise ``event_type`` to lowercase + stripped.
            3. Add ``event_date`` (``YYYY-MM-DD``) derived from ``created_at``
               for ClickHouse date partitioning.
            4. Deduplicate on ``PRIMARY_KEY``, keeping the row with the latest
               ``updated_at`` (last-write-wins); rows without ``updated_at``
               are treated as older than any row that has it.

        Args:
            rows: Raw row dicts from extract_from_s3.

        Returns:
            Cleaned and deduplicated row dicts ready for bulk insert.
        """
        valid: list[dict[str, Any]] = []
        dropped = 0

        for row in rows:
            missing = REQUIRED_FIELDS - row.keys()
            if missing:
                log.debug("Dropping row — missing fields %s: %r", missing, row)
                dropped += 1
                continue

            transformed_row = {
                **row,
                "event_type": row["event_type"].lower().strip(),
                "event_date": str(row["created_at"])[:10],  # "YYYY-MM-DD"
            }
            valid.append(transformed_row)

        # Deduplicate: last-write-wins on PRIMARY_KEY
        seen: dict[Any, dict[str, Any]] = {}
        for row in valid:
            pk = row[PRIMARY_KEY]
            existing = seen.get(pk)
            if existing is None:
                seen[pk] = row
            else:
                # Keep the row with the larger updated_at (or the challenger if no field)
                current_ts = existing.get("updated_at", "")
                challenger_ts = row.get("updated_at", "")
                if challenger_ts > current_ts:
                    seen[pk] = row

        result = list(seen.values())
        log.info(
            "Transform complete | in=%d valid=%d dropped=%d deduped=%d out=%d",
            len(rows),
            len(valid),
            dropped,
            len(valid) - len(result),
            len(result),
        )
        return result

    # ------------------------------------------------------------------
    # 4. Idempotent load into ClickHouse
    # ------------------------------------------------------------------

    @task
    def load_to_clickhouse(
        rows: list[dict[str, Any]],
        logical_date: datetime = None,
        **context: Any,
    ) -> None:
        """
        Load rows into ClickHouse with an idempotent DROP PARTITION + bulk_insert.

        Dropping the ClickHouse partition for ``logical_date`` before inserting
        ensures that re-running the same DAG run never produces duplicate rows,
        regardless of how many times the task is retried.

        The target table must be partitioned by ``PARTITION_COLUMN`` (a Date
        column) for DROP PARTITION to work correctly.

        Args:
            rows:         Transformed rows from transform().
            logical_date: Airflow logical date — determines which partition to drop.
        """
        if not rows:
            log.info("No rows to load — skipping.")
            return

        run_key = build_idempotent_run_key("s3_ingestion_pipeline", logical_date)
        partition_value = logical_date.strftime("%Y-%m-%d")

        log.info(
            "Starting load | rows=%d | partition=%s | run_key=%s",
            len(rows),
            partition_value,
            run_key,
        )

        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_analytics")

        # Idempotent: remove the partition for this date before inserting.
        # ClickHouse DROP PARTITION is an atomic metadata operation — much
        # faster than a DELETE mutation for date-partitioned MergeTree tables.
        hook.execute(
            f"ALTER TABLE {TARGET_TABLE} DROP PARTITION %(partition)s",
            {"partition": partition_value},
        )
        log.info("Dropped partition %s from %s", partition_value, TARGET_TABLE)

        inserted = hook.bulk_insert(TARGET_TABLE, rows)
        log.info(
            "Load complete | inserted=%d | partition=%s | run_key=%s",
            inserted,
            partition_value,
            run_key,
        )

    # ------------------------------------------------------------------
    # Task wiring
    # ------------------------------------------------------------------

    raw_rows = extract_from_s3()
    wait_for_s3 >> raw_rows
    clean_rows = transform(raw_rows)
    load_to_clickhouse(clean_rows)
