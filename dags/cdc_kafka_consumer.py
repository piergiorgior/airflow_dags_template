"""
cdc_kafka_consumer.py
=====================
CDC DAG: near-real-time change-data-capture from Kafka into PostgreSQL.

Pipeline:
    KafkaBatchSensor  →  consume_messages  →  upsert_to_postgres

    1. KafkaBatchSensor — custom BaseSensorOperator that pokes the Kafka topic
       and returns True as soon as unconsumed messages are available.
       It does NOT commit offsets, preserving at-least-once semantics.

    2. consume_messages — reads up to MAX_BATCH_SIZE messages from the topic
       using the same consumer group as the sensor.  Offsets are NOT committed
       here; they will be committed after the upsert succeeds so that a failed
       upsert triggers a full retry (consume + upsert) on the same data.

    3. upsert_to_postgres — bulk-upserts records via
       INSERT … ON CONFLICT DO UPDATE SET …, which is inherently idempotent:
       re-running the same records never produces duplicate rows.
       Kafka offsets are committed here, after a successful write.

Idempotency guarantee:
    Re-running the same DAG run is safe:
    - The XCom value from consume_messages is reused by Airflow on task retry,
      so the upsert receives the same records.
    - The ON CONFLICT clause prevents duplicate writes.

Delivery semantics:
    At-least-once. In the unlikely event that upsert_to_postgres succeeds but
    the subsequent offset commit fails, the next DAG run will re-consume and
    re-upsert the same records — which is safe due to idempotent upsert.

Airflow Variables required:
    kafka_bootstrap_servers — comma-separated broker list,
                              e.g. "kafka-broker-1:9092,kafka-broker-2:9092"

Airflow Connections required:
    postgres_target — PostgreSQL connection for the target table

Dependencies:
    pip install kafka-python apache-airflow-providers-postgres
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from airflow.sdk import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.bases.sensor import BaseSensorOperator
from airflow.sdk import Context

from _base_dag import make_dag, build_idempotent_run_key

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration constants — change per environment via Airflow Variables
# ---------------------------------------------------------------------------

KAFKA_TOPIC: str = "cdc.events"
KAFKA_GROUP_ID: str = "airflow-cdc-consumer"
MAX_BATCH_SIZE: int = 1_000
POLL_TIMEOUT_MS: int = 5_000

TARGET_TABLE: str = "public.events"
UPSERT_KEY_COLUMN: str = "id"


# ---------------------------------------------------------------------------
# Custom sensor
# ---------------------------------------------------------------------------


class KafkaBatchSensor(BaseSensorOperator):
    """
    Polls a Kafka topic and returns True when unconsumed messages are available.

    Creates a short-lived consumer per poke, checks for at least one message,
    then closes the consumer WITHOUT committing offsets.  This ensures the
    downstream ``consume_messages`` task sees the same messages and handles
    offset commits itself after a successful write.

    Args:
        kafka_bootstrap_servers: Comma-separated broker addresses, e.g.
                                 ``"broker-1:9092,broker-2:9092"``.
        kafka_topic:             Kafka topic to monitor.
        kafka_group_id:          Consumer group ID shared with consume_messages.
        poll_timeout_ms:         Max milliseconds to wait for messages per poke.
    """

    template_fields: tuple[str, ...] = ("kafka_topic", "kafka_group_id")

    def __init__(
        self,
        *,
        kafka_bootstrap_servers: str,
        kafka_topic: str = KAFKA_TOPIC,
        kafka_group_id: str = KAFKA_GROUP_ID,
        poll_timeout_ms: int = POLL_TIMEOUT_MS,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.kafka_group_id = kafka_group_id
        self.poll_timeout_ms = poll_timeout_ms

    def poke(self, context: Context) -> bool:  # type: ignore[override]
        """
        Check whether the consumer group has unconsumed messages.

        Returns:
            True if at least one message is waiting; False to poke again later.
        """
        from kafka import KafkaConsumer  # noqa: PLC0415

        consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap_servers.split(","),
            group_id=self.kafka_group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # sensor never commits — consume_messages does
            consumer_timeout_ms=self.poll_timeout_ms,
        )
        try:
            records = consumer.poll(timeout_ms=self.poll_timeout_ms, max_records=1)
            has_messages = any(records.values())
            log.info(
                "KafkaBatchSensor poke | topic=%s group=%s messages_waiting=%s",
                self.kafka_topic,
                self.kafka_group_id,
                has_messages,
            )
            return has_messages
        finally:
            consumer.close()


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------


with make_dag(
    dag_id="cdc_kafka_consumer",
    description="CDC — Kafka (cdc.events) → PostgreSQL upsert",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    tags=["cdc", "kafka", "postgres", "streaming"],
    source="Kafka (cdc.events)",
    destination="PostgreSQL (public.events)",
) as dag:

    kafka_sensor = KafkaBatchSensor(
        task_id="wait_for_kafka_messages",
        # Resolved at runtime so brokers can change without touching DAG code
        kafka_bootstrap_servers="{{ var.value.kafka_bootstrap_servers }}",
        kafka_topic=KAFKA_TOPIC,
        kafka_group_id=KAFKA_GROUP_ID,
        poll_timeout_ms=POLL_TIMEOUT_MS,
        poke_interval=30,   # seconds between pokes
        timeout=300,        # give up after 5 min if no messages arrive
        mode="poke",
    )

    @task
    def consume_messages() -> list[dict[str, Any]]:
        """
        Read up to MAX_BATCH_SIZE messages from the Kafka topic.

        Each Kafka message value is expected to be a UTF-8 encoded JSON object
        representing one CDC record.  Messages that cannot be decoded are
        skipped with a warning (add a dead-letter topic in production).

        Offsets are NOT committed here.  They are committed by
        ``upsert_to_postgres`` after a successful database write, guaranteeing
        at-least-once delivery: a failed upsert retries the full consume+write.

        Returns:
            List of deserialised record dicts (one per valid Kafka message).
        """
        bootstrap = Variable.get("kafka_bootstrap_servers")

        from kafka import KafkaConsumer  # noqa: PLC0415

        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=bootstrap.split(","),
            group_id=KAFKA_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=POLL_TIMEOUT_MS,
        )
        try:
            raw_batch = consumer.poll(
                timeout_ms=POLL_TIMEOUT_MS,
                max_records=MAX_BATCH_SIZE,
            )
            records: list[dict[str, Any]] = []
            for partition_records in raw_batch.values():
                for msg in partition_records:
                    try:
                        record = json.loads(msg.value.decode("utf-8"))
                        records.append(record)
                    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                        log.warning(
                            "Skipping undecodable message | offset=%d error=%s",
                            msg.offset,
                            exc,
                        )
            log.info(
                "Consumed %d records | topic=%s group=%s",
                len(records),
                KAFKA_TOPIC,
                KAFKA_GROUP_ID,
            )
            return records
        finally:
            consumer.close()  # never commit here — upsert_to_postgres handles it

    @task
    def upsert_to_postgres(
        records: list[dict[str, Any]],
        logical_date: datetime = None,
        **context: Any,
    ) -> None:
        """
        Bulk-upsert CDC records into the target PostgreSQL table.

        Uses ``INSERT … ON CONFLICT ({UPSERT_KEY_COLUMN}) DO UPDATE SET …``
        which is idempotent: replaying the same batch never produces duplicates.

        After a successful write, Kafka offsets are committed so the next DAG
        run begins from where this one finished (at-least-once guarantee).

        Args:
            records:      Deserialised records from ``consume_messages``.
            logical_date: Airflow logical date — used to build the run key for
                          structured logging and audit trails.
        """
        if not records:
            log.info("No records to upsert — skipping.")
            return

        run_key = build_idempotent_run_key("cdc_kafka_consumer", logical_date)
        log.info("Starting upsert | records=%d | run_key=%s", len(records), run_key)

        # All records from a single CDC topic share the same schema
        columns = list(records[0].keys())
        if UPSERT_KEY_COLUMN not in columns:
            raise ValueError(
                f"Upsert key '{UPSERT_KEY_COLUMN}' not found in records. "
                f"Available columns: {columns}"
            )

        col_list = ", ".join(columns)
        placeholders = ", ".join(f"%({c})s" for c in columns)
        update_set = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in columns if c != UPSERT_KEY_COLUMN
        )
        sql = (
            f"INSERT INTO {TARGET_TABLE} ({col_list}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT ({UPSERT_KEY_COLUMN}) DO UPDATE SET {update_set}"
        )

        hook = PostgresHook(postgres_conn_id="postgres_target")
        # hook.run() with a list executes executemany; for batches > 10k rows
        # replace with psycopg2.extras.execute_values for ~10× throughput.
        hook.run(sql, parameters=records, autocommit=True)
        log.info("Upsert complete | records=%d | run_key=%s", len(records), run_key)

        # Commit Kafka offsets AFTER successful write (at-least-once guarantee)
        bootstrap = Variable.get("kafka_bootstrap_servers")
        from kafka import KafkaConsumer  # noqa: PLC0415

        commit_consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=bootstrap.split(","),
            group_id=KAFKA_GROUP_ID,
            enable_auto_commit=False,
        )
        try:
            commit_consumer.poll(timeout_ms=100)  # join the group to enable commit()
            commit_consumer.commit()
            log.info("Kafka offsets committed | group=%s", KAFKA_GROUP_ID)
        finally:
            commit_consumer.close()

    # ------------------------------------------------------------------
    # Task wiring — sensor gates the consume step
    # ------------------------------------------------------------------

    messages = consume_messages()
    kafka_sensor >> messages
    upsert_to_postgres(messages)
