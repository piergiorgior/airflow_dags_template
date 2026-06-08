"""
demo/setup.py — one-shot demo environment setup.

Runs inside the demo-init container after all demo services are healthy.
Creates:
  • Kafka topic: cdc.events
  • S3 bucket: airflow-demo (via LocalStack)
  • S3 sample file: raw/events/YYYY/MM/DD/events.csv  (yesterday's date)
  • S3 _SUCCESS marker (signals upstream job finished)
  • Seeds Kafka topic with 5 CDC events (so the CDC DAG triggers immediately)
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from datetime import date, timedelta

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
LOCALSTACK_ENDPOINT = os.getenv("LOCALSTACK_ENDPOINT", "http://localstack:4566")
S3_BUCKET = "airflow-demo"
S3_PREFIX = "raw/events"
KAFKA_TOPIC = "cdc.events"


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

def setup_kafka() -> None:
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        admin.create_topics([
            NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
        ])
        log.info("Kafka topic '%s' created", KAFKA_TOPIC)
    except TopicAlreadyExistsError:
        log.info("Kafka topic '%s' already exists — skipping", KAFKA_TOPIC)
    finally:
        admin.close()


def seed_kafka() -> None:
    """Produce a small batch of CDC events so the CDC DAG triggers on first run."""
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    records = [
        {
            "id": i,
            "event_type": etype,
            "user_id": uid,
            "created_at": "2024-03-15T10:00:00",
            "updated_at": "2024-03-15T10:00:00",
            "payload": "{}",
        }
        for i, (etype, uid) in enumerate(
            [("click", 10), ("view", 11), ("purchase", 12), ("scroll", 13), ("signup", 14)],
            start=1,
        )
    ]
    for record in records:
        producer.send(KAFKA_TOPIC, value=record)
    producer.flush()
    producer.close()
    log.info("Seeded %d messages to Kafka topic '%s'", len(records), KAFKA_TOPIC)


# ---------------------------------------------------------------------------
# S3 / LocalStack
# ---------------------------------------------------------------------------

def setup_s3() -> None:
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client(
        "s3",
        endpoint_url=LOCALSTACK_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )

    # Create bucket
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
        log.info("S3 bucket '%s' created", S3_BUCKET)
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            log.info("S3 bucket '%s' already exists — skipping", S3_BUCKET)
        else:
            raise

    # Upload sample CSV for yesterday so the @daily DAG can be triggered manually
    yesterday = date.today() - timedelta(days=1)
    prefix = f"{S3_PREFIX}/{yesterday:%Y/%m/%d}"

    rows = [
        {
            "id": str(i),
            "event_type": etype,
            "user_id": str(uid),
            "created_at": f"{yesterday}T{hour:02d}:00:00",
            "updated_at": f"{yesterday}T{hour:02d}:00:00",
            "payload": "{}",
        }
        for i, (etype, uid, hour) in enumerate(
            [
                ("click",    101, 8),
                ("view",     102, 9),
                ("purchase", 103, 10),
                ("click",    101, 11),
                ("signup",   104, 12),
                ("view",     105, 13),
                ("scroll",   102, 14),
                ("click",    106, 15),
                ("view",     107, 16),
                ("purchase", 108, 17),
            ],
            start=1,
        )
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)

    csv_key = f"{prefix}/events.csv"
    success_key = f"{prefix}/_SUCCESS"

    s3.put_object(Bucket=S3_BUCKET, Key=csv_key, Body=buf.getvalue().encode("utf-8"))
    s3.put_object(Bucket=S3_BUCKET, Key=success_key, Body=b"")

    log.info("Uploaded %d rows → s3://%s/%s", len(rows), S3_BUCKET, csv_key)
    log.info("Uploaded _SUCCESS marker → s3://%s/%s", S3_BUCKET, success_key)
    log.info(
        "To trigger the S3 DAG: unpause 's3_ingestion_pipeline' in the UI "
        "and run it for date %s",
        yesterday,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    log.info("=== demo-init starting ===")

    log.info("--- Kafka ---")
    setup_kafka()
    seed_kafka()

    log.info("--- S3 (LocalStack) ---")
    setup_s3()

    log.info("=== demo-init complete ===")
    log.info(
        "Services ready:\n"
        "  Airflow UI   → http://localhost:8080   (admin / admin)\n"
        "  ClickHouse   → http://localhost:8123/play\n"
        "  Kafka        → localhost:9092\n"
        "  LocalStack   → http://localhost:4566\n"
        "  Postgres src → localhost:5433  db=source_db  user=demo  pw=demo\n"
    )
