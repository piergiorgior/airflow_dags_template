# airflow-dag-templates

Production-grade Apache Airflow 3.x DAG templates demonstrating Senior Data Platform Engineering patterns: idempotent pipelines, custom hooks, CDC streaming, S3 ingestion, and a full pytest suite.

The entire stack runs locally via Docker Compose — no external accounts required.

---

## Architecture

```mermaid
flowchart LR
    subgraph Sources["Sources (local Docker)"]
        PG_src[(postgres-demo\nsource_db.events)]
        KF[Kafka\ncdc.events]
        S3[LocalStack S3\nairflow-demo/raw/events/]
    end

    subgraph Airflow["Airflow 3.2.1 · CeleryExecutor"]
        direction TB
        ETL["etl_postgres_to_clickhouse\n─────────────────────\nextract → transform → load\n@hourly · delete-then-insert"]
        CDC["cdc_kafka_consumer\n─────────────────────\nKafkaBatchSensor\n→ consume_messages\n→ upsert_to_postgres\nevery 5 min · ON CONFLICT"]
        S3P["s3_ingestion_pipeline\n─────────────────────\nS3KeySensor\n→ extract_from_s3\n→ transform\n→ load_to_clickhouse\n@daily · DROP PARTITION"]
    end

    subgraph Targets["Targets (local Docker)"]
        CH[(ClickHouse\nanalytics.events)]
        PG_dst[(postgres-demo\ntarget_db.events)]
    end

    PG_src -->|hourly batch| ETL --> CH
    KF -->|near-real-time CDC| CDC --> PG_dst
    S3 -->|daily batch| S3P --> CH
```

---

## Features

| Pattern | Where |
|---|---|
| `make_dag()` factory — retry x3, exponential backoff, Slack alerting | `dags/_base_dag.py` |
| Idempotent delete-then-insert (batch ETL) | `dags/etl_postgres_to_clickhouse.py` |
| Custom `KafkaBatchSensor` (no offset commit on poke) | `dags/cdc_kafka_consumer.py` |
| At-least-once CDC with idempotent `ON CONFLICT DO UPDATE` | `dags/cdc_kafka_consumer.py` |
| S3 `_SUCCESS` marker pattern + CSV/NDJSON parsing | `dags/s3_ingestion_pipeline.py` |
| Idempotent ClickHouse load via `DROP PARTITION` + bulk insert | `dags/s3_ingestion_pipeline.py` |
| Last-write-wins deduplication on `updated_at` | `dags/s3_ingestion_pipeline.py` |
| Custom ClickHouse Hook — `get_conn`, `execute`, `get_records`, `bulk_insert` | `plugins/hooks/clickhouse_hook.py` |
| Full pytest suite — 100+ unit tests, zero real external connections | `tests/` |

---

## Prerequisites

| Tool | Version |
|---|---|
| Docker & Docker Compose v2+ | any recent |
| Python | 3.12+ (local dev / tests only) |
| `make` | optional |

---

## Quickstart

```bash
# 1. Set your UID (Linux/macOS/Git Bash) — skip on Windows (defaults to 50000)
echo "AIRFLOW_UID=$(id -u)" > .env

# 2. Initialise the metadata DB and create the admin user
docker compose up airflow-init
# Wait for: "apache-airflow==3.2.1" in the output, then Ctrl-C

# 3. Start the full stack (Airflow + ClickHouse + Kafka + LocalStack + demo data)
docker compose up -d

# 4. Open the Airflow UI
open http://localhost:8080   # credentials: admin / ... (to retrieve from airflow api server logs)
```

All connections and variables are **pre-wired** to the local demo containers — no manual configuration in the UI needed.

### Optional: Celery Flower monitoring

```bash
docker compose --profile flower up -d
open http://localhost:5555
```

### Stop / reset

```bash
docker compose down                       # stop containers, keep volumes
docker compose down -v --remove-orphans   # full reset
```

---

## Demo stack — what runs locally

| Service | Container | Endpoint | Notes |
|---|---|---|---|
| Airflow UI | `airflow_api_server` | http://localhost:8080 | admin / ... (to retrieve from airflow api server logs) |
| ClickHouse | `airflow_clickhouse` | http://localhost:8123/play | TCP: 9000 |
| Kafka | `airflow_kafka` | localhost:9092 | KRaft, no Zookeeper |
| LocalStack (S3) | `airflow_localstack` | http://localhost:4566 | |
| Postgres (Airflow) | `airflow_postgres` | localhost:5432 | metadata DB |
| Postgres (demo) | `airflow_postgres_demo` | localhost:5433 | source + target data |

### What `demo-init` sets up automatically

The `demo-init` container runs once after all services are healthy:

- **Kafka** — creates topic `cdc.events` and produces 5 seed CDC messages so the CDC DAG triggers on first run
- **S3** — creates bucket `airflow-demo`, uploads `raw/events/YYYY/MM/DD/events.csv` + `_SUCCESS` marker for yesterday's date
- **Postgres `source_db`** — `events` table with 500 seeded rows spanning the last 7 days
- **Postgres `target_db`** — empty `events` table ready for CDC upserts
- **ClickHouse** — `analytics.events` MergeTree table (created at container startup via init SQL)

### Triggering the DAGs

After `docker compose up -d`, unpause each DAG in the UI and:

| DAG | How to trigger |
|---|---|
| `etl_postgres_to_clickhouse` | Runs `@hourly` — or trigger manually for any past hour |
| `cdc_kafka_consumer` | Runs every 5 min — seed messages already in Kafka from `demo-init` |
| `s3_ingestion_pipeline` | Trigger manually for yesterday's date (CSV + `_SUCCESS` already in S3) |

---

## Running tests

> **Platform note** — Airflow 3.x depends on `fcntl` and other POSIX-only system
> calls deep in its import chain (`DagBundlesManager`, `BaseDagBundle`, etc.).
> These modules do not exist on Windows, so the full test suite **must run on
> Linux or macOS** — either inside Docker or in a native Linux/macOS virtualenv.
> Running `pytest` directly on Windows will fail with
> `ModuleNotFoundError: No module named 'fcntl'` for any test that imports
> `DagBag` or other Airflow internals.

### Inside Docker (the only supported way on Windows)

```bash
docker compose exec airflow-worker bash -c "
  pip install pytest pytest-mock --quiet &&
  pytest tests/ -v
"
```

### Linux / macOS virtualenv

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest tests/ -v
```

### Run a single test module

```bash
pytest tests/dags/test_base_dag.py -v
pytest tests/dags/test_s3_ingestion_pipeline.py -v
pytest tests/plugins/hooks/test_clickhouse_hook.py -v
```

---

## How to add a new DAG

1. **Create** `dags/my_new_dag.py` using the factory:

```python
from __future__ import annotations
from datetime import datetime
from typing import Any
from airflow.sdk import task
from _base_dag import make_dag, build_idempotent_run_key

with make_dag(
    dag_id="my_new_dag",
    description="Short description shown in the Airflow UI",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    tags=["my-team", "source-system"],
    source="source_db.my_table",
    destination="target_db.my_table",
) as dag:

    @task
    def extract(logical_date: datetime = None, **context: Any) -> list[dict]:
        run_key = build_idempotent_run_key("my_new_dag", logical_date)
        ...

    @task
    def load(rows: list[dict], logical_date: datetime = None, **context: Any) -> None:
        ...

    load(extract())
```

2. **Add tests** in `tests/dags/test_my_new_dag.py` mirroring the same structure as the existing test files.

3. **Register connections** in the Airflow UI (Admin → Connections) or via `AIRFLOW_CONN_*` environment variables in `.env`.

---

## Airflow Variables

All variables are **pre-configured** for the local demo stack via environment variables.
To override for production, set them in the UI (Admin → Variables) or in `.env`.

| Variable | Demo value | Description |
|---|---|---|
| `alert_slack_enabled` | `false` | Enable Slack failure alerts |
| `alert_slack_webhook` | — | Slack Incoming Webhook URL (only if Slack enabled) |
| `kafka_bootstrap_servers` | `kafka:29092` | Comma-separated Kafka broker list |
| `s3_bucket` | `airflow-demo` | S3 bucket name |
| `s3_prefix` | `raw/events` | Key prefix inside the bucket |

---

## Airflow Connections

All connections are **pre-configured** for the local demo stack via `AIRFLOW_CONN_*` environment variables.
To override for production, set them in the UI (Admin → Connections) or in `.env`.

| Conn ID | Type | Demo target | Used by |
|---|---|---|---|
| `postgres_source` | Postgres | `postgres-demo:5432/source_db` | `etl_postgres_to_clickhouse` |
| `clickhouse_analytics` | Generic | `clickhouse:9000/analytics` | `etl_postgres_to_clickhouse`, `s3_ingestion_pipeline` |
| `postgres_target` | Postgres | `postgres-demo:5432/target_db` | `cdc_kafka_consumer` |
| `aws_default` | AWS | `localstack:4566` | `s3_ingestion_pipeline` |

---

## Project structure

```
airflow-dag-templates/
│
├── dags/
│   ├── _base_dag.py                        # make_dag() factory, retry policy, Slack callback,
│   │                                       # build_idempotent_run_key(), build_dag_doc()
│   ├── etl_postgres_to_clickhouse.py       # Hourly batch ETL: extract → transform → load
│   ├── cdc_kafka_consumer.py               # Near-real-time CDC: KafkaBatchSensor → consume → upsert
│   └── s3_ingestion_pipeline.py            # Daily S3 ingestion: S3Sensor → extract → transform → load
│
├── plugins/
│   └── hooks/
│       └── clickhouse_hook.py              # Custom ClickHouse Hook (clickhouse-driver)
│
├── tests/
│   ├── conftest.py                         # Shared fixtures, Airflow unit-test env setup
│   ├── dags/
│   │   ├── test_base_dag.py                # 35+ unit tests for _base_dag.py
│   │   ├── test_etl_postgres_to_clickhouse.py
│   │   └── test_s3_ingestion_pipeline.py   # DAG structure + parse/transform/load tests
│   └── plugins/
│       └── hooks/
│           └── test_clickhouse_hook.py     # 20+ unit tests for ClickHouseHook
│
├── demo/
│   ├── postgres_init.sh                    # Creates source_db + target_db tables, seeds 500 rows
│   ├── clickhouse_init.sql                 # Creates analytics.events MergeTree table
│   └── setup.py                            # Creates Kafka topic, S3 bucket, uploads demo data
│
├── docker-compose.yml                      # Full local stack: Airflow + ClickHouse + Kafka + LocalStack
├── requirements.txt                        # All Python dependencies
├── .env.example                            # Environment variable template
├── pytest.ini                              # pytest configuration
└── .gitignore
```

---

## Design decisions

**Idempotency strategies** — each DAG uses the right tool for its access pattern:
- Batch ETL → delete-then-insert (window-based, fixed time range)
- CDC → `ON CONFLICT DO UPDATE` (row-level, keyed on primary key)
- S3 ingestion → `DROP PARTITION` + bulk insert (partition-level, cheapest operation in ClickHouse MergeTree)

**At-least-once CDC** — the `KafkaBatchSensor` never commits offsets. `consume_messages` reads without committing. Only `upsert_to_postgres` commits after a successful write, so a crash anywhere retries the full consume+write cycle.

**S3 `_SUCCESS` marker** — the sensor waits for a marker file written by the upstream Spark/Glue job rather than polling for data files directly. This prevents partial reads when the upstream job is still writing.

**Last-write-wins deduplication** — the S3 pipeline deduplicates on primary key before loading, keeping the row with the largest `updated_at`. This handles re-delivered or out-of-order records from upstream systems.

**Alerting as configuration** — Slack alerts are toggled via `alert_slack_enabled` Airflow Variable, not by changing DAG code. The callback reads the variable at runtime, so enabling/disabling alerts requires no deployment.

**ClickHouse hook lazy import** — `clickhouse-driver` is imported inside `get_conn()`, not at module level. Workers that do not have the driver installed can still parse DAG files without errors; only tasks that call the hook need the dependency.

---

## Tech stack

- **Orchestration**: Apache Airflow 3.2.1
- **Executor**: CeleryExecutor (Redis broker)
- **Metadata DB**: PostgreSQL 17
- **OLAP**: ClickHouse 24 (via `clickhouse-driver`)
- **Streaming**: Apache Kafka 7.6 KRaft (via `kafka-python`)
- **Object storage**: LocalStack S3 (via `apache-airflow-providers-amazon`)
- **Testing**: pytest, unittest.mock
- **Containerisation**: Docker Compose
