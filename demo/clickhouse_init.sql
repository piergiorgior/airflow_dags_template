-- clickhouse_init.sql — runs on first start of the ClickHouse container.
-- Creates the analytics database and the events table used by both the
-- ETL DAG (etl_postgres_to_clickhouse) and the S3 ingestion DAG.

CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events
(
    id          UInt64,
    event_type  LowCardinality(String),
    user_id     UInt64,
    created_at  DateTime('UTC'),
    payload     String DEFAULT '{}',
    event_date  Date
)
ENGINE = MergeTree()
PARTITION BY event_date
ORDER BY (event_date, id)
SETTINGS index_granularity = 8192;
