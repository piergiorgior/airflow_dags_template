#!/bin/bash
# postgres_init.sh — runs on first start of postgres-demo container.
# Creates source_db tables with seed data and target_db for CDC.
set -e

# ── source_db (already created by POSTGRES_DB env var) ──────────────────────
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "source_db" <<-'EOSQL'
    CREATE TABLE IF NOT EXISTS events (
        id         BIGSERIAL    PRIMARY KEY,
        event_type VARCHAR(100) NOT NULL,
        user_id    BIGINT       NOT NULL,
        created_at TIMESTAMP    NOT NULL DEFAULT NOW(),
        payload    TEXT         NOT NULL DEFAULT '{}'
    );

    -- Seed: 500 rows spread across the last 7 days
    INSERT INTO events (event_type, user_id, created_at, payload)
    SELECT
        (ARRAY['click', 'view', 'purchase', 'scroll', 'signup'])
            [ (ROW_NUMBER() OVER () % 5) + 1 ],
        (RANDOM() * 999 + 1)::BIGINT,
        NOW() - ((RANDOM() * 7)::INT || ' days')::INTERVAL
                - ((RANDOM() * 23)::INT || ' hours')::INTERVAL,
        '{"source": "web"}'
    FROM GENERATE_SERIES(1, 500);
EOSQL

# ── target_db (CDC upsert destination) ──────────────────────────────────────
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "source_db" <<-'EOSQL'
    CREATE DATABASE target_db;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "target_db" <<-'EOSQL'
    CREATE TABLE IF NOT EXISTS events (
        id         BIGINT       PRIMARY KEY,
        event_type VARCHAR(100),
        user_id    BIGINT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP    DEFAULT NOW(),
        payload    TEXT         DEFAULT '{}'
    );
EOSQL

echo "postgres-demo init complete: source_db (500 rows) + target_db created."
