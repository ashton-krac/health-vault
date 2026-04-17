"""
Database layer for Health Vault.

Manages PostgreSQL connections and schema lifecycle using psycopg v3.
"""

import logging

import psycopg
from psycopg.rows import dict_row

from .config import DATABASE_URL

logger = logging.getLogger("health_vault.db")

# ──────────────────────────────────────────────────────────────────────
# Schema DDL
# ──────────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
-- Primary ingestion table: each row is one health data point
CREATE TABLE IF NOT EXISTS health_metrics (
    id              BIGSERIAL PRIMARY KEY,
    metric_type     TEXT NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_device   TEXT,
    value           DOUBLE PRECISION,
    unit            TEXT,
    raw_payload     JSONB NOT NULL
);

-- Fast lookups by metric type + time
CREATE INDEX IF NOT EXISTS idx_metrics_type_time
    ON health_metrics (metric_type, recorded_at DESC);

-- Time-series range scans
CREATE INDEX IF NOT EXISTS idx_metrics_recorded_at
    ON health_metrics (recorded_at DESC);

-- Full JSONB search capability
CREATE INDEX IF NOT EXISTS idx_metrics_raw_gin
    ON health_metrics USING GIN (raw_payload);

-- Dedup: prevent identical data points from being inserted twice
-- Uses a partial index (only rows with a non-null numeric value)
CREATE UNIQUE INDEX IF NOT EXISTS idx_metrics_dedup
    ON health_metrics (metric_type, recorded_at, source_device, value)
    WHERE value IS NOT NULL;

-- File tracking: never process the same export file twice
CREATE TABLE IF NOT EXISTS ingested_files (
    id              SERIAL PRIMARY KEY,
    filename        TEXT NOT NULL UNIQUE,
    sha256_hash     TEXT NOT NULL,
    record_count    INTEGER NOT NULL DEFAULT 0,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    file_size_bytes BIGINT
);
"""


# ──────────────────────────────────────────────────────────────────────
# Connection helpers
# ──────────────────────────────────────────────────────────────────────


def get_connection() -> psycopg.Connection:
    """Return a new psycopg v3 connection to the health_vault database."""
    conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=False)
    logger.debug("Connected to PostgreSQL via %s", DATABASE_URL)
    return conn


def ensure_schema(conn: psycopg.Connection) -> None:
    """Create tables and indexes if they don't already exist."""
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    logger.info("Schema verified / created.")


def health_check(conn: psycopg.Connection) -> bool:
    """Quick connectivity check."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            return cur.fetchone() is not None
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────
# Batch insert
# ──────────────────────────────────────────────────────────────────────


def insert_metrics(conn: psycopg.Connection, rows: list[dict]) -> int:
    """
    Batch-insert parsed health metric rows.

    Each row dict must contain:
        metric_type, recorded_at, source_device, value, unit, raw_payload

    Returns the number of rows actually inserted (after dedup).
    Uses ON CONFLICT DO NOTHING to silently skip duplicates.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO health_metrics
            (metric_type, recorded_at, source_device, value, unit, raw_payload)
        VALUES
            (%(metric_type)s, %(recorded_at)s, %(source_device)s,
             %(value)s, %(unit)s, %(raw_payload)s)
        ON CONFLICT DO NOTHING
    """

    inserted = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(sql, row)
            # rowcount is 1 if inserted, 0 if skipped by ON CONFLICT
            inserted += cur.rowcount

    conn.commit()
    logger.info("Inserted %d / %d rows (duplicates skipped: %d)",
                inserted, len(rows), len(rows) - inserted)
    return inserted


def register_file(conn: psycopg.Connection, filename: str,
                  sha256_hash: str, record_count: int,
                  file_size_bytes: int) -> None:
    """Record a file as successfully ingested."""
    sql = """
        INSERT INTO ingested_files (filename, sha256_hash, record_count, file_size_bytes)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (filename) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(sql, (filename, sha256_hash, record_count, file_size_bytes))
    conn.commit()
    logger.debug("Registered file: %s (%d records)", filename, record_count)


def is_file_ingested(conn: psycopg.Connection, filename: str) -> bool:
    """Check if a file has already been processed."""
    sql = "SELECT 1 FROM ingested_files WHERE filename = %s"
    with conn.cursor() as cur:
        cur.execute(sql, (filename,))
        return cur.fetchone() is not None
