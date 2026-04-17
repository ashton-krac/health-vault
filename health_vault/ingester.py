"""
Ingestion orchestrator for Health Vault.

Coordinates the full pipeline for a single file:
    parse → dedup check → batch insert → register file → archive
"""

import logging
import shutil
from pathlib import Path

import psycopg

from .config import ARCHIVE_DIR, BATCH_SIZE
from .db import get_connection, insert_metrics, register_file, is_file_ingested
from .dedup import compute_sha256
from .parser import parse_export_file

logger = logging.getLogger("health_vault.ingester")


def ingest_file(filepath: Path, conn: psycopg.Connection | None = None) -> bool:
    """
    Process a single JSON export file through the full pipeline.

    1. Check if file was already ingested (by filename)
    2. Parse the JSON into structured rows
    3. Batch-insert into health_metrics (with row-level dedup via ON CONFLICT)
    4. Register the file in ingested_files
    5. Move the file to the archive directory

    Returns True if the file was successfully processed, False otherwise.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        filename = filepath.name

        # ── Step 1: File-level dedup ──────────────────────────────────
        if is_file_ingested(conn, filename):
            logger.info("⏭  Already ingested: %s", filename)
            _archive_file(filepath)
            return True

        # ── Step 2: Parse ─────────────────────────────────────────────
        rows = parse_export_file(filepath)
        if not rows:
            logger.warning("⚠  No data points found in: %s", filename)
            # Still register it so we don't re-try an empty/malformed file
            sha = compute_sha256(filepath)
            register_file(conn, filename, sha, 0, filepath.stat().st_size)
            _archive_file(filepath)
            return True

        # ── Step 3: Batch insert ──────────────────────────────────────
        total_inserted = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            total_inserted += insert_metrics(conn, batch)

        # ── Step 4: Register file ────────────────────────────────────
        sha = compute_sha256(filepath)
        register_file(conn, filename, sha, total_inserted, filepath.stat().st_size)

        # ── Step 5: Archive ──────────────────────────────────────────
        _archive_file(filepath)

        logger.info("✅ Ingested %s → %d records inserted", filename, total_inserted)
        return True

    except Exception as e:
        logger.error("❌ Failed to ingest %s: %s", filepath.name, e, exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass
        return False

    finally:
        if own_conn and conn:
            conn.close()


def _archive_file(filepath: Path) -> None:
    """Move a processed file to the archive directory."""
    try:
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        dest = ARCHIVE_DIR / filepath.name

        # If a file with the same name already exists in archive, add a suffix
        if dest.exists():
            stem = filepath.stem
            suffix = filepath.suffix
            counter = 1
            while dest.exists():
                dest = ARCHIVE_DIR / f"{stem}_{counter}{suffix}"
                counter += 1

        shutil.move(str(filepath), str(dest))
        logger.debug("Archived: %s → %s", filepath.name, dest.name)
    except Exception as e:
        logger.warning("Could not archive %s: %s", filepath.name, e)
