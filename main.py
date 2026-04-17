#!/usr/bin/env python3
"""
Health Vault — Main Entry Point

A passive, self-healing ingestion pipeline that watches iCloud Drive
for health data exports from iOS and loads them into PostgreSQL.

Usage:
    python3 main.py
"""

import signal
import sys
import threading

from health_vault.config import setup_logging, INBOX_DIR, ARCHIVE_DIR
from health_vault.db import get_connection, ensure_schema, health_check
from health_vault.watcher import start_watcher, sweep_inbox, periodic_sweep


def main():
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("Health Vault starting up")
    logger.info("=" * 60)
    logger.info("Inbox:   %s", INBOX_DIR)
    logger.info("Archive: %s", ARCHIVE_DIR)

    # ── Database connection ───────────────────────────────────────────
    try:
        conn = get_connection()
        ensure_schema(conn)
    except Exception as e:
        logger.critical("Cannot connect to PostgreSQL: %s", e)
        logger.critical("Is the health_vault database created? Run: python3 setup_db.py")
        sys.exit(1)

    if not health_check(conn):
        logger.critical("Database health check failed")
        sys.exit(1)

    logger.info("✅ Database connected and schema verified")

    # ── Catch-up sweep ────────────────────────────────────────────────
    logger.info("Running initial inbox sweep...")
    count = sweep_inbox(conn)
    logger.info("Initial sweep complete: %d file(s) processed", count)

    # ── Graceful shutdown machinery ───────────────────────────────────
    stop_event = threading.Event()

    def shutdown_handler(signum, frame):
        sig_name = signal.Signals(signum).name
        logger.info("Received %s — shutting down gracefully...", sig_name)
        stop_event.set()

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    # ── Start file watcher ────────────────────────────────────────────
    observer = start_watcher(conn, stop_event)

    # ── Start periodic sweep thread ───────────────────────────────────
    sweep_thread = threading.Thread(
        target=periodic_sweep,
        args=(conn, stop_event),
        daemon=True,
        name="periodic-sweep",
    )
    sweep_thread.start()

    logger.info("🟢 Health Vault is running. Press Ctrl+C to stop.")

    # ── Block until shutdown signal ───────────────────────────────────
    try:
        while not stop_event.is_set():
            stop_event.wait(1)
    except KeyboardInterrupt:
        stop_event.set()

    # ── Cleanup ───────────────────────────────────────────────────────
    logger.info("Stopping file watcher...")
    observer.stop()
    observer.join(timeout=10)

    logger.info("Closing database connection...")
    conn.close()

    logger.info("🔴 Health Vault stopped.")


if __name__ == "__main__":
    main()
