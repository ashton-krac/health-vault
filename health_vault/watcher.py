"""
File system watcher for Health Vault.

Monitors the iCloud inbox directory for new JSON health exports.
Uses watchdog for real-time filesystem events plus a periodic sweep
to catch files that arrived while the watcher was down.
"""

import logging
import threading
import time
from pathlib import Path

import psycopg
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent

from .config import INBOX_DIR, ICLOUD_SETTLE_DELAY, SWEEP_INTERVAL
from .dedup import is_icloud_placeholder, wait_for_download
from .ingester import ingest_file

logger = logging.getLogger("health_vault.watcher")


class HealthFileHandler(FileSystemEventHandler):
    """Handles new JSON files appearing in the iCloud inbox."""

    def __init__(self, conn: psycopg.Connection):
        super().__init__()
        self.conn = conn
        self._processing_lock = threading.Lock()
        self._recently_processed: set[str] = set()

    def on_created(self, event):
        if event.is_directory:
            return
        self._handle_event(event)

    def on_modified(self, event):
        if event.is_directory:
            return
        self._handle_event(event)

    def _handle_event(self, event):
        """Process a file event with iCloud-aware settling."""
        filepath = Path(event.src_path)

        # Only process JSON files
        if filepath.suffix.lower() != ".json":
            # Check if it's an iCloud placeholder for a JSON file
            if is_icloud_placeholder(filepath):
                real_name = filepath.name[1:].rsplit(".icloud", 1)[0]
                if not real_name.endswith(".json"):
                    return
                # Wait for download then process the real file
                if wait_for_download(filepath):
                    filepath = filepath.parent / real_name
                else:
                    return
            else:
                return

        # Avoid duplicate processing of the same file event
        file_key = filepath.name
        if file_key in self._recently_processed:
            return

        # Let iCloud finish writing the file
        logger.debug("Detected: %s — waiting %ds for iCloud sync...",
                     filepath.name, ICLOUD_SETTLE_DELAY)
        time.sleep(ICLOUD_SETTLE_DELAY)

        # Verify file still exists and has content after settling
        if not filepath.exists() or filepath.stat().st_size == 0:
            logger.debug("File vanished or empty after settle: %s", filepath.name)
            return

        with self._processing_lock:
            self._recently_processed.add(file_key)
            try:
                ingest_file(filepath, self.conn)
            finally:
                # Allow re-processing after some time (in case of retries)
                threading.Timer(30, lambda: self._recently_processed.discard(file_key)).start()


def sweep_inbox(conn: psycopg.Connection) -> int:
    """
    One-time scan of the inbox directory for any unprocessed JSON files.

    This catches files that arrived while the watcher was not running.
    Returns the count of files processed.
    """
    INBOX_DIR.mkdir(parents=True, exist_ok=True)
    json_files = sorted(INBOX_DIR.glob("*.json"))

    if not json_files:
        return 0

    logger.info("📂 Sweep found %d JSON file(s) in inbox", len(json_files))
    processed = 0
    for filepath in json_files:
        if filepath.stat().st_size > 0:
            if ingest_file(filepath, conn):
                processed += 1

    return processed


def periodic_sweep(conn: psycopg.Connection, stop_event: threading.Event) -> None:
    """
    Background thread that periodically sweeps the inbox.

    Runs every SWEEP_INTERVAL seconds until stop_event is set.
    """
    while not stop_event.is_set():
        stop_event.wait(SWEEP_INTERVAL)
        if stop_event.is_set():
            break
        try:
            count = sweep_inbox(conn)
            if count:
                logger.info("🔄 Periodic sweep ingested %d file(s)", count)
        except Exception as e:
            logger.error("Periodic sweep failed: %s", e, exc_info=True)


def start_watcher(conn: psycopg.Connection, stop_event: threading.Event) -> Observer:
    """
    Start the watchdog observer on the iCloud inbox directory.

    Returns the Observer instance (caller is responsible for joining).
    """
    INBOX_DIR.mkdir(parents=True, exist_ok=True)

    handler = HealthFileHandler(conn)
    observer = Observer()
    observer.schedule(handler, str(INBOX_DIR), recursive=False)
    observer.start()

    logger.info("👁  Watching: %s", INBOX_DIR)
    return observer
