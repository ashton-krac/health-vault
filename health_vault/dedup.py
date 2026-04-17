"""
File-level deduplication for Health Vault.

Uses SHA-256 hashing to ensure the same export file is never processed
twice, even if it appears with a different filename.
"""

import hashlib
import logging
from pathlib import Path

logger = logging.getLogger("health_vault.dedup")

HASH_CHUNK_SIZE = 8192  # 8 KB read buffer


def compute_sha256(filepath: Path) -> str:
    """Compute the SHA-256 hex digest of a file."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(HASH_CHUNK_SIZE):
            h.update(chunk)
    return h.hexdigest()


def is_icloud_placeholder(filepath: Path) -> bool:
    """
    Check if a file is an iCloud placeholder (not yet downloaded).

    iCloud placeholders have a '.' prefix and '.icloud' extension:
        .example.json.icloud
    """
    name = filepath.name
    return name.startswith(".") and name.endswith(".icloud")


def wait_for_download(filepath: Path, timeout: int = 60) -> bool:
    """
    Wait for an iCloud file to finish downloading.

    Returns True if the file is ready, False if it timed out or
    is still a placeholder.
    """
    import time

    if not is_icloud_placeholder(filepath):
        # It's a real file — check it exists and has content
        return filepath.exists() and filepath.stat().st_size > 0

    # Derive the real filename from the placeholder name
    # .example.json.icloud -> example.json
    real_name = filepath.name[1:].rsplit(".icloud", 1)[0]
    real_path = filepath.parent / real_name

    logger.info("Waiting for iCloud download: %s -> %s", filepath.name, real_name)

    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if real_path.exists() and real_path.stat().st_size > 0:
            logger.info("iCloud download complete: %s", real_name)
            return True
        time.sleep(2)

    logger.warning("iCloud download timed out after %ds: %s", timeout, real_name)
    return False
