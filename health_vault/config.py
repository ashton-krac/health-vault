"""
Configuration for Health Vault.

All paths and connection parameters are centralized here.
Environment variables can override defaults via a .env file.
"""

import os
import logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv

load_dotenv()

# ──────────────────────────────────────────────────────────────────────
# Paths
# ──────────────────────────────────────────────────────────────────────

ICLOUD_BASE = Path.home() / "Library" / "Mobile Documents" / "com~apple~CloudDocs"

INBOX_DIR = Path(os.getenv(
    "HEALTH_VAULT_INBOX",
    str(ICLOUD_BASE / "health_vault" / "inbox"),
))

ARCHIVE_DIR = Path(os.getenv(
    "HEALTH_VAULT_ARCHIVE",
    str(ICLOUD_BASE / "health_vault" / "archive"),
))

LOG_DIR = Path.home() / "Library" / "Logs"

# ──────────────────────────────────────────────────────────────────────
# Database
# ──────────────────────────────────────────────────────────────────────

DB_NAME = os.getenv("HEALTH_VAULT_DB", "health_vault")
DB_USER = os.getenv("HEALTH_VAULT_DB_USER", os.getenv("USER", "ashtoncoghlan"))
DB_HOST = os.getenv("HEALTH_VAULT_DB_HOST", "")  # empty = Unix socket (peer auth)
DB_PORT = os.getenv("HEALTH_VAULT_DB_PORT", "5432")

# Build a conninfo string compatible with psycopg v3
_parts = [f"dbname={DB_NAME}", f"user={DB_USER}"]
if DB_HOST:
    _parts.append(f"host={DB_HOST}")
if DB_PORT:
    _parts.append(f"port={DB_PORT}")

DATABASE_URL = os.getenv("HEALTH_VAULT_DATABASE_URL", " ".join(_parts))

# ──────────────────────────────────────────────────────────────────────
# Watcher
# ──────────────────────────────────────────────────────────────────────

FILE_PATTERN = "*.json"
ICLOUD_SETTLE_DELAY = int(os.getenv("HEALTH_VAULT_SETTLE_DELAY", "5"))  # seconds
SWEEP_INTERVAL = int(os.getenv("HEALTH_VAULT_SWEEP_INTERVAL", "60"))    # seconds
BATCH_SIZE = int(os.getenv("HEALTH_VAULT_BATCH_SIZE", "500"))           # rows per INSERT

# ──────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────


def setup_logging() -> logging.Logger:
    """Configure rotating file + console logging."""
    logger = logging.getLogger("health_vault")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # Rotating file handler
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        LOG_DIR / "health_vault.log",
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
