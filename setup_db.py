#!/usr/bin/env python3
"""
Health Vault — One-time Database Bootstrap

Creates the 'health_vault' database and applies the schema.
Safe to re-run (idempotent).

Usage:
    python3 setup_db.py
"""

import os
import sys

import psycopg
from psycopg.rows import dict_row


DB_NAME = os.getenv("HEALTH_VAULT_DB", "health_vault")
DB_USER = os.getenv("HEALTH_VAULT_DB_USER", os.getenv("USER", "ashtoncoghlan"))


def main():
    print(f"🔧 Health Vault Database Bootstrap")
    print(f"   Database: {DB_NAME}")
    print(f"   User:     {DB_USER}")
    print()

    # ── Step 1: Connect to default 'postgres' database ────────────────
    try:
        admin_conn = psycopg.connect(
            f"dbname=postgres user={DB_USER}",
            autocommit=True,
            row_factory=dict_row,
        )
    except Exception as e:
        print(f"❌ Cannot connect to PostgreSQL: {e}")
        print("   Make sure PostgreSQL is running: brew services start postgresql@17")
        sys.exit(1)

    # ── Step 2: Create database if it doesn't exist ───────────────────
    with admin_conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (DB_NAME,),
        )
        exists = cur.fetchone() is not None

    if exists:
        print(f"✅ Database '{DB_NAME}' already exists")
    else:
        with admin_conn.cursor() as cur:
            # CREATE DATABASE can't run inside a transaction
            cur.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"✅ Created database '{DB_NAME}'")

    admin_conn.close()

    # ── Step 3: Apply schema ──────────────────────────────────────────
    from health_vault.db import get_connection, ensure_schema

    conn = get_connection()
    ensure_schema(conn)
    conn.close()

    print(f"✅ Schema applied successfully")
    print()
    print("You can now run: python3 main.py")


if __name__ == "__main__":
    main()
