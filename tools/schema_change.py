"""Simulated schema change: adds core.transactions.installments while everything is running.

    docker compose exec simulator python schema_change.py
"""
import os

import psycopg

DSN = (
    f"host={os.getenv('PG_HOST', 'postgres')} dbname={os.environ['PG_DB']} "
    f"user={os.environ['PG_ADMIN_USER']} password={os.environ['PG_ADMIN_PASSWORD']}"
)

with psycopg.connect(DSN, autocommit=True) as conn:
    conn.execute("SET lock_timeout = '5s'")
    conn.execute("ALTER TABLE core.transactions ADD COLUMN IF NOT EXISTS installments SMALLINT")
    print("core.transactions.installments added (new rows get a value; old rows stay NULL).")
