"""Shared SQLite access for the delivery service.

One connection per process guarded by an RLock keeps in-process access
serialized; WAL + busy_timeout make the same file safe to share between the
two delivery instances (separate processes/containers on one host volume).
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager

DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(__file__), "..", "delivery.db")

_lock = threading.RLock()
_conn: sqlite3.Connection | None = None


def conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
        _conn = sqlite3.connect(DB_PATH, check_same_thread=False, isolation_level=None)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA busy_timeout=8000")
        _conn.execute("PRAGMA synchronous=NORMAL")
    return _conn


@contextmanager
def tx():
    """Short write transaction (BEGIN IMMEDIATE ... COMMIT/ROLLBACK)."""
    with _lock:
        c = conn()
        c.execute("BEGIN IMMEDIATE")
        try:
            yield c
        except Exception:
            c.execute("ROLLBACK")
            raise
        else:
            c.execute("COMMIT")


def init_schema(sql: str) -> None:
    with _lock:
        conn().executescript(sql)


def query(sql: str, args=()):
    with _lock:
        return conn().execute(sql, args).fetchall()


def query_one(sql: str, args=()):
    rows = query(sql, args)
    return rows[0] if rows else None


def execute(sql: str, args=()):
    with _lock:
        return conn().execute(sql, args)
