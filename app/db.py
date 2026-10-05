"""SQLite access: schema, short-lived connections, batch/row queries."""
from __future__ import annotations

import sqlite3
from typing import Iterator

from .config import BUSY_TIMEOUT_SECONDS, DB_PATH

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS batches (
    id         TEXT PRIMARY KEY,
    row_count  INTEGER NOT NULL CHECK (row_count >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS records (
    batch_id TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    id       TEXT NOT NULL,
    message  TEXT NOT NULL,
    PRIMARY KEY (batch_id, position)
);

CREATE INDEX IF NOT EXISTS idx_records_batch ON records (batch_id, position);
"""


def connect(database: str | sqlite3.Connection = str(DB_PATH)) -> sqlite3.Connection:
    conn = sqlite3.connect(database, timeout=BUSY_TIMEOUT_SECONDS)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=%d" % int(BUSY_TIMEOUT_SECONDS * 1000))
    if database != ":memory:":
        conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    """Idempotent schema initialization (also used by the Docker entrypoint)."""
    conn = connect()
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def commit_spool(main_conn: sqlite3.Connection, batch_id: str, spool_path: str) -> int:
    """Attach a validated staging DB and publish its rows in one transaction.

    Either the whole batch becomes visible or nothing is left behind: on any
    failure the transaction is rolled back and the batch id never exists.
    """
    main_conn.execute("ATTACH DATABASE ? AS stage", (spool_path,))
    try:
        row = main_conn.execute("SELECT COUNT(*) FROM stage.staged").fetchone()
        row_count = row[0]
        with main_conn:  # single BEGIN/COMMIT
            main_conn.execute(
                "INSERT INTO batches (id, row_count) VALUES (?, ?)",
                (batch_id, row_count),
            )
            main_conn.execute(
                "INSERT INTO records (batch_id, position, id, message) "
                "SELECT ?, position, id, message FROM stage.staged",
                (batch_id,),
            )
    except Exception:
        try:
            main_conn.rollback()
            main_conn.execute("DETACH DATABASE stage")
        except sqlite3.Error:
            pass
        raise
    main_conn.execute("DETACH DATABASE stage")
    return row_count


def batch_exists(conn: sqlite3.Connection, batch_id: str) -> bool:
    row = conn.execute("SELECT 1 FROM batches WHERE id=?", (batch_id,)).fetchone()
    return row is not None


def list_batches(conn: sqlite3.Connection, limit: int,
                 after_id: str | None) -> list[sqlite3.Row]:
    if after_id is None:
        return list(conn.execute(
            "SELECT id, row_count, created_at FROM batches "
            "ORDER BY id LIMIT ?", (limit,)))
    return list(conn.execute(
        "SELECT id, row_count, created_at FROM batches "
        "WHERE id > ? ORDER BY id LIMIT ?", (after_id, limit)))


def get_batch(conn: sqlite3.Connection, batch_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT id, row_count, created_at FROM batches WHERE id=?",
        (batch_id,),
    ).fetchone()


def iter_batch_rows(conn: sqlite3.Connection, batch_id: str,
                    limit: int, offset: int) -> Iterator[sqlite3.Row]:
    # Keep ordering identical to the uploaded NDJSON stream.
    return conn.execute(
        "SELECT position, id, message FROM records "
        "WHERE batch_id=? ORDER BY position LIMIT ? OFFSET ?",
        (batch_id, limit, offset),
    )
