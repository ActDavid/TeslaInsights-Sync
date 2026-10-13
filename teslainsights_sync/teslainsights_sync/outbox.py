"""Durable SQLite outbox and cursor store in /data.

Batches and the cursors that produced them are written in one transaction, so a
crash can never advance a cursor without the data being queued for delivery.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path

from . import payload

_SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
  key TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  kind TEXT NOT NULL,
  body BLOB NOT NULL,
  record_count INTEGER NOT NULL,
  created_at REAL NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at REAL NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  sent_at REAL,
  last_error TEXT
);
CREATE INDEX IF NOT EXISTS ix_batches_due ON batches (status, next_attempt_at);
CREATE TABLE IF NOT EXISTS cursors (
  name TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at REAL NOT NULL
);
"""

MAX_BACKOFF_SECONDS = 3600


@dataclass(frozen=True)
class Batch:
    key: str
    source: str
    kind: str
    body: bytes
    record_count: int
    attempts: int


class Outbox:
    def __init__(self, path: Path | str, clock=time.time):
        self._db = sqlite3.connect(str(path), isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.executescript(_SCHEMA)
        self._clock = clock

    def close(self) -> None:
        self._db.close()

    def get_cursor(self, name: str, default: str | None = None) -> str | None:
        row = self._db.execute("SELECT value FROM cursors WHERE name = ?", (name,)).fetchone()
        return row[0] if row else default

    def set_cursors(self, values: dict[str, str]) -> None:
        self.enqueue("local", "cursor", [], cursor_updates=values)

    def enqueue(
        self,
        source: str,
        kind: str,
        records: list[dict],
        *,
        cursor_updates: dict[str, str] | None = None,
        payload_cursor: str | None = None,
        error: str | None = None,
        max_records: int = 500,
    ) -> int:
        """Queues records and moves cursors atomically. Returns the number of new batches."""
        bodies = payload.build_batches(source, kind, records, cursor=payload_cursor, error=error, max_records=max_records)
        now = self._clock()
        added = 0
        with self._transaction():
            for body, count in bodies:
                key = payload.idempotency_key(source, kind, body)
                cur = self._db.execute(
                    "INSERT OR IGNORE INTO batches (key, source, kind, body, record_count, created_at, next_attempt_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (key, source, kind, body, count, now, now),
                )
                added += cur.rowcount
            for name, value in (cursor_updates or {}).items():
                self._db.execute(
                    "INSERT INTO cursors (name, value, updated_at) VALUES (?, ?, ?)"
                    " ON CONFLICT(name) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                    (name, str(value), now),
                )
        return added

    def due(self, limit: int = 50) -> list[Batch]:
        rows = self._db.execute(
            "SELECT key, source, kind, body, record_count, attempts FROM batches"
            " WHERE status = 'pending' AND next_attempt_at <= ? ORDER BY created_at, rowid LIMIT ?",
            (self._clock(), limit),
        ).fetchall()
        return [Batch(*row) for row in rows]

    def pending_count(self) -> int:
        return self._db.execute("SELECT COUNT(*) FROM batches WHERE status = 'pending'").fetchone()[0]

    def mark_sent(self, key: str) -> None:
        self._db.execute(
            "UPDATE batches SET status = 'sent', sent_at = ?, attempts = attempts + 1, last_error = NULL WHERE key = ?",
            (self._clock(), key),
        )

    def mark_retry(self, key: str, error: str) -> None:
        row = self._db.execute("SELECT attempts FROM batches WHERE key = ?", (key,)).fetchone()
        attempts = (row[0] if row else 0) + 1
        delay = min(MAX_BACKOFF_SECONDS, 15 * 2 ** min(attempts - 1, 12))
        self._db.execute(
            "UPDATE batches SET attempts = ?, next_attempt_at = ?, last_error = ? WHERE key = ?",
            (attempts, self._clock() + delay, error[:500], key),
        )

    def mark_dead(self, key: str, error: str) -> None:
        self._db.execute(
            "UPDATE batches SET status = 'dead', attempts = attempts + 1, last_error = ? WHERE key = ?",
            (error[:500], key),
        )

    def stats(self) -> dict[str, int]:
        return dict(self._db.execute("SELECT status, COUNT(*) FROM batches GROUP BY status").fetchall())

    def prune(self, keep_days: int = 30) -> None:
        """Drops delivered payloads after a while; the server still deduplicates by key."""
        self._db.execute(
            "DELETE FROM batches WHERE status = 'sent' AND sent_at < ?",
            (self._clock() - keep_days * 86400,),
        )

    def _transaction(self):
        db = self._db

        class _Tx:
            def __enter__(self):
                db.execute("BEGIN IMMEDIATE")

            def __exit__(self, exc_type, exc, tb):
                db.execute("ROLLBACK" if exc_type else "COMMIT")
                return False

        return _Tx()
