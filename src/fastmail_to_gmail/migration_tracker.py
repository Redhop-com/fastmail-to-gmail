"""SQLite-based state tracking for resumable email migration."""

import logging
import sqlite3
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

STATUS_PENDING = "pending"
STATUS_FETCHED = "fetched"
STATUS_UPLOADED = "uploaded"
STATUS_VERIFIED = "verified"
STATUS_SKIPPED = "skipped"
STATUS_FAILED = "failed"


class MigrationTracker:
    """Track per-email migration state in SQLite for safe resume."""

    def __init__(self, db_path: str = "migration_state.db"):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS migration_state (
                message_id TEXT PRIMARY KEY,
                date TEXT,
                from_addr TEXT,
                subject TEXT,
                fastmail_folder TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                gmail_message_id TEXT,
                gmail_labels TEXT,
                error TEXT,
                fastmail_blob_id TEXT,
                size INTEGER,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS migration_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at TEXT,
                finished_at TEXT,
                total INTEGER,
                success INTEGER,
                skipped INTEGER,
                failed INTEGER
            )
        """)
        self._conn.commit()

    def start_run(self, total: int) -> int:
        """Record the start of a migration run. Returns run ID."""
        cursor = self._conn.execute(
            "INSERT INTO migration_runs (started_at, total, success, skipped, failed) VALUES (?, ?, 0, 0, 0)",
            (datetime.utcnow().isoformat(), total),
        )
        self._conn.commit()
        return cursor.lastrowid

    def finish_run(self, run_id: int, success: int, skipped: int, failed: int):
        """Record completion of a migration run."""
        self._conn.execute(
            "UPDATE migration_runs SET finished_at=?, success=?, skipped=?, failed=? WHERE id=?",
            (datetime.utcnow().isoformat(), success, skipped, failed, run_id),
        )
        self._conn.commit()

    def add_email(self, message_id: str, date: str, from_addr: str,
                  subject: str, fastmail_folder: str):
        """Add an email to the migration tracker as pending."""
        now = datetime.utcnow().isoformat()
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO migration_state
                   (message_id, date, from_addr, subject, fastmail_folder, status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (message_id, date, from_addr, subject, fastmail_folder, STATUS_PENDING, now, now),
            )
            self._conn.commit()

    def add_emails_batch(self, emails: list[dict]):
        """Bulk-register emails as pending in one transaction."""
        now = datetime.utcnow().isoformat()
        rows = [
            (em["message_id"], em["date"], em["from_addr"],
             em["subject"], em["fastmail_folder"], STATUS_PENDING, now, now)
            for em in emails
        ]
        with self._lock:
            self._conn.executemany(
                """INSERT OR IGNORE INTO migration_state
                   (message_id, date, from_addr, subject, fastmail_folder,
                    status, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                rows,
            )
            self._conn.commit()

    def get_status(self, message_id: str) -> str | None:
        """Get the current status of an email."""
        cursor = self._conn.execute(
            "SELECT status FROM migration_state WHERE message_id = ?", (message_id,)
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def update_status(self, message_id: str, status: str,
                      gmail_message_id: str | None = None,
                      gmail_labels: str | None = None,
                      error: str | None = None,
                      fastmail_blob_id: str | None = None,
                      size: int | None = None):
        """Update the status and metadata of a tracked email. Thread-safe."""
        now = datetime.utcnow().isoformat()
        fields = ["status=?", "updated_at=?"]
        values: list = [status, now]

        if gmail_message_id is not None:
            fields.append("gmail_message_id=?")
            values.append(gmail_message_id)
        if gmail_labels is not None:
            fields.append("gmail_labels=?")
            values.append(gmail_labels)
        if error is not None:
            fields.append("error=?")
            values.append(error)
        if fastmail_blob_id is not None:
            fields.append("fastmail_blob_id=?")
            values.append(fastmail_blob_id)
        if size is not None:
            fields.append("size=?")
            values.append(size)

        values.append(message_id)
        with self._lock:
            self._conn.execute(
                f"UPDATE migration_state SET {', '.join(fields)} WHERE message_id = ?",
                values,
            )
            self._conn.commit()

    def get_pending(self) -> list[dict]:
        """Get all emails that still need to be migrated (pending or fetched)."""
        cursor = self._conn.execute(
            """SELECT message_id, date, from_addr, subject, fastmail_folder
               FROM migration_state
               WHERE status IN (?, ?)
               ORDER BY date""",
            (STATUS_PENDING, STATUS_FETCHED),
        )
        return [
            {
                "message_id": row[0],
                "date": row[1],
                "from_addr": row[2],
                "subject": row[3],
                "fastmail_folder": row[4],
            }
            for row in cursor.fetchall()
        ]

    def get_summary(self) -> dict[str, int]:
        """Get a count of emails by status."""
        cursor = self._conn.execute(
            "SELECT status, COUNT(*) FROM migration_state GROUP BY status"
        )
        return dict(cursor.fetchall())

    def get_all(self) -> list[dict]:
        """Get all tracked emails with full details."""
        cursor = self._conn.execute(
            """SELECT message_id, date, from_addr, subject, fastmail_folder,
                      status, gmail_message_id, gmail_labels, error
               FROM migration_state"""
        )
        return [
            {
                "message_id": row[0],
                "date": row[1],
                "from_addr": row[2],
                "subject": row[3],
                "fastmail_folder": row[4],
                "status": row[5],
                "gmail_message_id": row[6] or "",
                "gmail_labels": row[7] or "",
                "error": row[8] or "",
            }
            for row in cursor.fetchall()
        ]

    def has_existing_state(self) -> bool:
        """Check if there's any prior migration state."""
        cursor = self._conn.execute("SELECT COUNT(*) FROM migration_state")
        return cursor.fetchone()[0] > 0

    def clear(self):
        """Clear all migration state."""
        self._conn.execute("DELETE FROM migration_state")
        self._conn.commit()
        logger.info("Migration state cleared.")

    def close(self):
        self._conn.close()
