"""SQLite-backed runtime progress persistence for downloader resume."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path

from .mirror_planner import DownloadJob


class SQLiteProgressStore:
    """Persist downloader progress incrementally in SQLite."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.connection = sqlite3.connect(self.path, isolation_level=None)
        self._initialize()

    def _initialize(self) -> None:
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA temp_store=FILE")

        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS results (
                relative_key TEXT PRIMARY KEY,
                result TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS completed_directories (
                relative_key TEXT PRIMARY KEY,
                updated_at TEXT NOT NULL
            )
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                relative_key TEXT NOT NULL,
                is_directory INTEGER NOT NULL,
                source_entry TEXT NOT NULL,
                candidate_urls TEXT NOT NULL,
                bases TEXT NOT NULL,
                status TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self.connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_pending_jobs_status
            ON pending_jobs(status, is_directory, id)
            """
        )
        self.connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_jobs_dedupe
            ON pending_jobs(relative_key, is_directory, status)
            """
        )

    def reset_active_jobs(self) -> None:
        """Move interrupted in-flight jobs back to pending state after restart."""
        self.connection.execute(
            """
            UPDATE pending_jobs
            SET status = 'pending', updated_at = ?
            WHERE status = 'active'
            """,
            (datetime.now().isoformat(),),
        )

    def load_completed_directories(self) -> set[str]:
        """Load already enumerated directories."""
        rows = self.connection.execute(
            "SELECT relative_key FROM completed_directories"
        ).fetchall()
        return {str(row[0]) for row in rows}

    def load_pending_jobs(self) -> list[DownloadJob]:
        """Load pending jobs in insertion order."""
        rows = self.connection.execute(
            """
            SELECT relative_key, is_directory, source_entry, candidate_urls, bases
            FROM pending_jobs
            WHERE status IN ('pending', 'active')
            ORDER BY id ASC
            """
        ).fetchall()

        loaded: list[DownloadJob] = []
        for (
            relative_key,
            is_directory,
            source_entry,
            candidate_urls_json,
            bases_json,
        ) in rows:
            try:
                candidate_urls_raw = json.loads(candidate_urls_json)
                bases_raw = json.loads(bases_json)
            except (TypeError, ValueError, json.JSONDecodeError):
                continue

            candidate_urls = (
                [str(item) for item in candidate_urls_raw if isinstance(item, str)]
                if isinstance(candidate_urls_raw, list)
                else []
            )
            bases = (
                [str(item) for item in bases_raw if isinstance(item, str)]
                if isinstance(bases_raw, list)
                else []
            )
            loaded.append(
                DownloadJob(
                    relative_key=str(relative_key),
                    candidate_urls=candidate_urls,
                    is_directory=bool(is_directory),
                    source_entry=str(source_entry),
                    bases=bases,
                )
            )

        return loaded

    def enqueue_job(self, job: DownloadJob) -> None:
        """Insert a queued job if not already queued."""
        try:
            self.connection.execute(
                """
                INSERT INTO pending_jobs (
                    relative_key, is_directory, source_entry, candidate_urls, bases, status, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    job.relative_key,
                    int(job.is_directory),
                    job.source_entry,
                    json.dumps(job.candidate_urls),
                    json.dumps(job.bases),
                    datetime.now().isoformat(),
                ),
            )
        except sqlite3.IntegrityError:
            return

    def mark_job_active(self, job: DownloadJob) -> None:
        """Mark one queued job instance as active right before execution."""
        now = datetime.now().isoformat()
        cursor = self.connection.execute(
            """
            UPDATE pending_jobs
            SET status = 'active', updated_at = ?
            WHERE id = (
                SELECT id
                FROM pending_jobs
                WHERE relative_key = ? AND is_directory = ? AND status = 'pending'
                ORDER BY id ASC
                LIMIT 1
            )
            """,
            (now, job.relative_key, int(job.is_directory)),
        )
        if cursor.rowcount != 0:
            return

        self.enqueue_job(job)
        self.connection.execute(
            """
            UPDATE pending_jobs
            SET status = 'active', updated_at = ?
            WHERE id = (
                SELECT id
                FROM pending_jobs
                WHERE relative_key = ? AND is_directory = ? AND status = 'pending'
                ORDER BY id ASC
                LIMIT 1
            )
            """,
            (now, job.relative_key, int(job.is_directory)),
        )

    def mark_job_done(self, job: DownloadJob) -> None:
        """Remove one active (or pending) queue record after job completion."""
        cursor = self.connection.execute(
            """
            DELETE FROM pending_jobs
            WHERE id = (
                SELECT id
                FROM pending_jobs
                WHERE relative_key = ? AND is_directory = ? AND status = 'active'
                ORDER BY id ASC
                LIMIT 1
            )
            """,
            (job.relative_key, int(job.is_directory)),
        )
        if cursor.rowcount != 0:
            return

        self.connection.execute(
            """
            DELETE FROM pending_jobs
            WHERE id = (
                SELECT id
                FROM pending_jobs
                WHERE relative_key = ? AND is_directory = ? AND status = 'pending'
                ORDER BY id ASC
                LIMIT 1
            )
            """,
            (job.relative_key, int(job.is_directory)),
        )

    def get_result(self, relative_key: str) -> str | None:
        """Read one saved result value if it exists."""
        row = self.connection.execute(
            "SELECT result FROM results WHERE relative_key = ?",
            (relative_key,),
        ).fetchone()
        if row is None:
            return None
        return str(row[0])

    def upsert_result(self, relative_key: str, result: str) -> None:
        """Persist latest job result."""
        self.connection.execute(
            """
            INSERT INTO results(relative_key, result, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(relative_key)
            DO UPDATE SET
                result = excluded.result,
                updated_at = excluded.updated_at
            """,
            (relative_key, result, datetime.now().isoformat()),
        )

    def mark_directory_completed(self, relative_key: str) -> None:
        """Persist a completed directory-enumeration marker."""
        self.connection.execute(
            """
            INSERT INTO completed_directories(relative_key, updated_at)
            VALUES (?, ?)
            ON CONFLICT(relative_key)
            DO UPDATE SET updated_at = excluded.updated_at
            """,
            (relative_key, datetime.now().isoformat()),
        )

    def fetch_failed_results(self) -> dict[str, str]:
        """Read failed results to keep final in-memory footprint small."""
        rows = self.connection.execute(
            "SELECT relative_key, result FROM results WHERE result LIKE 'failed:%'"
        ).fetchall()
        return {str(relative_key): str(result) for relative_key, result in rows}

    def close(self) -> None:
        self.connection.close()
