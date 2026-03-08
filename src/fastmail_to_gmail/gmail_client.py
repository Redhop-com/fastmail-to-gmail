"""Gmail API client for fetching email metadata."""

import base64
import email
import logging
import os
import sqlite3
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


class GmailClient:
    """Read-only Gmail API client."""

    def __init__(self, credentials_path: str = "credentials.json",
                 token_path: str = "token.json",
                 db_path: str = "migration_cache.db",
                 force_headless: bool = False):
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.db_path = db_path
        self.force_headless = force_headless
        self.service = None
        self._init_db()

    def _init_db(self):
        """Initialize the SQLite cache table for Gmail emails."""
        conn = sqlite3.connect(self.db_path)
        # Drop old table with broken PRIMARY KEY (message_id, labels)
        # that caused massive data loss via collisions.
        try:
            cols = [r[1] for r in conn.execute("PRAGMA table_info(gmail_emails)").fetchall()]
            if "gmail_id" not in cols:
                conn.execute("DROP TABLE IF EXISTS gmail_emails")
                logger.info("Dropped old gmail_emails table (missing gmail_id column).")
        except Exception:
            pass
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gmail_emails (
                gmail_id TEXT PRIMARY KEY,
                message_id TEXT,
                date TEXT,
                from_addr TEXT,
                to_addr TEXT,
                subject TEXT,
                size INTEGER,
                labels TEXT,
                fetched_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gmail_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()
        conn.close()

    def _authenticate(self):
        """Authenticate via OAuth2, reusing token.json if available."""
        creds = None
        if os.path.exists(self.token_path):
            creds = Credentials.from_authorized_user_file(self.token_path, SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logger.info("Refreshing expired Gmail token...")
                creds.refresh(Request())
            else:
                if not os.path.exists(self.credentials_path):
                    raise RuntimeError(
                        f"Google credentials file not found at '{self.credentials_path}'.\n"
                        "  1. Go to Google Cloud Console → APIs & Services → Credentials\n"
                        "  2. Create an OAuth 2.0 Client ID (Desktop application)\n"
                        "  3. Enable the Gmail API in your project\n"
                        "  4. Download the credentials JSON and save as credentials.json\n"
                        "  5. Pass the path with --google-creds if not in current directory"
                    )
                from fastmail_to_gmail.auth import run_oauth_flow
                creds = run_oauth_flow(self.credentials_path, SCOPES,
                                       force_headless=self.force_headless)

            with open(self.token_path, "w") as f:
                f.write(creds.to_json())
            logger.info("Gmail token saved to %s", self.token_path)

        return creds

    def connect(self):
        """Establish connection to the Gmail API."""
        logger.info("Connecting to Gmail API...")
        try:
            creds = self._authenticate()
            self.service = build("gmail", "v1", credentials=creds)
            # Quick test: get profile
            profile = self.service.users().getProfile(userId="me").execute()
            logger.info("Connected to Gmail. Email: %s, Total messages: %s",
                        profile.get("emailAddress"), profile.get("messagesTotal"))
        except Exception as e:
            raise RuntimeError(
                "Failed to connect to Gmail API.\n"
                "  1. Ensure credentials.json is a valid OAuth2 client credential\n"
                "  2. Ensure Gmail API is enabled in your Google Cloud project\n"
                "  3. Delete token.json and re-authenticate if token is corrupted\n"
                f"  Error: {e}"
            ) from e

    def test_connection(self) -> bool:
        """Test authentication without fetching emails."""
        try:
            self.connect()
            return True
        except Exception as e:
            logger.error("Gmail connection test failed: %s", e)
            return False

    def _api_call_with_retry(self, request):
        """Execute a Gmail API request with retry and backoff."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return request.execute()
            except HttpError as e:
                if e.resp.status in (429, 500, 503):
                    wait = (2 ** (attempt + 1)) + random.uniform(0, 1)
                    logger.warning("Gmail API error %d. Retrying in %.1fs (attempt %d/%d)",
                                   e.resp.status, wait, attempt + 1, max_retries)
                    time.sleep(wait)
                else:
                    raise
            except Exception:
                if attempt < max_retries - 1:
                    wait = (2 ** (attempt + 1)) + random.uniform(0, 1)
                    logger.warning("Gmail request error. Retrying in %.1fs (attempt %d/%d)",
                                   wait, attempt + 1, max_retries)
                    time.sleep(wait)
                else:
                    raise
        raise RuntimeError(f"Gmail API request failed after {max_retries} attempts")

    def get_labels(self) -> dict:
        """Fetch all Gmail labels. Returns {label_id: label_name}."""
        result = self._api_call_with_retry(
            self.service.users().labels().list(userId="me")
        )
        labels = {}
        for label in result.get("labels", []):
            labels[label["id"]] = label["name"]
        logger.info("Found %d Gmail labels.", len(labels))
        return labels

    def _build_query(self, folder: str | None, date_from: str | None, date_to: str | None) -> str:
        """Build a Gmail search query string."""
        parts = []
        if folder:
            parts.append(f"label:{folder}")
        if date_from:
            parts.append(f"after:{date_from}")
        if date_to:
            parts.append(f"before:{date_to}")
        return " ".join(parts) if parts else ""

    def _parse_headers(self, headers: list) -> dict:
        """Extract common headers from Gmail message headers list."""
        result = {}
        for h in headers:
            name = h.get("name", "").lower()
            value = h.get("value", "")
            if name == "message-id":
                # Strip angle brackets
                result["message_id"] = value.strip("<>").strip()
            elif name == "date":
                result["date"] = value
            elif name == "from":
                result["from_addr"] = value
            elif name == "to":
                result["to_addr"] = value
            elif name == "subject":
                result["subject"] = value
        return result

    def _get_cached_gmail_ids(self) -> set[str]:
        """Return all gmail_ids currently in the cache."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT gmail_id FROM gmail_emails")
        ids = {row[0] for row in cursor.fetchall()}
        conn.close()
        return ids

    def _save_batch_to_cache(self, emails: list[dict]):
        """Incrementally save a batch of emails to the SQLite cache."""
        if not emails:
            return
        conn = sqlite3.connect(self.db_path)
        now = datetime.utcnow().isoformat()
        conn.executemany(
            """INSERT OR REPLACE INTO gmail_emails
               (gmail_id, message_id, date, from_addr, to_addr, subject, size, labels, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    e.get("gmail_id", ""), e["message_id"], e["date"],
                    e["from_addr"], e["to_addr"], e["subject"], e["size"],
                    e["labels"], now,
                )
                for e in emails
            ],
        )
        conn.commit()
        conn.close()

    def _process_msg(self, msg, labels_map_ref):
        """Convert a raw Gmail API message response to our email dict."""
        headers = self._parse_headers(msg.get("payload", {}).get("headers", []))
        label_ids = msg.get("labelIds", [])
        label_names = [labels_map_ref.get(lid, lid) for lid in label_ids]
        from_raw = headers.get("from_addr", "")
        from_addr = self._extract_email(from_raw)
        to_raw = headers.get("to_addr", "")
        to_addr = ", ".join(
            self._extract_email(part.strip())
            for part in to_raw.split(",")
        ) if to_raw else ""
        return {
            "gmail_id": msg.get("id", ""),
            "message_id": headers.get("message_id", ""),
            "date": headers.get("date", ""),
            "from_addr": from_addr,
            "to_addr": to_addr,
            "subject": headers.get("subject", ""),
            "size": msg.get("sizeEstimate", 0),
            "labels": ", ".join(sorted(label_names)),
        }

    def fetch_emails(self, folder: str | None = None, date_from: str | None = None,
                     date_to: str | None = None, use_cache: bool = True) -> list[dict]:
        """Fetch all email metadata from Gmail.

        Incremental & resumable: saves each batch to SQLite immediately.
        On subsequent calls, only fetches gmail_ids not already in cache.

        Returns a list of dicts with keys:
            gmail_id, message_id, date, from_addr, to_addr, subject, size, labels
        """
        if self.service is None:
            self.connect()

        # Check cache — return cached data if we have it and aren't refreshing
        if use_cache:
            cached = self._load_from_cache()
            if cached:
                logger.info("Loaded %d Gmail emails from cache.", len(cached))
                return cached

        labels_map = self.get_labels()
        query = self._build_query(folder, date_from, date_to)

        # Step 1: List all message IDs from Gmail API
        all_msg_ids = []
        page_token = None
        logger.info("Listing Gmail messages%s...", f" (query: {query})" if query else "")

        while True:
            list_args = {"userId": "me", "maxResults": 500}
            if query:
                list_args["q"] = query
            if page_token:
                list_args["pageToken"] = page_token

            result = self._api_call_with_retry(
                self.service.users().messages().list(**list_args)
            )
            messages = result.get("messages", [])
            all_msg_ids.extend(m["id"] for m in messages)

            page_token = result.get("nextPageToken")
            if not page_token:
                break

        total = len(all_msg_ids)
        logger.info("Found %d Gmail messages total.", total)

        if total == 0:
            return []

        # Step 2: Check which gmail_ids are already cached
        cached_ids = self._get_cached_gmail_ids()
        missing_ids = [mid for mid in all_msg_ids if mid not in cached_ids]
        logger.info("Already cached: %d. Need to fetch: %d.",
                    len(cached_ids), len(missing_ids))

        if missing_ids:
            self._fetch_and_cache_messages(missing_ids, labels_map)

        # Step 3: Update meta timestamp
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT OR REPLACE INTO gmail_meta (key, value) VALUES (?, ?)",
            ("last_fetch", datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()

        # Step 4: Return all cached emails
        all_emails = self._load_from_cache() or []
        logger.info("Returning %d Gmail email records.", len(all_emails))
        return all_emails

    def _fetch_and_cache_messages(self, msg_ids: list[str], labels_map: dict):
        """Fetch messages by ID using concurrent batch requests, saving incrementally.

        Uses multiple threads with a rate limiter to run batch API requests in
        parallel while staying within Gmail API quota (250 units/sec).
        Strategy: dispatch 1 batch/second, with up to 5 in-flight simultaneously.
        Each batch of 50 * 5 units = 250 units/sec exactly at quota limit.
        """
        total = len(msg_ids)
        batch_size = 50
        max_workers = 5
        fetched_count = 0
        failed_ids = []
        save_lock = threading.Lock()
        progress_lock = threading.Lock()
        # Rate limiter: only dispatch 1 batch per second
        dispatch_lock = threading.Lock()
        last_dispatch_time = [0.0]

        creds = self._authenticate()

        # Create one service instance per worker thread (thread-local storage)
        thread_local = threading.local()

        def _get_thread_service():
            if not hasattr(thread_local, "service"):
                thread_local.service = build("gmail", "v1", credentials=creds)
            return thread_local.service

        def _rate_limited_fetch(batch_ids_chunk):
            """Fetch a batch with rate limiting on dispatch."""
            # Rate limit: wait until 1 second since last dispatch
            with dispatch_lock:
                now = time.time()
                wait = max(0, 1.0 - (now - last_dispatch_time[0]))
                if wait > 0:
                    time.sleep(wait)
                last_dispatch_time[0] = time.time()

            service = _get_thread_service()
            results = []
            failed = []

            def callback(request_id, response, exception):
                if exception:
                    failed.append(request_id)
                else:
                    results.append(response)

            batch = service.new_batch_http_request(callback=callback)
            for msg_id in batch_ids_chunk:
                batch.add(
                    service.users().messages().get(
                        userId="me", id=msg_id, format="metadata",
                        metadataHeaders=["Message-ID", "Date", "From", "To", "Subject"],
                    ),
                    request_id=msg_id,
                )

            try:
                batch.execute()
            except Exception as e:
                logger.warning("Batch execute error: %s", e)
                return [], list(batch_ids_chunk)

            emails = [self._process_msg(msg, labels_map) for msg in results]
            return emails, failed

        # --- Main pass: Rate-limited concurrent batch fetch ---
        progress = tqdm(total=total, desc="Fetching Gmail emails", unit="email")
        batch_chunks = [msg_ids[i:i + batch_size] for i in range(0, total, batch_size)]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for chunk in batch_chunks:
                future = executor.submit(_rate_limited_fetch, chunk)
                futures[future] = chunk

            for future in as_completed(futures):
                chunk = futures[future]
                try:
                    emails, batch_failed = future.result()
                except Exception as e:
                    logger.warning("Thread error: %s", e)
                    emails = []
                    batch_failed = list(chunk)

                if emails:
                    with save_lock:
                        self._save_batch_to_cache(emails)
                        fetched_count += len(emails)

                if batch_failed:
                    with save_lock:
                        failed_ids.extend(batch_failed)

                with progress_lock:
                    progress.update(len(chunk))

        progress.close()
        logger.info("Main pass complete: fetched %d, failed %d out of %d.",
                    fetched_count, len(failed_ids), total)

        # --- Retry pass: Sequential with rate limiting for failed messages ---
        if failed_ids:
            logger.info("Retrying %d failed messages (sequential, rate-limited)...",
                        len(failed_ids))
            retry_progress = tqdm(total=len(failed_ids), desc="Retrying failed", unit="email")
            still_failed = []

            for i in range(0, len(failed_ids), batch_size):
                retry_batch_ids = failed_ids[i:i + batch_size]
                try:
                    emails, batch_failed = _rate_limited_fetch(retry_batch_ids)
                except Exception as e:
                    logger.warning("Retry error: %s", e)
                    emails = []
                    batch_failed = list(retry_batch_ids)

                if emails:
                    self._save_batch_to_cache(emails)
                    fetched_count += len(emails)

                if batch_failed:
                    still_failed.extend(batch_failed)

                retry_progress.update(len(retry_batch_ids))
                time.sleep(3.0)  # Slower rate for retries

            retry_progress.close()

            # --- Individual fetch for any remaining failures ---
            if still_failed:
                logger.info("Fetching %d remaining messages individually...", len(still_failed))
                ind_progress = tqdm(total=len(still_failed), desc="Individual fetch",
                                   unit="email")
                for msg_id in still_failed:
                    try:
                        result = self._api_call_with_retry(
                            self.service.users().messages().get(
                                userId="me", id=msg_id, format="metadata",
                                metadataHeaders=["Message-ID", "Date", "From", "To", "Subject"],
                            )
                        )
                        email_dict = self._process_msg(result, labels_map)
                        self._save_batch_to_cache([email_dict])
                        fetched_count += 1
                    except Exception as e:
                        logger.error("Permanently failed to fetch message %s: %s", msg_id, e)
                    ind_progress.update(1)
                    time.sleep(1.0)
                ind_progress.close()

        logger.info("Total fetched and cached: %d out of %d requested.", fetched_count, total)

    @staticmethod
    def _extract_email(addr_string: str) -> str:
        """Extract just the email address from 'Name <email>' format."""
        if "<" in addr_string and ">" in addr_string:
            return addr_string.split("<")[1].split(">")[0].strip()
        return addr_string.strip()

    def _save_to_cache(self, emails: list[dict]):
        """Persist fetched emails into SQLite cache (INSERT OR REPLACE, no delete)."""
        if not emails:
            return
        conn = sqlite3.connect(self.db_path)
        now = datetime.utcnow().isoformat()
        conn.executemany(
            """INSERT OR REPLACE INTO gmail_emails
               (gmail_id, message_id, date, from_addr, to_addr, subject, size, labels, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    e.get("gmail_id", ""), e["message_id"], e["date"],
                    e["from_addr"], e["to_addr"], e["subject"], e["size"],
                    e["labels"], now,
                )
                for e in emails
            ],
        )
        conn.execute(
            "INSERT OR REPLACE INTO gmail_meta (key, value) VALUES (?, ?)",
            ("last_fetch", datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()
        logger.info("Cached %d Gmail emails to database.", len(emails))

    def _load_from_cache(self) -> list[dict] | None:
        """Load emails from SQLite cache if available."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT COUNT(*) FROM gmail_emails")
        count = cursor.fetchone()[0]
        if count == 0:
            conn.close()
            return None
        cursor = conn.execute(
            "SELECT gmail_id, message_id, date, from_addr, to_addr, subject, size, labels FROM gmail_emails"
        )
        emails = [
            {
                "gmail_id": row[0],
                "message_id": row[1],
                "date": row[2],
                "from_addr": row[3],
                "to_addr": row[4],
                "subject": row[5],
                "size": row[6],
                "labels": row[7],
            }
            for row in cursor.fetchall()
        ]
        conn.close()
        return emails

    def add_to_cache(self, email_record: dict):
        """Add a single email to the Gmail cache (used after migration import)."""
        conn = sqlite3.connect(self.db_path)
        gmail_id = email_record.get("gmail_id", email_record.get("message_id", ""))
        conn.execute(
            """INSERT OR REPLACE INTO gmail_emails
               (gmail_id, message_id, date, from_addr, to_addr, subject, size, labels, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                gmail_id,
                email_record.get("message_id", ""),
                email_record.get("date", ""),
                email_record.get("from_addr", ""),
                email_record.get("to_addr", ""),
                email_record.get("subject", ""),
                email_record.get("size", 0),
                email_record.get("labels", ""),
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        conn.close()

    def get_cached_message_ids(self) -> set[str]:
        """Return all message_ids currently in the Gmail cache."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT DISTINCT message_id FROM gmail_emails WHERE message_id != ''")
        ids = {row[0] for row in cursor.fetchall()}
        conn.close()
        return ids

    def clear_cache(self):
        """Remove all cached Gmail data."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM gmail_emails")
        conn.execute("DELETE FROM gmail_meta")
        conn.commit()
        conn.close()
        logger.info("Gmail cache cleared.")
