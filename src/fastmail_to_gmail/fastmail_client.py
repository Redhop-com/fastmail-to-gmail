"""Fastmail JMAP API client for fetching email metadata."""

import logging
import time
import random
import sqlite3
from datetime import datetime

import requests
from dateutil import parser as dateutil_parser
from tqdm import tqdm

logger = logging.getLogger(__name__)

JMAP_SESSION_URL = "https://api.fastmail.com/jmap/session"


class FastmailClient:
    """Read-only Fastmail client using the JMAP protocol."""

    def __init__(self, api_token: str, db_path: str = "migration_cache.db"):
        self.api_token = api_token
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        })
        self._api_url = None
        self._account_id = None
        self._init_db()

    def _init_db(self):
        """Initialize the SQLite cache table for Fastmail emails."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fastmail_emails (
                message_id TEXT,
                date TEXT,
                from_addr TEXT,
                to_addr TEXT,
                subject TEXT,
                size INTEGER,
                folder TEXT,
                fetched_at TEXT,
                PRIMARY KEY (message_id, folder)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fastmail_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        conn.commit()
        conn.close()

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make an HTTP request with exponential backoff on rate limits / errors."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                resp = self.session.request(method, url, timeout=60, **kwargs)
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 2 ** (attempt + 1)))
                    jitter = random.uniform(0, 1)
                    wait = retry_after + jitter
                    logger.warning("Rate limited by Fastmail. Waiting %.1fs (attempt %d/%d)", wait, attempt + 1, max_retries)
                    time.sleep(wait)
                    continue
                if resp.status_code >= 500:
                    wait = (2 ** (attempt + 1)) + random.uniform(0, 1)
                    logger.warning("Fastmail server error %d. Retrying in %.1fs (attempt %d/%d)", resp.status_code, wait, attempt + 1, max_retries)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except requests.exceptions.Timeout:
                wait = (2 ** (attempt + 1)) + random.uniform(0, 1)
                logger.warning("Fastmail request timed out. Retrying in %.1fs (attempt %d/%d)", wait, attempt + 1, max_retries)
                time.sleep(wait)
            except requests.exceptions.ConnectionError:
                wait = (2 ** (attempt + 1)) + random.uniform(0, 1)
                logger.warning("Fastmail connection error. Retrying in %.1fs (attempt %d/%d)", wait, attempt + 1, max_retries)
                time.sleep(wait)
        raise RuntimeError(f"Fastmail API request failed after {max_retries} attempts: {method} {url}")

    def connect(self):
        """Establish a JMAP session and discover the API URL and account ID."""
        logger.info("Connecting to Fastmail JMAP session...")
        try:
            resp = self._request_with_retry("GET", JMAP_SESSION_URL)
        except Exception as e:
            raise RuntimeError(
                "Failed to connect to Fastmail. Check your API token.\n"
                "  1. Go to Fastmail → Settings → Privacy & Security → API Tokens\n"
                "  2. Generate a token with Mail read-only access\n"
                "  3. Run fm-setup --profile <name> to configure your token\n"
                f"  Error: {e}"
            ) from e

        session_data = resp.json()
        self._api_url = session_data.get("apiUrl")
        accounts = session_data.get("accounts", {})
        if not accounts:
            raise RuntimeError("No accounts found in Fastmail JMAP session.")

        # Pick the account that has mail capability — the session may return
        # multiple accounts (e.g. a contacts-only account and a mail account).
        self._account_id = None
        for acct_id, acct_data in accounts.items():
            caps = acct_data.get("accountCapabilities", {})
            if "urn:ietf:params:jmap:mail" in caps:
                self._account_id = acct_id
                logger.info("Found mail-capable account: %s", acct_id)
                break

        if self._account_id is None:
            # Fallback: no account has mail capability
            self._account_id = next(iter(accounts))
            logger.warning("No account with mail capability found. Falling back to: %s", self._account_id)

        logger.info("Connected to Fastmail. Account ID: %s", self._account_id)

    def test_connection(self) -> bool:
        """Test authentication without fetching emails."""
        try:
            self.connect()
            return True
        except Exception as e:
            logger.error("Fastmail connection test failed: %s", e)
            return False

    def _jmap_call(self, method_calls: list) -> dict:
        """Execute a JMAP method call."""
        payload = {
            "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
            "methodCalls": method_calls,
        }
        resp = self._request_with_retry("POST", self._api_url, json=payload)
        return resp.json()

    def get_mailboxes(self) -> dict:
        """Fetch all mailboxes/folders. Returns {mailbox_id: full_path}.

        Builds full folder paths by following parentId chains, using '/' as
        the separator (matches Gmail's nested label format).
        e.g. Investments/Greenhills/Alphington
        """
        result = self._jmap_call([
            ["Mailbox/get", {"accountId": self._account_id}, "0"]
        ])
        raw = {}
        for item in result["methodResponses"][0][1].get("list", []):
            raw[item["id"]] = {"name": item["name"], "parentId": item.get("parentId")}

        def _full_path(mid):
            parts = []
            seen = set()
            while mid and mid in raw:
                if mid in seen:
                    logger.warning("Circular parentId detected for mailbox %s", mid)
                    break
                seen.add(mid)
                parts.append(raw[mid]["name"])
                mid = raw[mid]["parentId"]
            return "/".join(reversed(parts))

        mailboxes = {mid: _full_path(mid) for mid in raw}
        logger.info("Found %d Fastmail mailboxes.", len(mailboxes))
        return mailboxes

    def _build_filter(self, folder: str | None, date_from: str | None, date_to: str | None, mailboxes: dict) -> dict | None:
        """Build a JMAP filter object based on CLI options."""
        conditions = []
        if folder:
            mailbox_id = None
            folder_lower = folder.lower()
            for mid, name in mailboxes.items():
                # Match against full path or leaf name
                leaf = name.rsplit("/", 1)[-1]
                if name.lower() == folder_lower or leaf.lower() == folder_lower:
                    mailbox_id = mid
                    break
            if mailbox_id:
                conditions.append({"inMailbox": mailbox_id})
            else:
                logger.warning("Folder '%s' not found in Fastmail. Fetching all.", folder)
        if date_from:
            conditions.append({"after": f"{date_from}T00:00:00Z"})
        if date_to:
            conditions.append({"before": f"{date_to}T23:59:59Z"})

        if not conditions:
            return None
        if len(conditions) == 1:
            return conditions[0]
        return {"operator": "AND", "conditions": conditions}

    def fetch_emails(self, folder: str | None = None, date_from: str | None = None,
                     date_to: str | None = None, use_cache: bool = True) -> list[dict]:
        """Fetch all email metadata from Fastmail.

        Returns a list of dicts with keys:
            message_id, date, from_addr, to_addr, subject, size, folder
        """
        if self._api_url is None:
            self.connect()

        # Check cache
        if use_cache:
            cached = self._load_from_cache()
            if cached:
                logger.info("Loaded %d Fastmail emails from cache.", len(cached))
                return cached

        mailboxes = self.get_mailboxes()
        email_filter = self._build_filter(folder, date_from, date_to, mailboxes)

        # First, get total count with Email/query
        query_args = {
            "accountId": self._account_id,
            "limit": 0,
            "calculateTotal": True,
        }
        if email_filter:
            query_args["filter"] = email_filter

        count_result = self._jmap_call([
            ["Email/query", query_args, "0"]
        ])
        total = count_result["methodResponses"][0][1].get("total", 0)
        logger.info("Fastmail reports %d total emails to fetch.", total)

        if total == 0:
            return []

        # Paginate through all email IDs, then fetch metadata in batches
        all_emails = []
        position = 0
        batch_size = 50  # JMAP typical max
        progress = tqdm(total=total, desc="Fetching Fastmail emails", unit="email")

        while position < total:
            query_args = {
                "accountId": self._account_id,
                "position": position,
                "limit": batch_size,
                "calculateTotal": True,
            }
            if email_filter:
                query_args["filter"] = email_filter

            # Query IDs + get properties in one round-trip
            result = self._jmap_call([
                ["Email/query", query_args, "q"],
                ["Email/get", {
                    "accountId": self._account_id,
                    "#ids": {
                        "resultOf": "q",
                        "name": "Email/query",
                        "path": "/ids",
                    },
                    "properties": [
                        "messageId", "sentAt", "receivedAt", "from", "to",
                        "subject", "size", "mailboxIds",
                    ],
                }, "g"],
            ])

            # Parse query response
            query_resp = result["methodResponses"][0][1]
            ids = query_resp.get("ids", [])
            if not ids:
                break

            # Parse get response
            get_resp = result["methodResponses"][1][1]
            email_list = get_resp.get("list", [])

            for email in email_list:
                # Determine folder names from mailbox IDs
                mbox_ids = email.get("mailboxIds", {})
                folders = [mailboxes.get(mid, mid) for mid in mbox_ids if mbox_ids[mid]]

                # Extract from/to addresses
                from_list = email.get("from") or []
                from_addr = from_list[0].get("email", "") if from_list else ""
                to_list = email.get("to") or []
                to_addr = ", ".join(t.get("email", "") for t in to_list)

                # Message-ID: JMAP returns a list
                msg_id_list = email.get("messageId") or []
                msg_id = msg_id_list[0] if msg_id_list else ""

                date_str = email.get("sentAt") or email.get("receivedAt") or ""

                for folder_name in (folders or [""]):
                    all_emails.append({
                        "message_id": msg_id,
                        "date": date_str,
                        "from_addr": from_addr,
                        "to_addr": to_addr,
                        "subject": email.get("subject", ""),
                        "size": email.get("size", 0),
                        "folder": folder_name,
                    })

            position += len(ids)
            progress.update(len(ids))

        progress.close()
        logger.info("Fetched %d Fastmail email records.", len(all_emails))

        # Save to cache
        self._save_to_cache(all_emails)
        return all_emails

    def _save_to_cache(self, emails: list[dict]):
        """Persist fetched emails into SQLite cache."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM fastmail_emails")
        conn.executemany(
            """INSERT OR REPLACE INTO fastmail_emails
               (message_id, date, from_addr, to_addr, subject, size, folder, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                (
                    e["message_id"], e["date"], e["from_addr"], e["to_addr"],
                    e["subject"], e["size"], e["folder"],
                    datetime.utcnow().isoformat(),
                )
                for e in emails
            ],
        )
        conn.execute(
            "INSERT OR REPLACE INTO fastmail_meta (key, value) VALUES (?, ?)",
            ("last_fetch", datetime.utcnow().isoformat()),
        )
        conn.commit()
        conn.close()
        logger.info("Cached %d Fastmail emails to database.", len(emails))

    def _load_from_cache(self) -> list[dict] | None:
        """Load emails from SQLite cache if available."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT COUNT(*) FROM fastmail_emails")
        count = cursor.fetchone()[0]
        if count == 0:
            conn.close()
            return None
        cursor = conn.execute(
            "SELECT message_id, date, from_addr, to_addr, subject, size, folder FROM fastmail_emails"
        )
        emails = [
            {
                "message_id": row[0],
                "date": row[1],
                "from_addr": row[2],
                "to_addr": row[3],
                "subject": row[4],
                "size": row[5],
                "folder": row[6],
            }
            for row in cursor.fetchall()
        ]
        conn.close()
        return emails

    def clear_cache(self):
        """Remove all cached Fastmail data."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM fastmail_emails")
        conn.execute("DELETE FROM fastmail_meta")
        conn.commit()
        conn.close()
        logger.info("Fastmail cache cleared.")
