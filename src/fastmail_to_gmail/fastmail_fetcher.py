"""Fetch raw RFC 2822 email content from Fastmail via JMAP."""

import http.client
import logging
import random
import time

import requests
from urllib3.exceptions import IncompleteRead as Urllib3IncompleteRead

logger = logging.getLogger(__name__)

JMAP_SESSION_URL = "https://api.fastmail.com/jmap/session"

# Gmail API import endpoint supports up to 50 MB via resumable upload.
MAX_RAW_SIZE = 50 * 1024 * 1024


class FastmailFetcher:
    """Fetch full raw email (.eml) content from Fastmail using JMAP."""

    def __init__(self, api_token: str):
        self.api_token = api_token
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json",
        })
        self._api_url: str | None = None
        self._account_id: str | None = None
        self._download_url_template: str | None = None

    def connect(self):
        """Establish JMAP session."""
        logger.info("Connecting to Fastmail JMAP session...")
        resp = self._request_with_retry("GET", JMAP_SESSION_URL)
        session_data = resp.json()
        self._api_url = session_data.get("apiUrl")
        self._download_url_template = session_data.get("downloadUrl")

        accounts = session_data.get("accounts", {})
        if not accounts:
            raise RuntimeError("No accounts found in Fastmail JMAP session.")

        # Pick the account with mail capability
        self._account_id = None
        for acct_id, acct_data in accounts.items():
            caps = acct_data.get("accountCapabilities", {})
            if "urn:ietf:params:jmap:mail" in caps:
                self._account_id = acct_id
                break

        if self._account_id is None:
            self._account_id = next(iter(accounts))
            logger.warning("No mail-capable account found, falling back to %s", self._account_id)

        logger.info("Connected to Fastmail. Account: %s", self._account_id)

    def test_connection(self) -> bool:
        try:
            self.connect()
            return True
        except Exception as e:
            logger.error("Fastmail connection test failed: %s", e)
            return False

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """HTTP request with exponential backoff."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                resp = self.session.request(method, url, timeout=120, **kwargs)
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 2 ** (attempt + 1)))
                    wait = retry_after + random.uniform(0, 1)
                    logger.warning("Fastmail rate limited. Waiting %.1fs", wait)
                    time.sleep(wait)
                    continue
                if resp.status_code >= 500:
                    wait = (2 ** (attempt + 1)) + random.uniform(0, 1)
                    logger.warning("Fastmail server error %d. Retrying in %.1fs", resp.status_code, wait)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except (requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
                    http.client.IncompleteRead,
                    Urllib3IncompleteRead) as e:
                wait = (2 ** (attempt + 1)) + random.uniform(0, 1)
                logger.warning("Fastmail network error: %s. Retrying in %.1fs", e, wait)
                time.sleep(wait)
        raise RuntimeError(f"Fastmail request failed after {max_retries} attempts: {method} {url}")

    def _jmap_call(self, method_calls: list) -> dict:
        payload = {
            "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
            "methodCalls": method_calls,
        }
        resp = self._request_with_retry("POST", self._api_url, json=payload)
        return resp.json()

    def find_email_by_message_id(self, message_id: str) -> dict | None:
        """Find a Fastmail email by its Message-ID header and return its metadata.

        Returns dict with keys: id, blobId, size, mailboxIds, receivedAt
        or None if not found.
        """
        if self._api_url is None:
            self.connect()

        # Search by Message-ID header
        result = self._jmap_call([
            ["Email/query", {
                "accountId": self._account_id,
                "filter": {"header": ["Message-ID", f"<{message_id}>"]},
                "limit": 5,
            }, "q"],
            ["Email/get", {
                "accountId": self._account_id,
                "#ids": {"resultOf": "q", "name": "Email/query", "path": "/ids"},
                "properties": ["id", "blobId", "size", "mailboxIds", "receivedAt", "messageId"],
            }, "g"],
        ])

        emails = result["methodResponses"][1][1].get("list", [])
        if not emails:
            # Try without angle brackets
            result = self._jmap_call([
                ["Email/query", {
                    "accountId": self._account_id,
                    "filter": {"header": ["Message-ID", message_id]},
                    "limit": 5,
                }, "q"],
                ["Email/get", {
                    "accountId": self._account_id,
                    "#ids": {"resultOf": "q", "name": "Email/query", "path": "/ids"},
                    "properties": ["id", "blobId", "size", "mailboxIds", "receivedAt", "messageId"],
                }, "g"],
            ])
            emails = result["methodResponses"][1][1].get("list", [])

        if not emails:
            return None

        email = emails[0]
        return {
            "id": email["id"],
            "blobId": email["blobId"],
            "size": email.get("size", 0),
            "mailboxIds": email.get("mailboxIds", {}),
            "receivedAt": email.get("receivedAt", ""),
        }

    def find_email_by_composite(self, date: str, from_addr: str, subject: str) -> dict | None:
        """Fallback: find email by date + from + subject."""
        if self._api_url is None:
            self.connect()

        conditions = []
        if from_addr:
            conditions.append({"from": from_addr})
        if subject:
            conditions.append({"subject": subject})

        if not conditions:
            return None

        email_filter = {"operator": "AND", "conditions": conditions} if len(conditions) > 1 else conditions[0]

        result = self._jmap_call([
            ["Email/query", {
                "accountId": self._account_id,
                "filter": email_filter,
                "limit": 10,
            }, "q"],
            ["Email/get", {
                "accountId": self._account_id,
                "#ids": {"resultOf": "q", "name": "Email/query", "path": "/ids"},
                "properties": ["id", "blobId", "size", "mailboxIds", "receivedAt", "messageId", "subject"],
            }, "g"],
        ])

        emails = result["methodResponses"][1][1].get("list", [])
        if not emails:
            return None

        # Pick best match by subject similarity
        for email in emails:
            if email.get("subject", "").strip().lower() == subject.strip().lower():
                return {
                    "id": email["id"],
                    "blobId": email["blobId"],
                    "size": email.get("size", 0),
                    "mailboxIds": email.get("mailboxIds", {}),
                    "receivedAt": email.get("receivedAt", ""),
                }

        # Return first result as best guess
        email = emails[0]
        return {
            "id": email["id"],
            "blobId": email["blobId"],
            "size": email.get("size", 0),
            "mailboxIds": email.get("mailboxIds", {}),
            "receivedAt": email.get("receivedAt", ""),
        }

    def download_raw_email(self, blob_id: str) -> bytes:
        """Download the full RFC 2822 raw email content by blob ID.

        Uses streaming to handle large emails and retries on incomplete reads.
        """
        if self._api_url is None:
            self.connect()

        # Build download URL from template
        download_url = self._download_url_template.replace("{accountId}", self._account_id)
        download_url = download_url.replace("{blobId}", blob_id)
        download_url = download_url.replace("{name}", "email.eml")
        download_url = download_url.replace("{type}", "application/octet-stream")

        max_retries = 3
        for attempt in range(max_retries):
            try:
                resp = self.session.get(download_url, timeout=120, stream=True)
                resp.raise_for_status()
                # Stream in chunks to avoid IncompleteRead on large emails
                chunks = []
                for chunk in resp.iter_content(chunk_size=64 * 1024):
                    chunks.append(chunk)
                return b"".join(chunks)
            except (requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    http.client.IncompleteRead,
                    Urllib3IncompleteRead) as e:
                if attempt < max_retries - 1:
                    wait = (2 ** (attempt + 1)) + random.uniform(0, 1)
                    logger.warning("Blob download failed (%s). Retrying in %.1fs (attempt %d/%d)",
                                   e, wait, attempt + 1, max_retries)
                    time.sleep(wait)
                else:
                    raise

    def is_oversized(self, size: int) -> bool:
        """Check if an email is too large for Gmail import."""
        return size > MAX_RAW_SIZE
