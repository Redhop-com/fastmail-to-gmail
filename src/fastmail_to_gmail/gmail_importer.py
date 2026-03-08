"""Upload emails to Gmail via the import API."""

import base64
import logging
import os
import random
import time

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import io

from googleapiclient.http import MediaInMemoryUpload, MediaIoBaseUpload

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/gmail.insert",
    "https://www.googleapis.com/auth/gmail.labels",
    "https://www.googleapis.com/auth/gmail.readonly",
]

# Throttle to ~10 imports/second to stay under Gmail's 25/sec quota
MIN_IMPORT_INTERVAL = 0.1  # seconds between imports


class GmailImporter:
    """Upload raw emails to Gmail using the import API."""

    def __init__(self, credentials_path: str = "credentials.json",
                 token_path: str = "token_import.json",
                 force_headless: bool = False):
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.force_headless = force_headless
        self.service = None
        self._last_import_time = 0.0

    def _authenticate(self) -> Credentials:
        """OAuth2 authentication, reusing existing token if possible."""
        creds = None
        if os.path.exists(self.token_path):
            creds = Credentials.from_authorized_user_file(self.token_path, SCOPES)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logger.info("Refreshing Gmail token...")
                creds.refresh(Request())
            else:
                if not os.path.exists(self.credentials_path):
                    raise RuntimeError(
                        f"Google credentials not found at '{self.credentials_path}'.\n"
                        "  Download OAuth 2.0 credentials from Google Cloud Console."
                    )
                from fastmail_to_gmail.auth import run_oauth_flow
                creds = run_oauth_flow(self.credentials_path, SCOPES,
                                       force_headless=self.force_headless)

            with open(self.token_path, "w") as f:
                f.write(creds.to_json())

        return creds

    def connect(self):
        """Connect to the Gmail API."""
        logger.info("Connecting to Gmail API (import mode)...")
        creds = self._authenticate()
        self.service = build("gmail", "v1", credentials=creds)
        profile = self.service.users().getProfile(userId="me").execute()
        logger.info("Connected to Gmail: %s", profile.get("emailAddress"))

    def test_connection(self) -> bool:
        try:
            self.connect()
            return True
        except Exception as e:
            logger.error("Gmail connection test failed: %s", e)
            return False

    def _api_call_with_retry(self, request, max_retries: int = 3):
        """Execute a Gmail API request with retry and backoff."""
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
                    logger.warning("Gmail request error. Retrying in %.1fs", wait)
                    time.sleep(wait)
                else:
                    raise
        raise RuntimeError(f"Gmail API request failed after {max_retries} attempts")

    def check_email_exists(self, message_id: str) -> bool:
        """Check if an email with this Message-ID already exists in Gmail."""
        if not message_id:
            return False

        try:
            query = f"rfc822msgid:{message_id}"
            result = self._api_call_with_retry(
                self.service.users().messages().list(userId="me", q=query, maxResults=1)
            )
            messages = result.get("messages", [])
            if messages:
                logger.debug("Email with Message-ID %s already exists in Gmail.", message_id)
                return True
            return False
        except Exception as e:
            logger.warning("Error checking for existing email %s: %s", message_id, e)
            return False

    def import_email(self, raw_email: bytes, label_ids: list[str] | None = None) -> dict:
        """Import a raw RFC 2822 email into Gmail.

        Uses users.messages.import() which preserves original dates and headers.

        Returns the Gmail API response with the new message ID.
        """
        if self.service is None:
            self.connect()

        # Throttle
        now = time.time()
        elapsed = now - self._last_import_time
        if elapsed < MIN_IMPORT_INTERVAL:
            time.sleep(MIN_IMPORT_INTERVAL - elapsed)

        # Use MediaIoBaseUpload for all emails — MediaInMemoryUpload has an
        # ASCII encoding bug with non-ASCII UTF-8 content in raw emails.
        media = MediaIoBaseUpload(
            io.BytesIO(raw_email), mimetype="message/rfc822", resumable=True
        )

        request = self.service.users().messages().import_(
            userId="me",
            body={
                "labelIds": label_ids or [],
            },
            media_body=media,
            neverMarkSpam=True,
            processForCalendar=False,
            internalDateSource="dateHeader",
        )

        result = self._api_call_with_retry(request)
        self._last_import_time = time.time()

        gmail_id = result.get("id", "")
        logger.debug("Imported email to Gmail. Gmail ID: %s", gmail_id)
        return result

    def import_email_dry_run(self, raw_email_size: int, label_ids_desc: str,
                             message_id: str, subject: str) -> str:
        """Simulate an import without actually uploading."""
        return (
            f"[DRY RUN] Would import: Message-ID={message_id}, "
            f"Subject='{subject[:60]}', Size={raw_email_size:,} bytes, "
            f"Labels={label_ids_desc}"
        )
