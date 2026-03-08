"""Shared OAuth helper with headless server support.

On headless servers (no DISPLAY), falls back to a manual flow where the user
opens the auth URL in any browser, then pastes the redirect URL back into
the terminal.
"""

import logging
import os
import sys

from google_auth_oauthlib.flow import InstalledAppFlow

logger = logging.getLogger(__name__)


def _is_headless():
    """Detect if running on a headless server (no display)."""
    if sys.platform == "win32":
        return False
    if os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"):
        return False
    return True


def run_oauth_flow(credentials_path, scopes, *, force_headless=False):
    """Run OAuth flow, auto-detecting headless vs GUI environment.

    Args:
        credentials_path: Path to Google OAuth credentials.json
        scopes: List of OAuth scopes to request
        force_headless: If True, skip auto-detection and use manual flow

    Returns:
        google.oauth2.credentials.Credentials
    """
    flow = InstalledAppFlow.from_client_secrets_file(credentials_path, scopes)

    if force_headless or _is_headless():
        logger.info("Headless environment detected — using manual OAuth flow")
        return _run_manual_flow(flow)
    else:
        return flow.run_local_server(port=0)


def _run_manual_flow(flow):
    """Manual OAuth for headless servers — user pastes redirect URL.

    1. Prints the auth URL for the user to open in any browser
    2. Google redirects to http://localhost:1?code=... (browser shows error)
    3. User copies the full URL from the address bar and pastes it here
    4. We extract the code and exchange it for tokens
    """
    # Allow http://localhost redirect (oauthlib rejects non-HTTPS by default)
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    flow.redirect_uri = "http://localhost:1"

    auth_url, _ = flow.authorization_url(
        access_type="offline",
        prompt="consent",
    )

    print()
    print("  Open this URL in any browser:")
    print()
    print(f"  {auth_url}")
    print()
    print("  After authorizing, your browser will redirect to http://localhost:1")
    print("  It will show a connection error — that's expected.")
    print("  Copy the FULL URL from your browser's address bar and paste it here:")
    print()

    redirect_url = input("  Paste redirect URL: ").strip()

    if not redirect_url:
        raise RuntimeError("No redirect URL provided.")

    flow.fetch_token(authorization_response=redirect_url)
    logger.info("OAuth token obtained via manual flow")
    return flow.credentials
