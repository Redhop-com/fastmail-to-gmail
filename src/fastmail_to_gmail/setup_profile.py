#!/usr/bin/env python3
"""Interactive profile setup wizard.

Sets up a new migration profile by:
1. Creating profile directories
2. Checking/prompting for Google OAuth credentials
3. Collecting and saving the Fastmail API token
4. Triggering Gmail OAuth consent for readonly access
5. Triggering Gmail OAuth consent for import access
6. Testing all connections

Usage:
    uv run fm-setup --profile rex
"""

import argparse
import os
import sys

from fastmail_to_gmail.profile import Profile


def _step(num: int, title: str):
    print(f"\n  [{num}/5] {title}")
    print(f"  {'-' * 50}")


def _check_google_credentials(profile: Profile) -> bool:
    """Check for Google OAuth credentials.json."""
    _step(1, "Google OAuth Credentials")

    if os.path.exists(profile.google_credentials):
        print(f"  Found: {profile.google_credentials}")
        return True

    print(f"  credentials.json not found at: {profile.google_credentials}")
    print()
    print("  To create one:")
    print("    1. Go to https://console.cloud.google.com/")
    print("    2. Create a project (or select existing)")
    print("    3. Enable the Gmail API under APIs & Services")
    print("    4. Go to Credentials -> Create Credentials -> OAuth 2.0 Client ID")
    print("    5. Select 'Desktop application' as application type")
    print(f"    6. Download the JSON and save as:")
    print(f"       {profile.google_credentials}")
    print()

    input("  Press Enter after placing credentials.json... ")

    if os.path.exists(profile.google_credentials):
        print("  Found credentials.json!")
        return True

    print("  ERROR: credentials.json still not found.")
    return False


def _check_fastmail_token(profile: Profile) -> bool:
    """Prompt for Fastmail API token."""
    _step(2, "Fastmail API Token")

    if os.path.exists(profile.fastmail_token_file):
        token = open(profile.fastmail_token_file, encoding="utf-8").read().strip()
        if token:
            print(f"  Found: {profile.fastmail_token_file}")
            print(f"  Token: {token[:8]}...{token[-4:]}")
            return True

    print("  No Fastmail token found.")
    print()
    print("  To generate one:")
    print("    1. Go to Fastmail -> Settings -> Privacy & Security -> API Tokens")
    print("    2. Create a token with 'Mail read-only' access")
    print()

    token = input("  Paste your Fastmail API token: ").strip()
    if not token:
        print("  ERROR: No token provided.")
        return False

    with open(profile.fastmail_token_file, "w", encoding="utf-8") as f:
        f.write(token)
    print(f"  Saved to: {profile.fastmail_token_file}")
    return True


def _check_gmail_readonly(profile: Profile, *, force_headless: bool = False) -> bool:
    """Trigger Gmail readonly OAuth consent if needed."""
    _step(3, "Gmail Readonly Access")

    from fastmail_to_gmail.gmail_client import GmailClient

    try:
        client = GmailClient(
            credentials_path=profile.google_credentials,
            token_path=profile.gmail_token,
            db_path=profile.migration_cache_db,
            force_headless=force_headless,
        )
        client.connect()
        print(f"  Connected to Gmail (readonly)")
        return True
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def _check_gmail_import(profile: Profile, *, force_headless: bool = False) -> bool:
    """Trigger Gmail import OAuth consent if needed."""
    _step(4, "Gmail Import Access")

    from fastmail_to_gmail.gmail_importer import GmailImporter

    try:
        importer = GmailImporter(
            credentials_path=profile.google_credentials,
            token_path=profile.gmail_import_token,
            force_headless=force_headless,
        )
        importer.connect()
        print(f"  Connected to Gmail (import)")
        return True
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def _test_fastmail(profile: Profile) -> bool:
    """Test Fastmail connection."""
    _step(5, "Test Fastmail Connection")

    token = open(profile.fastmail_token_file, encoding="utf-8").read().strip()

    from fastmail_to_gmail.fastmail_client import FastmailClient

    try:
        client = FastmailClient(api_token=token, db_path=profile.migration_cache_db)
        client.connect()
        print(f"  Connected to Fastmail")
        return True
    except Exception as e:
        print(f"  ERROR: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Set up a new migration profile",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--profile", required=True, help="Profile name (e.g. rex)")
    parser.add_argument("--headless", action="store_true",
                        help="Force headless OAuth (paste URL manually, for SSH sessions)")
    args = parser.parse_args()

    profile = Profile(args.profile)
    profile.ensure_dirs()

    print(f"\n  Setting up profile: {args.profile}")
    print(f"  Profile directory:  {profile.root}")

    results = []

    # Step 1: Google credentials
    results.append(("Google credentials", _check_google_credentials(profile)))
    if not results[-1][1]:
        print("\n  Setup cannot continue without Google credentials.")
        sys.exit(1)

    # Step 2: Fastmail token
    results.append(("Fastmail token", _check_fastmail_token(profile)))
    if not results[-1][1]:
        print("\n  Setup cannot continue without a Fastmail token.")
        sys.exit(1)

    # Step 3: Gmail readonly
    results.append(("Gmail readonly", _check_gmail_readonly(profile, force_headless=args.headless)))

    # Step 4: Gmail import
    results.append(("Gmail import", _check_gmail_import(profile, force_headless=args.headless)))

    # Step 5: Fastmail connection
    results.append(("Fastmail connection", _test_fastmail(profile)))

    # Summary
    print(f"\n  {'=' * 50}")
    print(f"  Profile Setup Summary: {args.profile}")
    print(f"  {'=' * 50}")
    all_ok = True
    for name, ok in results:
        status = "\033[32m PASS \033[0m" if ok else "\033[31m FAIL \033[0m"
        print(f"  [{status}] {name}")
        if not ok:
            all_ok = False

    if all_ok:
        print(f"\n  Profile '{args.profile}' is ready!")
        print(f"\n  Next steps:")
        print(f"    uv run fm-verify --profile {args.profile} --dry-run")
        print(f"    uv run fm-verify --profile {args.profile} --refresh")
        print(f"    uv run fm-migrate --profile {args.profile} --dry-run")
    else:
        print(f"\n  Some steps failed. Fix the issues and run setup again:")
        print(f"    uv run fm-setup --profile {args.profile}")
        sys.exit(1)

    print()


if __name__ == "__main__":
    main()
