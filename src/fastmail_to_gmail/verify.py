#!/usr/bin/env python3
"""
Email Migration Verification Script

Compares emails between a Fastmail account (source) and a Google Workspace
account (destination) to verify that all emails were successfully migrated.

This script is READ-ONLY — it does not modify, move, or delete any emails.
"""

import argparse
import logging
import os
import sys

from fastmail_to_gmail.fastmail_client import FastmailClient
from fastmail_to_gmail.gmail_client import GmailClient
from fastmail_to_gmail.comparator import compare_emails
from fastmail_to_gmail.report_generator import print_summary, write_csv_reports
from fastmail_to_gmail.profile import Profile


def _make_log_path(prefix: str, log_dir: str) -> str:
    """Return a timestamped log path inside log_dir, e.g. profiles/rex/logs/verify_20260307_121500.log"""
    from datetime import datetime
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(log_dir, f"{prefix}_{timestamp}.log")


def setup_logging(verbose: bool = False, log_dir: str = "logs"):
    """Configure logging to file and optionally to console."""
    log_file = _make_log_path("verify", log_dir)
    handlers = [
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
    if verbose:
        handlers.append(logging.StreamHandler(sys.stdout))

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=handlers,
    )
    return log_file


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Verify email migration from Fastmail to Google Workspace.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --dry-run
  %(prog)s --verbose
  %(prog)s --folder Inbox --date-from 2024-01-01
  %(prog)s --refresh --output-dir ./my-reports
        """,
    )
    parser.add_argument(
        "--profile",
        required=True,
        help="User profile name (e.g. rex)",
    )
    parser.add_argument(
        "--fastmail-token",
        default=None,
        help="Fastmail API token (overrides profile token file)",
    )
    parser.add_argument(
        "--save-token",
        action="store_true",
        help="Save the Fastmail token to .fastmail_token for reuse across runs",
    )
    parser.add_argument(
        "--google-creds",
        default=None,
        help="Path to Google OAuth credentials.json (default: profile credentials dir)",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Force re-fetch all data, ignoring local cache",
    )
    parser.add_argument(
        "--refresh-fastmail",
        action="store_true",
        help="Force re-fetch Fastmail data only (keep Gmail cache)",
    )
    parser.add_argument(
        "--refresh-gmail",
        action="store_true",
        help="Force re-fetch Gmail data only (keep Fastmail cache)",
    )
    parser.add_argument(
        "--folder",
        default=None,
        help="Only compare a specific folder/label",
    )
    parser.add_argument(
        "--date-from",
        default=None,
        metavar="YYYY-MM-DD",
        help="Only compare emails from this date onward",
    )
    parser.add_argument(
        "--date-to",
        default=None,
        metavar="YYYY-MM-DD",
        help="Only compare emails up to this date",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable detailed logging to console",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for CSV reports (default: profile reports dir)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Test authentication to both services without fetching emails",
    )
    return parser.parse_args()


def get_fastmail_token(args: argparse.Namespace, token_file: str) -> str:
    """Resolve the Fastmail API token from CLI flag or profile token file."""
    token = args.fastmail_token

    # Profile-specific token file
    if not token and os.path.exists(token_file):
        token = open(token_file, encoding="utf-8").read().strip()
        if token:
            logging.getLogger(__name__).info("Loaded Fastmail token from %s", token_file)

    if not token:
        print("ERROR: Fastmail API token not found.")
        print(f"  Expected token file: {token_file}")
        print("  Run fm-setup --profile <name> to configure your Fastmail token.")
        sys.exit(1)

    # Save if requested
    if getattr(args, "save_token", False) and token:
        with open(token_file, "w", encoding="utf-8") as f:
            f.write(token)
        print(f"  Fastmail token saved to {token_file}")

    return token


def run_dry_run(fm_client: FastmailClient, gm_client: GmailClient):
    """Test authentication to both services."""
    print("\n--- Dry Run: Testing Authentication ---\n")

    print("  Fastmail: ", end="", flush=True)
    if fm_client.test_connection():
        print("OK")
    else:
        print("FAILED (check logs for details)")

    print("  Gmail:    ", end="", flush=True)
    if gm_client.test_connection():
        print("OK")
    else:
        print("FAILED (check logs for details)")

    print("\n  Dry run complete. No emails were fetched.\n")


def main():
    args = parse_args()

    # Create profile and ensure directories exist
    profile = Profile(args.profile)
    profile.ensure_dirs()

    # Apply profile-based defaults for optional paths
    if not args.google_creds:
        args.google_creds = profile.google_credentials
    if not args.output_dir:
        args.output_dir = profile.reports_dir

    log_file = setup_logging(verbose=args.verbose, log_dir=profile.logs_dir)
    logger = logging.getLogger(__name__)

    logger.info("Starting email migration verification (profile=%s).", profile.name)
    print("\nEmail Migration Verification")
    print("Fastmail -> Google Workspace\n")

    # Resolve credentials
    fastmail_token = get_fastmail_token(args, token_file=profile.fastmail_token_file)

    # Initialize clients
    fm_client = FastmailClient(api_token=fastmail_token, db_path=profile.migration_cache_db)
    gm_client = GmailClient(
        credentials_path=args.google_creds,
        token_path=profile.gmail_token,
        db_path=profile.migration_cache_db,
    )

    # Dry run mode
    if args.dry_run:
        run_dry_run(fm_client, gm_client)
        return

    # Determine which caches to refresh
    refresh_fastmail = args.refresh or args.refresh_fastmail
    refresh_gmail = args.refresh or args.refresh_gmail

    if refresh_fastmail:
        logger.info("Clearing Fastmail cache...")
        print("  Clearing Fastmail cached data...")
        fm_client.clear_cache()
    if refresh_gmail:
        logger.info("Refreshing Gmail data (incremental fetch)...")
        print("  Refreshing Gmail data (incremental - will resume from cache)...")

    # Fetch from Fastmail
    try:
        print("  Fetching emails from Fastmail...")
        fm_emails = fm_client.fetch_emails(
            folder=args.folder,
            date_from=args.date_from,
            date_to=args.date_to,
            use_cache=not refresh_fastmail,
        )
    except Exception as e:
        logger.error("Failed to fetch Fastmail emails: %s", e)
        print(f"\nERROR: Failed to fetch Fastmail emails: {e}")
        print(f"  Check {log_file} for details.")
        sys.exit(1)

    # Fetch from Gmail
    try:
        print("  Fetching emails from Gmail...")
        gm_emails = gm_client.fetch_emails(
            folder=args.folder,
            date_from=args.date_from,
            date_to=args.date_to,
            use_cache=not refresh_gmail,
        )
    except Exception as e:
        logger.error("Failed to fetch Gmail emails: %s", e)
        print(f"\nERROR: Failed to fetch Gmail emails: {e}")
        print(f"  Check {log_file} for details.")
        sys.exit(1)

    # Compare
    print("  Comparing emails...")
    result = compare_emails(fm_emails, gm_emails)

    # Output
    print_summary(result)
    write_csv_reports(result, output_dir=args.output_dir)

    logger.info("Verification complete. Match rate: %.2f%%", result.match_percentage)
    print(f"\n  Log file: {log_file}")
    print()


if __name__ == "__main__":
    main()
