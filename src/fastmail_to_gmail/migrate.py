#!/usr/bin/env python3
"""
Migrate Missing Emails from Fastmail to Google Workspace.

Companion to verify_email_migration.py. Reads missing_from_google.csv and
copies those emails from Fastmail into Gmail using the import API.

This is COPY-ONLY — Fastmail emails are never modified or deleted.
"""

import argparse
import csv
import logging
import os
import signal
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from dateutil import parser as dateutil_parser
from tqdm import tqdm

from fastmail_to_gmail.fastmail_fetcher import FastmailFetcher
from fastmail_to_gmail.gmail_client import GmailClient
from fastmail_to_gmail.gmail_importer import GmailImporter
from fastmail_to_gmail.label_mapper import LabelMapper
from fastmail_to_gmail.migration_tracker import (
    MigrationTracker, STATUS_PENDING, STATUS_FETCHED,
    STATUS_UPLOADED, STATUS_VERIFIED, STATUS_SKIPPED, STATUS_FAILED,
)
from fastmail_to_gmail.profile import Profile


def _make_log_path(log_dir: str, prefix: str, timestamp: str) -> str:
    """Return a timestamped log path inside log_dir, e.g. profiles/rex/logs/migration_20260307_121500.log"""
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, f"{prefix}_{timestamp}.log")


def setup_logging(log_dir: str, verbose: bool = False, timestamp: str = ""):
    log_file = _make_log_path(log_dir, "migration", timestamp)
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


logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate missing emails from Fastmail to Google Workspace.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Workflow:
  1. Run verify_email_migration.py first to identify missing emails.
  2. Review reports/missing_from_google.csv if desired.
  3. Run this script with --dry-run to preview.
  4. Run this script to execute migration.
  5. Run verify_email_migration.py --refresh to confirm.
        """,
    )
    parser.add_argument("--profile", required=True, help="User profile name (e.g. rex)")
    parser.add_argument(
        "--input",
        default=None,
        help="Path to missing_from_google.csv (default: profiles/<profile>/reports/missing_from_google.csv)",
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
        help="Path to Google credentials.json (default: profiles/<profile>/credentials/credentials.json)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only process the first N emails (useful for test runs, e.g. --limit 10)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Emails per batch (default: 50)",
    )
    parser.add_argument(
        "--folder",
        default=None,
        help="Only migrate emails from a specific Fastmail folder",
    )
    parser.add_argument(
        "--date-from",
        default=None,
        metavar="YYYY-MM-DD",
        help="Only migrate emails from this date onward",
    )
    parser.add_argument(
        "--date-to",
        default=None,
        metavar="YYYY-MM-DD",
        help="Only migrate emails up to this date",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview migration without uploading to Gmail",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip confirmation prompt and proceed with migration",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume a previously interrupted migration",
    )
    parser.add_argument(
        "--max-errors",
        type=int,
        default=20,
        help="Stop after N consecutive failures (default: 20)",
    )
    parser.add_argument(
        "--displayemail",
        action="store_true",
        help="Display subject and sender for each email as it is processed",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable detailed console logging",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Number of parallel upload workers (default: 5, use 1 for sequential)",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory for reports (default: profiles/<profile>/reports)",
    )
    return parser.parse_args()


def load_missing_csv(csv_path: str) -> list[dict]:
    """Load missing_from_google.csv into a list of dicts."""
    if not os.path.exists(csv_path):
        print(f"\nERROR: File not found: {csv_path}")
        print("  Run verify_email_migration.py first to generate this file.")
        print("  Example: python verify_email_migration.py --refresh")
        sys.exit(1)

    rows = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({
                "message_id": row.get("Message-ID", "").strip(),
                "date": row.get("Date", "").strip(),
                "from_addr": row.get("From", "").strip(),
                "to_addr": row.get("To", "").strip(),
                "subject": row.get("Subject", "").strip(),
                "fastmail_folder": row.get("Fastmail Folder", "").strip(),
            })

    if not rows:
        print("\nNo missing emails found in the CSV. Nothing to migrate!")
        sys.exit(0)

    return rows


def filter_emails(emails: list[dict], folder: str | None,
                  date_from: str | None, date_to: str | None) -> list[dict]:
    """Apply folder and date filters to the email list."""
    filtered = emails

    if folder:
        folder_lower = folder.lower()
        filtered = [
            e for e in filtered
            if folder_lower in [f.strip().lower() for f in e["fastmail_folder"].split(",")]
        ]

    if date_from:
        dt_from = dateutil_parser.parse(date_from)
        result = []
        for e in filtered:
            try:
                dt = dateutil_parser.parse(e["date"])
                if dt >= dt_from:
                    result.append(e)
            except (ValueError, TypeError):
                result.append(e)  # Include if date can't be parsed
        filtered = result

    if date_to:
        dt_to = dateutil_parser.parse(date_to)
        result = []
        for e in filtered:
            try:
                dt = dateutil_parser.parse(e["date"])
                if dt <= dt_to:
                    result.append(e)
            except (ValueError, TypeError):
                result.append(e)
        filtered = result

    return filtered


def print_plan(emails: list[dict], dry_run: bool):
    """Print the migration plan summary."""
    folder_counts = defaultdict(int)
    dates = []
    for e in emails:
        folder_counts[e["fastmail_folder"] or "(no folder)"] += 1
        try:
            dates.append(dateutil_parser.parse(e["date"]))
        except (ValueError, TypeError):
            pass

    mode = "[DRY RUN] " if dry_run else ""
    print(f"\n  {mode}Migration Plan")
    print(f"  {'=' * 50}")
    print(f"  Total emails to migrate: {len(emails):,}")

    if dates:
        print(f"  Date range: {min(dates).strftime('%Y-%m-%d')} to {max(dates).strftime('%Y-%m-%d')}")

    print(f"\n  By folder:")
    max_folder_len = max(len(f) for f in folder_counts) if folder_counts else 30
    for folder in sorted(folder_counts):
        print(f"    {folder:<{max_folder_len}s} {folder_counts[folder]:>8,}")

    print()


def confirm_migration(email_count: int) -> bool:
    """Ask for user confirmation. Returns True if confirmed."""
    try:
        answer = input(f"  Proceed with migrating {email_count:,} emails? [y/N] ").strip().lower()
        return answer in ("y", "yes")
    except (EOFError, KeyboardInterrupt):
        print()
        return False


def migrate_single_email(
    email_record: dict,
    fetcher: FastmailFetcher,
    importer: GmailImporter,
    label_mapper: LabelMapper,
    tracker: MigrationTracker,
    dry_run: bool,
    verification_cache: GmailClient | None = None,
    on_stage: "callable | None" = None,
) -> str:
    """Migrate a single email. Returns status string."""
    msg_id = email_record["message_id"]
    subject = email_record["subject"]
    folder = email_record["fastmail_folder"]

    def _notify(stage):
        if on_stage is not None:
            on_stage(stage, email_record)

    # Step 1: Check for duplicates in Gmail
    _notify("checking")
    if msg_id and not dry_run:
        if importer.check_email_exists(msg_id):
            tracker.update_status(msg_id or _composite_key(email_record), status=STATUS_SKIPPED)
            logger.info("Skipped (already in Gmail): %s — %s", msg_id, subject[:60])
            return STATUS_SKIPPED

    # Step 2: Find the email in Fastmail
    _notify("searching")
    fm_email = None
    if msg_id:
        fm_email = fetcher.find_email_by_message_id(msg_id)

    if fm_email is None:
        fm_email = fetcher.find_email_by_composite(
            email_record["date"], email_record["from_addr"], subject
        )

    if fm_email is None:
        error = "Email not found in Fastmail"
        tracker.update_status(msg_id or _composite_key(email_record),
                              status=STATUS_FAILED, error=error)
        logger.warning("Not found in Fastmail: %s — %s", msg_id, subject[:60])
        return STATUS_FAILED

    blob_id = fm_email["blobId"]
    size = fm_email.get("size", 0)

    # Step 3: Check size
    if fetcher.is_oversized(size):
        error = f"Email too large for Gmail: {size:,} bytes (max ~50MB)"
        tracker.update_status(msg_id or _composite_key(email_record),
                              status=STATUS_FAILED, error=error)
        logger.warning("Oversized: %s — %s (%d bytes)", msg_id, subject[:60], size)
        return STATUS_FAILED

    tracker.update_status(msg_id or _composite_key(email_record),
                          status=STATUS_FETCHED, fastmail_blob_id=blob_id, size=size)

    # Step 4: Dry run — stop here
    if dry_run:
        individual_folders = [f.strip() for f in folder.split(",") if f.strip()]
        label_descs = [label_mapper.map_folder_dry_run(f) for f in individual_folders]
        label_desc = ", ".join(label_descs)
        print(importer.import_email_dry_run(size, label_desc, msg_id, subject))
        tracker.update_status(msg_id or _composite_key(email_record), status=STATUS_SKIPPED)
        return STATUS_SKIPPED

    # Step 5: Download raw email from Fastmail
    _notify("downloading")
    try:
        raw_email = fetcher.download_raw_email(blob_id)
    except Exception as e:
        error = f"Failed to download from Fastmail: {e}"
        tracker.update_status(msg_id or _composite_key(email_record),
                              status=STATUS_FAILED, error=error)
        logger.error("Download failed: %s — %s: %s", msg_id, subject[:60], e)
        return STATUS_FAILED

    # Step 6: Map folder(s) to Gmail labels
    # The folder field may contain comma-separated folder names (e.g. "Inbox, Inbox C")
    # when an email exists in multiple Fastmail mailboxes. Split and map each individually.
    try:
        individual_folders = [f.strip() for f in folder.split(",") if f.strip()]
        label_ids = []
        seen_label_ids = set()
        for f in individual_folders:
            for lid in label_mapper.map_folder(f):
                if lid not in seen_label_ids:
                    label_ids.append(lid)
                    seen_label_ids.add(lid)
    except Exception as e:
        error = f"Label mapping failed: {e}"
        tracker.update_status(msg_id or _composite_key(email_record),
                              status=STATUS_FAILED, error=error)
        logger.error("Label error: %s — %s: %s", msg_id, subject[:60], e)
        return STATUS_FAILED

    # Step 7: Import to Gmail
    _notify("importing")
    try:
        result = importer.import_email(raw_email, label_ids=label_ids)
        gmail_id = result.get("id", "")
        gmail_labels = ", ".join(result.get("labelIds", []))

        tracker.update_status(
            msg_id or _composite_key(email_record),
            status=STATUS_VERIFIED,
            gmail_message_id=gmail_id,
            gmail_labels=gmail_labels,
        )
        logger.info("Migrated: %s → Gmail %s — %s", msg_id, gmail_id, subject[:60])

        # Update the verification cache so verify script knows this email is in Gmail
        if verification_cache is not None:
            try:
                verification_cache.add_to_cache({
                    "message_id": msg_id,
                    "date": email_record.get("date", ""),
                    "from_addr": email_record.get("from_addr", ""),
                    "to_addr": email_record.get("to_addr", ""),
                    "subject": subject,
                    "size": size,
                    "labels": gmail_labels,
                })
            except Exception as cache_err:
                logger.warning("Failed to update verification cache: %s", cache_err)

        return STATUS_VERIFIED

    except Exception as e:
        error = f"Gmail import failed: {e}"
        tracker.update_status(msg_id or _composite_key(email_record),
                              status=STATUS_FAILED, error=error)
        logger.error("Import failed: %s — %s: %s", msg_id, subject[:60], e)
        return STATUS_FAILED


def _composite_key(email: dict) -> str:
    """Build a composite key for emails without a Message-ID."""
    from hashlib import sha256
    raw = f"{email.get('date', '')}|{email.get('from_addr', '')}|{email.get('subject', '')}"
    return f"composite:{sha256(raw.encode()).hexdigest()[:16]}"


def write_result_csvs(tracker: MigrationTracker, output_dir: str, timestamp: str = ""):
    """Write migration result CSV reports."""
    os.makedirs(output_dir, exist_ok=True)
    all_records = tracker.get_all()

    # Full results
    results_path = os.path.join(output_dir, f"migration_results_{timestamp}.csv")
    with open(results_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Message-ID", "Date", "From", "Subject", "Fastmail Folder",
            "Gmail Label", "Status", "Error", "Gmail Message ID",
        ])
        for r in all_records:
            writer.writerow([
                r["message_id"], r["date"], r["from_addr"], r["subject"],
                r["fastmail_folder"], r["gmail_labels"], r["status"],
                r["error"], r["gmail_message_id"],
            ])

    # Failures only
    failures = [r for r in all_records if r["status"] == STATUS_FAILED]
    failures_path = os.path.join(output_dir, f"migration_failures_{timestamp}.csv")
    with open(failures_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Message-ID", "Date", "From", "Subject", "Fastmail Folder",
            "Status", "Error",
        ])
        for r in failures:
            writer.writerow([
                r["message_id"], r["date"], r["from_addr"], r["subject"],
                r["fastmail_folder"], r["status"], r["error"],
            ])

    results_name = os.path.basename(results_path)
    failures_name = os.path.basename(failures_path)
    print(f"\n  Reports saved to: {output_dir}/")
    print(f"    - {results_name}  ({len(all_records):,} rows)")
    print(f"    - {failures_name}  ({len(failures):,} rows)")


def print_final_summary(counts: dict, start_time: float):
    """Print final migration summary."""
    elapsed = time.time() - start_time
    total = sum(counts.values())
    throughput = total / elapsed if elapsed > 0 else 0

    width = 50
    print(f"\n  {'=' * width}")
    print(f"  MIGRATION COMPLETE")
    print(f"  {'=' * width}")
    print(f"  Total processed:     {total:>8,}")
    print(f"  Successfully migrated: {counts.get(STATUS_VERIFIED, 0):>8,}")
    print(f"  Skipped (duplicates):  {counts.get(STATUS_SKIPPED, 0):>8,}")
    print(f"  Failed:                {counts.get(STATUS_FAILED, 0):>8,}")
    print(f"  {'─' * width}")
    print(f"  Duration:          {elapsed:>8.1f}s")
    print(f"  Throughput:        {throughput:>8.2f} emails/s")
    print(f"  {'=' * width}")


def main():
    args = parse_args()
    profile = Profile(args.profile)
    profile.ensure_dirs()

    if not args.google_creds:
        args.google_creds = profile.google_credentials
    if not args.input:
        args.input = profile.missing_from_google_csv
    if not args.output_dir:
        args.output_dir = profile.reports_dir

    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = setup_logging(log_dir=profile.logs_dir, verbose=args.verbose, timestamp=run_timestamp)

    mode_str = " [DRY RUN]" if args.dry_run else ""
    print(f"\nMigrate Missing Emails{mode_str}")
    print("Fastmail -> Google Workspace\n")

    # Load CSV
    all_emails = load_missing_csv(args.input)
    logger.info("Loaded %d emails from %s", len(all_emails), args.input)

    # Apply filters
    emails = filter_emails(all_emails, args.folder, args.date_from, args.date_to)
    if len(emails) != len(all_emails):
        logger.info("Filtered to %d emails.", len(emails))

    # Apply --limit
    if args.limit is not None:
        emails = emails[:args.limit]
        print(f"  Limited to first {args.limit} emails (--limit {args.limit})")
        logger.info("Limited to %d emails.", args.limit)

    if not emails:
        print("  No emails match the specified filters.")
        return

    # Print plan
    print_plan(emails, args.dry_run)

    # Confirm
    if not args.confirm and not args.dry_run:
        if not confirm_migration(len(emails)):
            print("  Migration cancelled.")
            return

    # Resolve Fastmail token: CLI flag > profile token file
    fm_token = args.fastmail_token

    token_file = profile.fastmail_token_file
    if not fm_token and os.path.exists(token_file):
        fm_token = open(token_file, encoding="utf-8").read().strip()
        if fm_token:
            logger.info("Loaded Fastmail token from %s", token_file)

    if not fm_token:
        print("ERROR: Fastmail API token not found.")
        print(f"  Expected token file: {token_file}")
        print("  Run fm-setup --profile <name> to configure your Fastmail token.")
        sys.exit(1)

    # Save if requested
    if args.save_token and fm_token:
        with open(token_file, "w", encoding="utf-8") as f:
            f.write(fm_token)
        print(f"  Fastmail token saved to {token_file}")

    # Initialize components
    fetcher = FastmailFetcher(api_token=fm_token)
    importer = GmailImporter(credentials_path=args.google_creds, token_path=profile.gmail_import_token)
    tracker = MigrationTracker(db_path=profile.migration_state_db)
    verification_cache = GmailClient(db_path=profile.migration_cache_db) if not args.dry_run else None

    # Connect
    print("  Connecting to Fastmail...", end=" ", flush=True)
    try:
        fetcher.connect()
        print("OK")
    except Exception as e:
        print(f"FAILED\n  {e}")
        sys.exit(1)

    if not args.dry_run:
        print("  Connecting to Gmail...", end=" ", flush=True)
        try:
            importer.connect()
            print("OK")
        except Exception as e:
            print(f"FAILED\n  {e}")
            sys.exit(1)
        label_mapper = LabelMapper(importer.service)
    else:
        label_mapper = LabelMapper(None)

    # Resume handling
    print("  Checking migration state...", end=" ", flush=True)
    if args.resume and tracker.has_existing_state():
        pending = tracker.get_pending()
        if pending:
            print(f"resuming {len(pending):,} pending emails.")
            logger.info("Resuming migration with %d pending emails.", len(pending))
            emails_to_process = pending
        else:
            print("no pending emails. Starting fresh.")
            emails_to_process = emails
    else:
        if not args.resume:
            tracker.clear()
        print("OK")
        emails_to_process = emails

    # Register all emails in tracker (batch insert for speed)
    print(f"  Registering {len(emails_to_process):,} emails...", end=" ", flush=True)
    batch = []
    for em in emails_to_process:
        key = em["message_id"] or _composite_key(em)
        batch.append({
            "message_id": key,
            "date": em["date"],
            "from_addr": em["from_addr"],
            "subject": em["subject"],
            "fastmail_folder": em["fastmail_folder"],
        })
    tracker.add_emails_batch(batch)
    print("OK")

    # Pre-load Gmail labels so workers don't all race to do it
    if not args.dry_run:
        print("  Loading Gmail labels...", end=" ", flush=True)
        label_mapper._load_existing_labels()
        print("OK")

    # Run migration
    num_workers = max(1, args.workers)
    if not args.dry_run:
        print(f"  Workers: {num_workers}")
    print()

    run_id = tracker.start_run(len(emails_to_process))
    start_time = time.time()
    counts = defaultdict(int)
    consecutive_errors = 0
    counts_lock = threading.Lock()
    stop_event = threading.Event()

    def _sigint_handler(sig, frame):
        """Handle Ctrl+C by setting stop_event and showing drain status."""
        if stop_event.is_set():
            # Already stopping — show current drain status
            ok = counts.get(STATUS_VERIFIED, 0)
            skip = counts.get(STATUS_SKIPPED, 0)
            fail = counts.get(STATUS_FAILED, 0)
            done = ok + skip + fail
            total = len(emails_to_process)
            print(f"\r  Still stopping... {done}/{total} done (ok={ok}, skip={skip}, fail={fail}). "
                  "Workers finishing current emails...")
            return
        stop_event.set()
        print("\n\n  Interrupted. Stopping gracefully... waiting for in-flight workers to finish.")
        print("  State saved. Use --resume to continue later.")
        logger.info("Migration interrupted by user (SIGINT).")

    signal.signal(signal.SIGINT, _sigint_handler)

    # Per-thread API clients (not thread-safe, so each worker gets its own)
    thread_local = threading.local()

    def _get_thread_fetcher():
        if not hasattr(thread_local, "fetcher"):
            thread_local.fetcher = FastmailFetcher(api_token=fm_token)
            thread_local.fetcher.connect()
        return thread_local.fetcher

    def _get_thread_importer():
        if not hasattr(thread_local, "importer"):
            thread_local.importer = GmailImporter(credentials_path=args.google_creds, token_path=profile.gmail_import_token)
            thread_local.importer.connect()
        return thread_local.importer

    # Rate limiter for Gmail imports (~40/sec across all workers)
    rate_lock = threading.Lock()
    last_import_time = [0.0]
    min_interval = 0.025  # 40 imports/sec

    # Stage display callback for parallel mode (tqdm description)
    def _stage_parallel(stage, email_record):
        sender = email_record.get("from_addr", "")[:30]
        progress.set_description(f"[{stage}] {sender}")

    def _rate_limited_migrate(email_record):
        """Worker function: migrate a single email with rate limiting."""
        if stop_event.is_set():
            return None, email_record

        par_callback = _stage_parallel if args.displayemail else None

        try:
            if args.dry_run:
                # Dry run uses the shared fetcher/importer (single-threaded)
                worker_fetcher = fetcher
                worker_importer = importer
            else:
                worker_fetcher = _get_thread_fetcher()
                worker_importer = _get_thread_importer()

                # Rate limit before Gmail API calls
                with rate_lock:
                    now = time.time()
                    wait = max(0, min_interval - (now - last_import_time[0]))
                    if wait > 0:
                        time.sleep(wait)
                    last_import_time[0] = time.time()

            status = migrate_single_email(
                email_record, worker_fetcher, worker_importer, label_mapper,
                tracker, args.dry_run, verification_cache=verification_cache,
                on_stage=par_callback,
            )
            return status, email_record
        except Exception as e:
            logger.error("Unexpected error processing %s: %s",
                         email_record.get("message_id"), e)
            return STATUS_FAILED, email_record

    progress = tqdm(total=len(emails_to_process), desc="Migrating", unit="email", position=0)

    # Status line below the progress bar (sequential mode only)
    status_bar = None
    if args.displayemail and (num_workers == 1 or args.dry_run):
        status_bar = tqdm(bar_format="{desc}", position=1, leave=False)

    # Stage display callback for sequential mode (updates status bar below progress)
    def _stage_sequential(stage, email_record):
        subj = email_record.get("subject", "(no subject)")[:60]
        sender = email_record.get("from_addr", "(unknown)")
        folder = email_record.get("fastmail_folder", "")
        status_bar.set_description_str(
            f"  \033[2m[{stage:<13s}]\033[0m {sender} — {subj} \033[36m[{folder}]\033[0m"
        )

    if num_workers == 1 or args.dry_run:
        # Sequential mode (--workers 1 or dry run)
        seq_callback = _stage_sequential if args.displayemail else None
        for email_record in emails_to_process:
            if stop_event.is_set():
                break
            try:
                status = migrate_single_email(
                    email_record, fetcher, importer, label_mapper, tracker, args.dry_run,
                    verification_cache=verification_cache,
                    on_stage=seq_callback,
                )
            except Exception as e:
                logger.error("Unexpected error processing %s: %s",
                             email_record.get("message_id"), e)
                status = STATUS_FAILED

            counts[status] += 1

            if args.displayemail:
                subj = email_record.get("subject", "(no subject)")[:60]
                sender = email_record.get("from_addr", "(unknown)")
                folder = email_record.get("fastmail_folder", "")
                status_colors = {
                    STATUS_VERIFIED: "\033[32m",
                    STATUS_SKIPPED: "\033[33m",
                    STATUS_FAILED: "\033[31m",
                }
                color = status_colors.get(status, "")
                reset = "\033[0m" if color else ""
                # Update status bar with final status, then commit as permanent line
                status_bar.set_description_str("")
                tqdm.write(f"  {color}[{status:<13s}]{reset} {sender} — {subj} \033[36m[{folder}]\033[0m")

            if status == STATUS_FAILED:
                consecutive_errors += 1
                if consecutive_errors >= args.max_errors:
                    logger.error("Max consecutive errors (%d) reached. Stopping.", args.max_errors)
                    print(f"\n  Stopped: {args.max_errors} consecutive failures. Use --resume to continue later.")
                    break
            else:
                consecutive_errors = 0

            progress.update(1)
            ok = counts.get(STATUS_VERIFIED, 0)
            skip = counts.get(STATUS_SKIPPED, 0)
            fail = counts.get(STATUS_FAILED, 0)
            progress.set_postfix(ok=ok, skip=skip, fail=fail)
    else:
        # Parallel mode
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {}
            for email_record in emails_to_process:
                if stop_event.is_set():
                    break
                future = executor.submit(_rate_limited_migrate, email_record)
                futures[future] = email_record

            pending = set(futures.keys())
            while pending and not stop_event.is_set():
                # Use short timeout so main thread wakes up to handle Ctrl+C
                done = set()
                for future in list(pending):
                    if future.done():
                        done.add(future)
                if not done:
                    # Sleep briefly then re-check (allows signal handler to run)
                    time.sleep(0.1)
                    continue

                for future in done:
                    pending.discard(future)
                    try:
                        status, email_record = future.result()
                    except Exception as e:
                        email_record = futures[future]
                        logger.error("Worker exception for %s: %s",
                                     email_record.get("message_id"), e)
                        status = STATUS_FAILED

                    if status is None:
                        # Was cancelled by stop_event
                        continue

                    with counts_lock:
                        counts[status] += 1

                        if args.displayemail:
                            subj = email_record.get("subject", "(no subject)")[:60]
                            sender = email_record.get("from_addr", "(unknown)")
                            folder = email_record.get("fastmail_folder", "")
                            status_colors = {
                                STATUS_VERIFIED: "\033[32m",
                                STATUS_SKIPPED: "\033[33m",
                                STATUS_FAILED: "\033[31m",
                            }
                            color = status_colors.get(status, "")
                            reset = "\033[0m" if color else ""
                            tqdm.write(f"  {color}[{status}]{reset} {sender} — {subj} \033[36m[{folder}]\033[0m")

                        if status == STATUS_FAILED:
                            consecutive_errors += 1
                            if consecutive_errors >= args.max_errors:
                                logger.error("Max consecutive errors (%d) reached. Stopping.",
                                             args.max_errors)
                                tqdm.write(
                                    f"\n  Stopped: {args.max_errors} consecutive failures. "
                                    "Use --resume to continue later."
                                )
                                stop_event.set()
                        else:
                            consecutive_errors = 0

                    progress.update(1)
                    ok = counts.get(STATUS_VERIFIED, 0)
                    skip = counts.get(STATUS_SKIPPED, 0)
                    fail = counts.get(STATUS_FAILED, 0)
                    progress.set_postfix(ok=ok, skip=skip, fail=fail)

            # Cancel remaining futures if interrupted
            if stop_event.is_set():
                for future in pending:
                    future.cancel()
                executor.shutdown(wait=False, cancel_futures=True)

    if status_bar is not None:
        status_bar.close()
    progress.close()

    # Finalize
    tracker.finish_run(run_id, counts.get(STATUS_VERIFIED, 0),
                       counts.get(STATUS_SKIPPED, 0), counts.get(STATUS_FAILED, 0))

    print_final_summary(counts, start_time)
    write_result_csvs(tracker, args.output_dir, timestamp=run_timestamp)

    print(f"\n  Log file: {log_file}")
    if counts.get(STATUS_FAILED, 0) > 0:
        print(f"  Review {args.output_dir}/migration_failures_{run_timestamp}.csv for failed emails.")
    print()

    tracker.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Interrupted. State saved. Use --resume to continue later.")
        logging.getLogger(__name__).info("Migration interrupted by user.")
        raise SystemExit(1)
