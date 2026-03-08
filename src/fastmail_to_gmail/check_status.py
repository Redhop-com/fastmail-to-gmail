#!/usr/bin/env python3
"""Live migration status checker — run in a separate terminal while migration is running.

Usage:
    uv run fm-status --profile rex                (summary, refreshes every 5s)
    uv run fm-status --profile rex --failures     (show failed emails)
    uv run fm-status --profile rex --folders      (show status by folder)
    uv run fm-status --profile rex --once         (single snapshot, no refresh)
    uv run fm-status --profile rex --interval 10  (refresh every 10s)
"""

import argparse
import os
import sqlite3
import sys
import time

from fastmail_to_gmail.profile import Profile


def get_connection(db_path):
    if not os.path.exists(db_path):
        print(f"  No migration_state.db found at {db_path}. Is the migration running?")
        sys.exit(1)
    conn = sqlite3.connect(db_path, timeout=5)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def show_failures(db_path):
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT from_addr, substr(subject,1,50), error "
        "FROM migration_state WHERE status='failed'"
    ).fetchall()
    conn.close()

    print(f"\n  Failed emails: {len(rows)}")
    print(f"  {'-' * 90}")
    if rows:
        for from_addr, subject, error in rows:
            print(f"  {from_addr:<30s} {subject:<50s} {error}")
    else:
        print("  (none)")

    # Summary by error type
    if rows:
        error_counts = {}
        for _, _, error in rows:
            key = error.split(":")[0].strip() if ":" in error else error.strip()
            error_counts[key] = error_counts.get(key, 0) + 1

        print(f"\n  Summary")
        print(f"  {'-' * 40}")
        for error_type, count in sorted(error_counts.items(), key=lambda x: -x[1]):
            print(f"  {count:>6,}  {error_type}")
        print(f"  {'-' * 40}")
        print(f"  {len(rows):>6,}  Total failures")
    print()


def show_folders(db_path):
    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT fastmail_folder, status, COUNT(*) "
        "FROM migration_state GROUP BY fastmail_folder, status "
        "ORDER BY fastmail_folder, status"
    ).fetchall()
    conn.close()

    # Build per-folder counts
    folders = {}
    for folder, status, count in rows:
        if folder not in folders:
            folders[folder] = {}
        folders[folder][status] = count

    # Determine column width
    col = max((len(f) for f in folders), default=10) + 2
    col = max(col, 10)

    print(f"\n  Status by folder")
    print(f"  {'-' * (col + 40)}")
    print(f"  {'Folder':<{col}} {'Verified':>10} {'Skipped':>10} {'Failed':>8} {'Pending':>10}")
    print(f"  {'-' * (col + 40)}")
    for folder in sorted(folders.keys()):
        s = folders[folder]
        print(
            f"  {folder:<{col}} "
            f"{s.get('verified', 0):>10,} "
            f"{s.get('skipped', 0):>10,} "
            f"{s.get('failed', 0):>8,} "
            f"{s.get('pending', 0):>10,}"
        )
    print()


def show_summary(db_path):
    conn = get_connection(db_path)
    counts = conn.execute(
        "SELECT status, COUNT(*) FROM migration_state GROUP BY status"
    ).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM migration_state").fetchone()[0]
    done = conn.execute(
        "SELECT COUNT(*) FROM migration_state WHERE status != 'pending'"
    ).fetchone()[0]
    conn.close()

    now = time.strftime("%H:%M:%S")
    print(f"\n  Migration Status  {now}")
    print(f"  {'-' * 40}")
    for status, count in sorted(counts):
        print(f"  {status:<14s} {count:>10,}")
    print(f"  {'-' * 40}")
    pct = (done / total * 100) if total else 0
    print(f"  Total: {total:,}  |  Processed: {done:,}  ({pct:.1f}%)")


def main():
    parser = argparse.ArgumentParser(description="Check migration status")
    parser.add_argument("--profile", required=True, help="User profile name (e.g. rex)")
    parser.add_argument("--failures", action="store_true", help="Show failed emails")
    parser.add_argument("--folders", action="store_true", help="Show status by folder")
    parser.add_argument("--once", action="store_true", help="Single check, no refresh")
    parser.add_argument("--interval", type=int, default=5, help="Refresh interval in seconds")
    args = parser.parse_args()

    profile = Profile(args.profile)
    db_path = profile.migration_state_db

    if args.failures:
        show_failures(db_path)
        return

    if args.folders:
        show_folders(db_path)
        return

    try:
        while True:
            os.system("cls" if os.name == "nt" else "clear")
            show_summary(db_path)
            if args.once:
                print()
                break
            print(f"\n  Refreshing every {args.interval}s... (Ctrl+C to stop)")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print()


if __name__ == "__main__":
    main()
