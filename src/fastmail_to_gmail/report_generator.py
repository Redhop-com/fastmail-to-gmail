"""Console and CSV report generation for email migration verification."""

import csv
import logging
import os
from collections import defaultdict

from fastmail_to_gmail.comparator import ComparisonResult

logger = logging.getLogger(__name__)


def print_summary(result: ComparisonResult):
    """Print a summary report to the console."""
    width = 60
    print("\n" + "=" * width)
    print("  EMAIL MIGRATION VERIFICATION REPORT")
    print("=" * width)

    print(f"\n  Total emails in Fastmail:       {result.total_fastmail:>8,}")
    print(f"  Total emails in Google:         {result.total_gmail:>8,}")
    print(f"  {'-' * (width - 4)}")
    print(f"  Matched (by Message-ID):        {len(result.matched_by_message_id):>8,}")
    print(f"  Matched (by composite key):     {len(result.matched_by_composite):>8,}")
    print(f"  Total matched:                  {result.total_matched:>8,}")
    print(f"  {'-' * (width - 4)}")
    print(f"  Missing from Google:            {len(result.missing_from_google):>8,}")
    print(f"  Extra in Google:                {len(result.extra_in_google):>8,}")
    print(f"  Folder/label mismatches:        {len(result.folder_mismatches):>8,}")
    print(f"  {'-' * (width - 4)}")
    print(f"  Match percentage:               {result.match_percentage:>7.2f}%")

    # Per-folder breakdown
    _print_folder_breakdown(result)

    print("\n" + "=" * width)


def _print_folder_breakdown(result: ComparisonResult):
    """Print per-folder/label match breakdown."""
    folder_stats: dict[str, dict[str, int]] = defaultdict(lambda: {"matched": 0, "missing": 0})

    for m in result.matched_by_message_id + result.matched_by_composite:
        folders = m.get("fastmail_folder", "").split(", ")
        for f in folders:
            if f:
                folder_stats[f]["matched"] += 1

    for m in result.missing_from_google:
        folders = m.get("fastmail_folder", "").split(", ")
        for f in folders:
            if f:
                folder_stats[f]["missing"] += 1

    if not folder_stats:
        return

    # Dynamic column width based on longest folder name
    col = max(len(f) for f in folder_stats) + 2
    col = max(col, 10)  # minimum width
    line_width = col + 10 + 10 + 10

    print(f"\n  Per-Folder Breakdown:")
    print(f"  {'Folder':<{col}} {'Matched':>10} {'Missing':>10} {'Match %':>10}")
    print(f"  {'-' * line_width}")

    for folder in sorted(folder_stats.keys()):
        stats = folder_stats[folder]
        total = stats["matched"] + stats["missing"]
        pct = (stats["matched"] / total * 100) if total > 0 else 0
        print(f"  {folder:<{col}} {stats['matched']:>10,} {stats['missing']:>10,} {pct:>9.1f}%")


def write_csv_reports(result: ComparisonResult, output_dir: str = "./reports"):
    """Write detailed CSV reports to the output directory."""
    os.makedirs(output_dir, exist_ok=True)

    _write_missing_csv(result, output_dir)
    _write_extra_csv(result, output_dir)
    _write_folder_mismatches_csv(result, output_dir)
    _write_full_comparison_csv(result, output_dir)

    logger.info("CSV reports written to %s", output_dir)
    print(f"\n  CSV reports saved to: {output_dir}/")
    print(f"    - missing_from_google.csv  ({len(result.missing_from_google):,} rows)")
    print(f"    - extra_in_google.csv      ({len(result.extra_in_google):,} rows)")
    print(f"    - folder_mismatches.csv    ({len(result.folder_mismatches):,} rows)")
    total_rows = (result.total_matched + len(result.missing_from_google) + len(result.extra_in_google))
    print(f"    - full_comparison.csv      ({total_rows:,} rows)")


def _write_missing_csv(result: ComparisonResult, output_dir: str):
    """Write emails missing from Google to CSV."""
    path = os.path.join(output_dir, "missing_from_google.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Message-ID", "Date", "From", "To", "Subject", "Fastmail Folder"])
        for em in result.missing_from_google:
            writer.writerow([
                em.get("message_id", ""),
                em.get("date", ""),
                em.get("from_addr", ""),
                em.get("to_addr", ""),
                em.get("subject", ""),
                em.get("fastmail_folder", ""),
            ])


def _write_extra_csv(result: ComparisonResult, output_dir: str):
    """Write emails extra in Google to CSV."""
    path = os.path.join(output_dir, "extra_in_google.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Message-ID", "Date", "From", "To", "Subject", "Gmail Labels"])
        for em in result.extra_in_google:
            writer.writerow([
                em.get("message_id", ""),
                em.get("date", ""),
                em.get("from_addr", ""),
                em.get("to_addr", ""),
                em.get("subject", ""),
                em.get("gmail_labels", ""),
            ])


def _write_folder_mismatches_csv(result: ComparisonResult, output_dir: str):
    """Write folder/label mismatches to CSV."""
    path = os.path.join(output_dir, "folder_mismatches.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Message-ID", "Subject", "Fastmail Folder", "Gmail Labels"])
        for em in result.folder_mismatches:
            writer.writerow([
                em.get("message_id", ""),
                em.get("subject", ""),
                em.get("fastmail_folder", ""),
                em.get("gmail_labels", ""),
            ])


def _write_full_comparison_csv(result: ComparisonResult, output_dir: str):
    """Write the full comparison (all emails with status) to CSV."""
    path = os.path.join(output_dir, "full_comparison.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Message-ID", "Date", "From", "To", "Subject",
            "Fastmail Size", "Gmail Size", "Fastmail Folder", "Gmail Labels",
            "Status", "Match Type",
        ])

        # Matched emails
        for em in result.matched_by_message_id:
            writer.writerow([
                em.get("message_id", ""),
                em.get("date", ""),
                em.get("from_addr", ""),
                em.get("to_addr", ""),
                em.get("subject", ""),
                em.get("fastmail_size", ""),
                em.get("gmail_size", ""),
                em.get("fastmail_folder", ""),
                em.get("gmail_labels", ""),
                "Matched",
                "Message-ID",
            ])

        for em in result.matched_by_composite:
            writer.writerow([
                em.get("message_id", ""),
                em.get("date", ""),
                em.get("from_addr", ""),
                em.get("to_addr", ""),
                em.get("subject", ""),
                em.get("fastmail_size", ""),
                em.get("gmail_size", ""),
                em.get("fastmail_folder", ""),
                em.get("gmail_labels", ""),
                "Matched",
                "Composite Key",
            ])

        # Missing from Google
        for em in result.missing_from_google:
            writer.writerow([
                em.get("message_id", ""),
                em.get("date", ""),
                em.get("from_addr", ""),
                em.get("to_addr", ""),
                em.get("subject", ""),
                "",  # no Gmail size
                "",
                em.get("fastmail_folder", ""),
                "",
                "Missing from Google",
                "",
            ])

        # Extra in Google
        for em in result.extra_in_google:
            writer.writerow([
                em.get("message_id", ""),
                em.get("date", ""),
                em.get("from_addr", ""),
                em.get("to_addr", ""),
                em.get("subject", ""),
                "",
                "",
                "",
                em.get("gmail_labels", ""),
                "Extra in Google",
                "",
            ])
