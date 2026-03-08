"""Matching and comparison logic for email migration verification."""

import logging
import warnings
from collections import defaultdict
from hashlib import sha256

from dateutil import parser as dateutil_parser

warnings.filterwarnings("ignore", category=dateutil_parser.UnknownTimezoneWarning)

logger = logging.getLogger(__name__)

# Fastmail folder → Gmail label mapping
FOLDER_LABEL_MAP = {
    "inbox": "INBOX",
    "sent": "SENT",
    "sent items": "SENT",
    "sent messages": "SENT",
    "drafts": "DRAFT",
    "draft": "DRAFT",
    "trash": "TRASH",
    "bin": "TRASH",
    "deleted messages": "TRASH",
    "archive": None,  # Gmail "All Mail" has no explicit label
    "spam": "SPAM",
    "junk": "SPAM",
    "junk mail": "SPAM",
}


def _normalize_message_id(msg_id: str) -> str:
    """Normalize a Message-ID for consistent matching."""
    if not msg_id:
        return ""
    return msg_id.strip().strip("<>").lower()


def _make_composite_key(email_record: dict) -> str:
    """Create a fallback composite key from Date + From + Subject + approximate size."""
    date_str = email_record.get("date", "")
    # Normalize date to just the date portion for comparison
    try:
        dt = dateutil_parser.parse(date_str)
        date_norm = dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        date_norm = date_str

    from_addr = (email_record.get("from_addr") or "").lower().strip()
    subject = (email_record.get("subject") or "").strip().lower()
    # Round size to nearest 1000 for fuzzy matching
    size = email_record.get("size", 0)
    size_bucket = str(round(size / 1000) * 1000) if size else "0"

    raw = f"{date_norm}|{from_addr}|{subject}|{size_bucket}"
    return sha256(raw.encode()).hexdigest()


def _map_fastmail_folder_to_gmail(folder: str) -> str | None:
    """Map a Fastmail folder name to expected Gmail label."""
    lower = folder.lower().strip()
    if lower in FOLDER_LABEL_MAP:
        return FOLDER_LABEL_MAP[lower]
    # Custom folder → should appear as Gmail label with same name
    return folder


def _check_folder_match(fastmail_folder: str, gmail_labels_str: str) -> bool:
    """Check if a Fastmail folder correctly maps to one of the Gmail labels."""
    expected_label = _map_fastmail_folder_to_gmail(fastmail_folder)

    gmail_labels = [l.strip() for l in gmail_labels_str.split(",") if l.strip()]

    # Archive maps to no label (All Mail), which means no user labels
    if expected_label is None:
        # "Archive" in Fastmail → in Gmail the email exists but may lack a
        # specific label. We consider it matched if present at all.
        return True

    # Check if expected label is present in Gmail labels (case-insensitive)
    expected_lower = expected_label.lower()
    for label in gmail_labels:
        if label.lower() == expected_lower:
            return True
        # Also check category-prefixed labels like "CATEGORY_SOCIAL"
        if label.lower().replace("category_", "") == expected_lower:
            return True

    return False


def _size_within_tolerance(size_a: int, size_b: int, tolerance: float = 0.10) -> bool:
    """Check if two sizes are within tolerance of each other."""
    if size_a == 0 and size_b == 0:
        return True
    if size_a == 0 or size_b == 0:
        return False
    ratio = abs(size_a - size_b) / max(size_a, size_b)
    return ratio <= tolerance


class ComparisonResult:
    """Container for the full comparison result."""

    def __init__(self):
        self.matched_by_message_id: list[dict] = []
        self.matched_by_composite: list[dict] = []
        self.missing_from_google: list[dict] = []
        self.extra_in_google: list[dict] = []
        self.folder_mismatches: list[dict] = []
        self.total_fastmail: int = 0
        self.total_gmail: int = 0

    @property
    def total_matched(self) -> int:
        return len(self.matched_by_message_id) + len(self.matched_by_composite)

    @property
    def match_percentage(self) -> float:
        if self.total_fastmail == 0:
            return 100.0
        return (self.total_matched / self.total_fastmail) * 100


def compare_emails(fastmail_emails: list[dict], gmail_emails: list[dict]) -> ComparisonResult:
    """Compare Fastmail and Gmail email lists and produce a ComparisonResult.

    Matching strategy:
    1. Primary: Match by normalized Message-ID
    2. Fallback: Match by composite key (Date + From + Subject + ~Size)
    """
    result = ComparisonResult()

    # Deduplicate Fastmail emails: keep one record per unique message_id,
    # but track all folders the email appears in.
    fm_by_msgid: dict[str, dict] = {}
    fm_by_composite: dict[str, dict] = {}
    fm_no_msgid: list[dict] = []

    for em in fastmail_emails:
        msg_id = _normalize_message_id(em.get("message_id", ""))
        if msg_id:
            if msg_id not in fm_by_msgid:
                fm_by_msgid[msg_id] = {**em, "message_id": msg_id, "_folders": [em.get("folder", "")]}
            else:
                fm_by_msgid[msg_id]["_folders"].append(em.get("folder", ""))
        else:
            comp_key = _make_composite_key(em)
            if comp_key not in fm_by_composite:
                fm_by_composite[comp_key] = {**em, "_composite_key": comp_key, "_folders": [em.get("folder", "")]}
            else:
                fm_by_composite[comp_key]["_folders"].append(em.get("folder", ""))
            fm_no_msgid.append(em)

    # Index Gmail emails similarly
    gm_by_msgid: dict[str, dict] = {}
    gm_by_composite: dict[str, dict] = {}
    gm_no_msgid: list[dict] = []

    for em in gmail_emails:
        msg_id = _normalize_message_id(em.get("message_id", ""))
        if msg_id:
            if msg_id not in gm_by_msgid:
                gm_by_msgid[msg_id] = {**em, "message_id": msg_id}
        else:
            comp_key = _make_composite_key(em)
            if comp_key not in gm_by_composite:
                gm_by_composite[comp_key] = {**em, "_composite_key": comp_key}
            gm_no_msgid.append(em)

    result.total_fastmail = len(fm_by_msgid) + len(fm_by_composite)
    result.total_gmail = len(gm_by_msgid) + len(gm_by_composite)

    logger.info("Fastmail unique emails: %d (by Message-ID: %d, no Message-ID: %d)",
                result.total_fastmail, len(fm_by_msgid), len(fm_by_composite))
    logger.info("Gmail unique emails: %d (by Message-ID: %d, no Message-ID: %d)",
                result.total_gmail, len(gm_by_msgid), len(gm_by_composite))

    # Phase 1: Match by Message-ID
    matched_msgids = set()
    for msg_id, fm_email in fm_by_msgid.items():
        if msg_id in gm_by_msgid:
            gm_email = gm_by_msgid[msg_id]
            matched_msgids.add(msg_id)
            match_record = {
                "message_id": msg_id,
                "date": fm_email.get("date", ""),
                "from_addr": fm_email.get("from_addr", ""),
                "to_addr": fm_email.get("to_addr", ""),
                "subject": fm_email.get("subject", ""),
                "fastmail_size": fm_email.get("size", 0),
                "gmail_size": gm_email.get("size", 0),
                "fastmail_folder": ", ".join(fm_email.get("_folders", [])),
                "gmail_labels": gm_email.get("labels", ""),
                "match_type": "message_id",
            }
            result.matched_by_message_id.append(match_record)

            # Check folder mapping
            for folder in fm_email.get("_folders", []):
                if folder and not _check_folder_match(folder, gm_email.get("labels", "")):
                    result.folder_mismatches.append({
                        "message_id": msg_id,
                        "subject": fm_email.get("subject", ""),
                        "fastmail_folder": folder,
                        "gmail_labels": gm_email.get("labels", ""),
                    })

    # Phase 1.5: Cross-match unmatched emails by composite key.
    # This catches emails where one side has a Message-ID but the other doesn't
    # (e.g. Gmail API didn't return the Message-ID header due to casing).
    matched_cross_fm = set()   # msg_ids from fm_by_msgid that got cross-matched
    matched_cross_gm = set()   # composite keys from gm_by_composite that got cross-matched

    # Build composite keys for unmatched Fastmail emails that have message_ids
    for msg_id, fm_email in fm_by_msgid.items():
        if msg_id in matched_msgids:
            continue
        comp_key = _make_composite_key(fm_email)
        if comp_key in gm_by_composite:
            gm_email = gm_by_composite[comp_key]
            matched_cross_fm.add(msg_id)
            matched_cross_gm.add(comp_key)
            match_record = {
                "message_id": msg_id,
                "date": fm_email.get("date", ""),
                "from_addr": fm_email.get("from_addr", ""),
                "to_addr": fm_email.get("to_addr", ""),
                "subject": fm_email.get("subject", ""),
                "fastmail_size": fm_email.get("size", 0),
                "gmail_size": gm_email.get("size", 0),
                "fastmail_folder": ", ".join(fm_email.get("_folders", [])),
                "gmail_labels": gm_email.get("labels", ""),
                "match_type": "composite_cross",
            }
            result.matched_by_composite.append(match_record)

            for folder in fm_email.get("_folders", []):
                if folder and not _check_folder_match(folder, gm_email.get("labels", "")):
                    result.folder_mismatches.append({
                        "message_id": msg_id,
                        "subject": fm_email.get("subject", ""),
                        "fastmail_folder": folder,
                        "gmail_labels": gm_email.get("labels", ""),
                    })

    # Also check: Gmail emails with message_ids that didn't match,
    # vs Fastmail emails without message_ids
    matched_cross_gm_msgid = set()
    matched_cross_fm_comp = set()
    for msg_id, gm_email in gm_by_msgid.items():
        if msg_id in matched_msgids:
            continue
        comp_key = _make_composite_key(gm_email)
        if comp_key in fm_by_composite and comp_key not in matched_cross_fm_comp:
            fm_email = fm_by_composite[comp_key]
            matched_cross_gm_msgid.add(msg_id)
            matched_cross_fm_comp.add(comp_key)
            match_record = {
                "message_id": msg_id,
                "date": fm_email.get("date", ""),
                "from_addr": fm_email.get("from_addr", ""),
                "to_addr": fm_email.get("to_addr", ""),
                "subject": fm_email.get("subject", ""),
                "fastmail_size": fm_email.get("size", 0),
                "gmail_size": gm_email.get("size", 0),
                "fastmail_folder": ", ".join(fm_email.get("_folders", [])),
                "gmail_labels": gm_email.get("labels", ""),
                "match_type": "composite_cross",
            }
            result.matched_by_composite.append(match_record)

            for folder in fm_email.get("_folders", []):
                if folder and not _check_folder_match(folder, gm_email.get("labels", "")):
                    result.folder_mismatches.append({
                        "message_id": msg_id,
                        "subject": fm_email.get("subject", ""),
                        "fastmail_folder": folder,
                        "gmail_labels": gm_email.get("labels", ""),
                    })

    if matched_cross_fm or matched_cross_gm_msgid:
        logger.info("Phase 1.5 cross-matched %d additional emails by composite key.",
                     len(matched_cross_fm) + len(matched_cross_gm_msgid))

    # Phase 1.75: Match unmatched emails where BOTH sides have Message-IDs
    # but the IDs are different (e.g. Exchange/Outlook rewrote the Message-ID
    # during migration). Use composite key to find matches.
    matched_phase175_fm = set()   # fm msg_ids matched in this phase
    matched_phase175_gm = set()   # gm msg_ids matched in this phase

    # Build composite key index for unmatched Gmail emails with message_ids
    gm_unmatched_by_composite: dict[str, tuple[str, dict]] = {}
    for msg_id, gm_email in gm_by_msgid.items():
        if msg_id in matched_msgids or msg_id in matched_cross_gm_msgid:
            continue
        comp_key = _make_composite_key(gm_email)
        if comp_key not in gm_unmatched_by_composite:
            gm_unmatched_by_composite[comp_key] = (msg_id, gm_email)

    # Match unmatched Fastmail emails against this index
    for msg_id, fm_email in fm_by_msgid.items():
        if msg_id in matched_msgids or msg_id in matched_cross_fm:
            continue
        comp_key = _make_composite_key(fm_email)
        if comp_key in gm_unmatched_by_composite:
            gm_msg_id, gm_email = gm_unmatched_by_composite[comp_key]
            if gm_msg_id in matched_phase175_gm:
                continue  # Already matched this Gmail email
            matched_phase175_fm.add(msg_id)
            matched_phase175_gm.add(gm_msg_id)
            match_record = {
                "message_id": msg_id,
                "date": fm_email.get("date", ""),
                "from_addr": fm_email.get("from_addr", ""),
                "to_addr": fm_email.get("to_addr", ""),
                "subject": fm_email.get("subject", ""),
                "fastmail_size": fm_email.get("size", 0),
                "gmail_size": gm_email.get("size", 0),
                "fastmail_folder": ", ".join(fm_email.get("_folders", [])),
                "gmail_labels": gm_email.get("labels", ""),
                "match_type": "composite_both_msgid",
            }
            result.matched_by_composite.append(match_record)

            for folder in fm_email.get("_folders", []):
                if folder and not _check_folder_match(folder, gm_email.get("labels", "")):
                    result.folder_mismatches.append({
                        "message_id": msg_id,
                        "subject": fm_email.get("subject", ""),
                        "fastmail_folder": folder,
                        "gmail_labels": gm_email.get("labels", ""),
                    })

    if matched_phase175_fm:
        logger.info("Phase 1.75 matched %d emails with different Message-IDs by composite key.",
                     len(matched_phase175_fm))

    # Emails in Fastmail with Message-ID but not matched (after all phases)
    for msg_id, fm_email in fm_by_msgid.items():
        if msg_id not in matched_msgids and msg_id not in matched_cross_fm and msg_id not in matched_phase175_fm:
            result.missing_from_google.append({
                "message_id": msg_id,
                "date": fm_email.get("date", ""),
                "from_addr": fm_email.get("from_addr", ""),
                "to_addr": fm_email.get("to_addr", ""),
                "subject": fm_email.get("subject", ""),
                "fastmail_folder": ", ".join(fm_email.get("_folders", [])),
            })

    # Emails in Gmail with Message-ID but not matched (after all phases)
    for msg_id, gm_email in gm_by_msgid.items():
        if msg_id not in matched_msgids and msg_id not in matched_cross_gm_msgid and msg_id not in matched_phase175_gm:
            result.extra_in_google.append({
                "message_id": msg_id,
                "date": gm_email.get("date", ""),
                "from_addr": gm_email.get("from_addr", ""),
                "to_addr": gm_email.get("to_addr", ""),
                "subject": gm_email.get("subject", ""),
                "gmail_labels": gm_email.get("labels", ""),
            })

    # Phase 2: Match remaining emails without Message-ID by composite key
    matched_composites = set()
    for comp_key, fm_email in fm_by_composite.items():
        if comp_key in matched_cross_fm_comp:
            continue  # Already matched in cross-phase
        if comp_key in gm_by_composite and comp_key not in matched_cross_gm:
            gm_email = gm_by_composite[comp_key]
            matched_composites.add(comp_key)
            match_record = {
                "message_id": "",
                "date": fm_email.get("date", ""),
                "from_addr": fm_email.get("from_addr", ""),
                "to_addr": fm_email.get("to_addr", ""),
                "subject": fm_email.get("subject", ""),
                "fastmail_size": fm_email.get("size", 0),
                "gmail_size": gm_email.get("size", 0),
                "fastmail_folder": ", ".join(fm_email.get("_folders", [])),
                "gmail_labels": gm_email.get("labels", ""),
                "match_type": "composite",
            }
            result.matched_by_composite.append(match_record)

            for folder in fm_email.get("_folders", []):
                if folder and not _check_folder_match(folder, gm_email.get("labels", "")):
                    result.folder_mismatches.append({
                        "message_id": "",
                        "subject": fm_email.get("subject", ""),
                        "fastmail_folder": folder,
                        "gmail_labels": gm_email.get("labels", ""),
                    })

    # Unmatched composites
    for comp_key, fm_email in fm_by_composite.items():
        if comp_key not in matched_composites and comp_key not in matched_cross_fm_comp:
            result.missing_from_google.append({
                "message_id": "",
                "date": fm_email.get("date", ""),
                "from_addr": fm_email.get("from_addr", ""),
                "to_addr": fm_email.get("to_addr", ""),
                "subject": fm_email.get("subject", ""),
                "fastmail_folder": ", ".join(fm_email.get("_folders", [])),
            })

    for comp_key, gm_email in gm_by_composite.items():
        if comp_key not in matched_composites and comp_key not in matched_cross_gm:
            result.extra_in_google.append({
                "message_id": "",
                "date": gm_email.get("date", ""),
                "from_addr": gm_email.get("from_addr", ""),
                "to_addr": gm_email.get("to_addr", ""),
                "subject": gm_email.get("subject", ""),
                "gmail_labels": gm_email.get("labels", ""),
            })

    logger.info("Comparison complete. Matched: %d (Message-ID: %d, Composite: %d), "
                "Missing: %d, Extra: %d, Folder mismatches: %d",
                result.total_matched,
                len(result.matched_by_message_id),
                len(result.matched_by_composite),
                len(result.missing_from_google),
                len(result.extra_in_google),
                len(result.folder_mismatches))

    return result
