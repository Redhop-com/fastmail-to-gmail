"""Fastmail folder to Gmail label mapping and label creation."""

import logging
import threading

logger = logging.getLogger(__name__)

# Fastmail folder → Gmail system label mapping
FOLDER_TO_LABEL = {
    "inbox": "INBOX",
    "sent": "SENT",
    "sent items": "SENT",
    "sent messages": "SENT",
    "drafts": "DRAFT",
    "draft": "DRAFT",
    "trash": "TRASH",
    "bin": "TRASH",
    "deleted messages": "TRASH",
    "archive": None,  # No label — just All Mail
    "spam": "SPAM",
    "junk": "SPAM",
    "junk mail": "SPAM",
}

# Gmail system labels that cannot be created via the API
SYSTEM_LABELS = {
    "INBOX", "SENT", "DRAFT", "TRASH", "SPAM", "STARRED", "IMPORTANT",
    "UNREAD", "CATEGORY_PERSONAL", "CATEGORY_SOCIAL", "CATEGORY_PROMOTIONS",
    "CATEGORY_UPDATES", "CATEGORY_FORUMS",
}


class LabelMapper:
    """Maps Fastmail folders to Gmail labels, creating custom labels as needed."""

    def __init__(self, gmail_service):
        self.service = gmail_service
        self._label_cache: dict[str, str] = {}  # name -> label ID
        self._loaded = False
        self._lock = threading.Lock()

    def _load_existing_labels(self):
        """Fetch all existing Gmail labels into the cache."""
        with self._lock:
            if self._loaded:
                return
            result = self.service.users().labels().list(userId="me").execute()
            for label in result.get("labels", []):
                self._label_cache[label["name"].lower()] = label["id"]
                # Also store by ID for system labels
                self._label_cache[label["id"].lower()] = label["id"]
            self._loaded = True
            logger.info("Loaded %d existing Gmail labels.", len(result.get("labels", [])))

    def map_folder(self, fastmail_folder: str) -> list[str]:
        """Map a Fastmail folder name to a list of Gmail label IDs to apply.

        Returns a list of label IDs. Empty list means no labels (Archive / All Mail).
        """
        self._load_existing_labels()

        folder_lower = fastmail_folder.strip().lower()

        # Check system mapping first
        if folder_lower in FOLDER_TO_LABEL:
            system_label = FOLDER_TO_LABEL[folder_lower]
            if system_label is None:
                # Archive — no label needed
                return []
            # System label — look up ID
            label_id = self._label_cache.get(system_label.lower(), system_label)
            return [label_id]

        # Custom folder — find or create a matching Gmail label
        label_id = self._get_or_create_label(fastmail_folder.strip())
        return [label_id]

    def _get_or_create_label(self, label_name: str) -> str:
        """Get an existing label ID or create a new one. Thread-safe."""
        with self._lock:
            # Check cache
            cached_id = self._label_cache.get(label_name.lower())
            if cached_id:
                return cached_id

            # Create the label
            logger.info("Creating Gmail label: %s", label_name)
            try:
                result = self.service.users().labels().create(
                    userId="me",
                    body={
                        "name": label_name,
                        "labelListVisibility": "labelShow",
                        "messageListVisibility": "show",
                    },
                ).execute()
                label_id = result["id"]
                self._label_cache[label_name.lower()] = label_id
                logger.info("Created Gmail label '%s' with ID %s", label_name, label_id)
                return label_id
            except Exception as e:
                # Label might already exist (race condition or case mismatch)
                logger.warning("Failed to create label '%s': %s. Retrying lookup.", label_name, e)
                self._loaded = False
                self._label_cache.clear()
                result = self.service.users().labels().list(userId="me").execute()
                for label in result.get("labels", []):
                    self._label_cache[label["name"].lower()] = label["id"]
                    self._label_cache[label["id"].lower()] = label["id"]
                self._loaded = True
                cached_id = self._label_cache.get(label_name.lower())
                if cached_id:
                    return cached_id
                raise RuntimeError(f"Cannot create or find Gmail label '{label_name}': {e}") from e

    def map_folder_dry_run(self, fastmail_folder: str) -> str:
        """Return the Gmail label name that would be used, without creating anything."""
        folder_lower = fastmail_folder.strip().lower()
        if folder_lower in FOLDER_TO_LABEL:
            system_label = FOLDER_TO_LABEL[folder_lower]
            return system_label if system_label else "(no label — All Mail)"
        return fastmail_folder.strip()
