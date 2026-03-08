"""Profile-based path resolver for multi-user support.

Each profile gets its own directory under profiles/ containing:
  - credentials/   (OAuth tokens, API keys)
  - data/          (SQLite databases)
  - logs/          (migration logs)
  - reports/       (CSV reports)

Usage:
    profile = Profile("rex")
    profile.ensure_dirs()
    tracker = MigrationTracker(db_path=profile.migration_state_db)
"""

import os

# Anchor to the project root (two levels up from this file: src/fastmail_to_gmail/profile.py)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PROFILES_DIR = os.path.join(_PROJECT_ROOT, "profiles")


class Profile:
    """Resolves all paths for a given user profile."""

    def __init__(self, name: str):
        self.name = name
        self.root = os.path.join(PROFILES_DIR, name)

    # --- Directories ---

    @property
    def credentials_dir(self) -> str:
        return os.path.join(self.root, "credentials")

    @property
    def data_dir(self) -> str:
        return os.path.join(self.root, "data")

    @property
    def logs_dir(self) -> str:
        return os.path.join(self.root, "logs")

    @property
    def reports_dir(self) -> str:
        return os.path.join(self.root, "reports")

    # --- Credential files ---

    @property
    def google_credentials(self) -> str:
        """Google OAuth client secret (credentials.json)."""
        return os.path.join(self.credentials_dir, "credentials.json")

    @property
    def gmail_token(self) -> str:
        """Gmail readonly OAuth token."""
        return os.path.join(self.credentials_dir, "token.json")

    @property
    def gmail_import_token(self) -> str:
        """Gmail import OAuth token (insert + labels scopes)."""
        return os.path.join(self.credentials_dir, "token_import.json")

    @property
    def fastmail_token_file(self) -> str:
        """Fastmail API token file."""
        return os.path.join(self.credentials_dir, ".fastmail_token")

    # --- Database files ---

    @property
    def migration_cache_db(self) -> str:
        """Email metadata cache (Fastmail + Gmail)."""
        return os.path.join(self.data_dir, "migration_cache.db")

    @property
    def migration_state_db(self) -> str:
        """Migration progress tracking database."""
        return os.path.join(self.data_dir, "migration_state.db")

    # --- Report defaults ---

    @property
    def missing_from_google_csv(self) -> str:
        """Default input CSV for migration."""
        return os.path.join(self.reports_dir, "missing_from_google.csv")

    # --- Helpers ---

    def ensure_dirs(self):
        """Create all profile directories if they don't exist."""
        for d in [self.credentials_dir, self.data_dir, self.logs_dir, self.reports_dir]:
            os.makedirs(d, exist_ok=True)

    def __repr__(self) -> str:
        return f"Profile({self.name!r})"
