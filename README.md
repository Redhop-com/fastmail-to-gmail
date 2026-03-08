# Email Migration: Fastmail → Google Workspace

Tools to verify and migrate emails from Fastmail to Google Workspace, with multi-user profile support.

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) package manager
- A Fastmail API token with Mail read-only access
- Google Cloud OAuth 2.0 credentials with Gmail API enabled

## Quick Start

```bash
# Install uv (if not already installed)
# Windows:
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
# macOS/Linux:
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Set up a profile (interactive wizard)
uv run fm-setup --profile rex

# Verify migration
uv run fm-verify --profile rex --dry-run        # Test auth
uv run fm-verify --profile rex --refresh         # Full comparison

# Preview and run migration
uv run fm-migrate --profile rex --dry-run        # Preview
uv run fm-migrate --profile rex --workers 10     # Parallel migration

# Monitor progress (separate terminal)
uv run fm-status --profile rex                   # Live dashboard
uv run fm-status --profile rex --failures        # Show failed emails
uv run fm-status --profile rex --folders         # Status by folder
```

---

## Setup

### 1. Install dependencies

```bash
uv sync
```

This creates a `.venv/` virtual environment and installs all dependencies from `pyproject.toml`.

### 2. Create a profile

```bash
uv run fm-setup --profile rex
```

The setup wizard walks you through:

1. **Google OAuth credentials** — checks for `credentials.json` and provides Google Cloud Console instructions if missing
2. **Fastmail API token** — prompts for your token and saves it securely
3. **Gmail readonly access** — opens browser for OAuth consent (readonly scope)
4. **Gmail import access** — opens browser for OAuth consent (import scope)
5. **Connection test** — verifies all connections work

### 3. Manual credential setup (alternative)

If you prefer to set up credentials manually:

#### Fastmail API Token

1. Go to **Fastmail → Settings → Privacy & Security → API Tokens**
2. Create a new token with **Mail read-only** access
3. Save to `profiles/<name>/credentials/.fastmail_token`

#### Google OAuth Credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project (or select an existing one)
3. Enable the **Gmail API** under APIs & Services
4. Go to **Credentials → Create Credentials → OAuth 2.0 Client ID**
5. Select **Desktop application** as the application type
6. Download the JSON file and save as `profiles/<name>/credentials/credentials.json`

---

## Tool 1: Setup (`fm-setup`)

Interactive wizard to create and configure a migration profile.

```bash
uv run fm-setup --profile rex
```

---

## Tool 2: Verify (`fm-verify`)

Compares emails between Fastmail and Gmail to identify what's missing, extra, or mismatched. **Read-only** — never modifies any emails.

### Usage

```bash
uv run fm-verify --profile rex --dry-run                    # Test auth
uv run fm-verify --profile rex                              # Full comparison
uv run fm-verify --profile rex --folder Inbox               # One folder
uv run fm-verify --profile rex --date-from 2024-01-01       # Date filter
uv run fm-verify --profile rex --refresh                    # Re-fetch all
uv run fm-verify --profile rex --refresh-fastmail           # Re-fetch Fastmail only
uv run fm-verify --profile rex --refresh-gmail              # Re-fetch Gmail only
```

### CLI Options

| Option | Description |
|---|---|
| `--profile` | **Required.** User profile name (e.g. `rex`) |
| `--refresh` | Re-fetch all data, ignoring cache |
| `--refresh-fastmail` | Re-fetch Fastmail only (keep Gmail cache) |
| `--refresh-gmail` | Re-fetch Gmail only (keep Fastmail cache) |
| `--folder <name>` | Only compare a specific folder/label |
| `--date-from YYYY-MM-DD` | Only compare emails from this date |
| `--date-to YYYY-MM-DD` | Only compare emails up to this date |
| `--verbose` | Detailed console logging |
| `--dry-run` | Test authentication only |

### Output

- Console summary with match percentages and per-folder breakdown
- `profiles/<name>/reports/missing_from_google.csv` — emails in Fastmail but not Gmail
- `profiles/<name>/reports/extra_in_google.csv` — emails in Gmail but not Fastmail
- `profiles/<name>/reports/folder_mismatches.csv` — matched emails with wrong labels
- `profiles/<name>/reports/full_comparison.csv` — all emails with match status

---

## Tool 3: Migrate (`fm-migrate`)

Copies missing emails from Fastmail to Gmail. Reads `missing_from_google.csv` from the verify step. **Copy-only** — Fastmail emails are never modified or deleted.

### Safety Features

- Checks for duplicates before each upload (via `rfc822msgid:` search)
- Requires explicit confirmation before starting (or `--yes`)
- Tracks state in SQLite for safe resume after interruption
- `--dry-run` previews without uploading
- Rate-limited to stay under Gmail quotas
- Stops after N consecutive errors (`--max-errors`)
- Graceful `Ctrl+C` — saves state immediately, resume with `--resume`

### Usage

```bash
uv run fm-migrate --profile rex --dry-run                    # Preview
uv run fm-migrate --profile rex                              # Execute (single-threaded)
uv run fm-migrate --profile rex --workers 10                 # Parallel (recommended)
uv run fm-migrate --profile rex --folder Inbox               # One folder
uv run fm-migrate --profile rex --resume                     # Resume interrupted
uv run fm-migrate --profile rex --resume --workers 20        # Resume with more workers
uv run fm-migrate --profile rex --yes                        # Skip confirmation
```

### CLI Options

| Option | Description |
|---|---|
| `--profile` | **Required.** User profile name (e.g. `rex`) |
| `--input` | Path to `missing_from_google.csv` (default: profile reports dir) |
| `--workers <n>` | Parallel workers (default: 1, recommended: 10-20) |
| `--folder <name>` | Only migrate a specific folder |
| `--date-from YYYY-MM-DD` | Only migrate emails from this date |
| `--date-to YYYY-MM-DD` | Only migrate emails up to this date |
| `--dry-run` | Preview without uploading |
| `--yes` | Skip confirmation prompt |
| `--resume` | Resume interrupted migration |
| `--max-errors <n>` | Stop after N consecutive failures (default: 20) |
| `--verbose` | Detailed console logging |
| `--displayemail` | Show per-email status during migration |

### Output

- Console summary with counts, duration, throughput
- `profiles/<name>/reports/migration_results_<timestamp>.csv` — every email with status
- `profiles/<name>/reports/migration_failures_<timestamp>.csv` — failed emails for investigation
- `profiles/<name>/logs/migration_<timestamp>.log` — detailed log

---

## Tool 4: Monitor (`fm-status`)

Live status checker to monitor migration progress from a separate terminal. Reads directly from the SQLite state database.

### Usage

```bash
uv run fm-status --profile rex                    # Live dashboard (refreshes every 5s)
uv run fm-status --profile rex --once             # Single snapshot
uv run fm-status --profile rex --failures         # Show failed emails with error summary
uv run fm-status --profile rex --folders          # Status breakdown by folder
uv run fm-status --profile rex --interval 10      # Custom refresh interval
```

---

## Recommended Workflow

```
1. Setup    →  uv run fm-setup --profile rex
2. Verify   →  uv run fm-verify --profile rex --refresh
3. Review   →  Open profiles/rex/reports/missing_from_google.csv
4. Preview  →  uv run fm-migrate --profile rex --dry-run
5. Migrate  →  uv run fm-migrate --profile rex --workers 10
6. Monitor  →  uv run fm-status --profile rex              (in a separate terminal)
7. Confirm  →  uv run fm-verify --profile rex --refresh
```

---

## Folder/Label Mapping

| Fastmail Folder | Gmail Label |
|---|---|
| Inbox | INBOX |
| Sent / Sent Items | SENT |
| Drafts | DRAFT |
| Trash | TRASH |
| Archive | All Mail (no label) |
| Spam / Junk | SPAM |
| Custom folders | Gmail labels (created automatically) |

---

## Multi-User Profiles

Each profile has its own isolated credentials, data, logs, and reports:

```
profiles/
└── rex/
    ├── credentials/
    │   ├── credentials.json      # Google OAuth client secret
    │   ├── token.json            # Gmail readonly token
    │   ├── token_import.json     # Gmail import token
    │   └── .fastmail_token       # Fastmail API token
    ├── data/
    │   ├── migration_state.db    # Migration state (resume support)
    │   └── migration_cache.db    # Email cache (verify performance)
    ├── logs/
    │   └── migration_*.log
    └── reports/
        ├── missing_from_google.csv
        ├── migration_results_*.csv
        └── migration_failures_*.csv
```

Create a new profile for another user:
```bash
uv run fm-setup --profile roxy
```

---

## Project Structure

```
fastmail-to-gmail/
├── src/
│   └── fastmail_to_gmail/
│       ├── __init__.py             # Package init
│       ├── profile.py              # Profile path resolver
│       ├── setup_profile.py        # Profile setup wizard (fm-setup)
│       ├── verify.py               # Verification script (fm-verify)
│       ├── migrate.py              # Migration script (fm-migrate)
│       ├── check_status.py         # Status monitor (fm-status)
│       ├── fastmail_client.py      # Fastmail JMAP client
│       ├── gmail_client.py         # Gmail API client (readonly)
│       ├── fastmail_fetcher.py     # Fetch raw .eml from Fastmail
│       ├── gmail_importer.py       # Upload to Gmail via import API
│       ├── comparator.py           # Matching and comparison logic
│       ├── report_generator.py     # Console and CSV output
│       ├── label_mapper.py         # Folder → label mapping
│       └── migration_tracker.py    # SQLite state tracking
│
├── profiles/                       # Per-user profiles (gitignored)
│
├── pyproject.toml                  # Project config & dependencies
├── .python-version                 # Python 3.12
├── .gitattributes
├── .gitignore
└── README.md
```
