"""Google API utilities for Drive and Sheets services."""

import csv
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

# Manifest cache file location
MANIFEST_PATH = Path("data/.drive_manifest.json")
MANIFEST_CACHE_TTL_HOURS = 24  # Refresh folders older than 24 hours

# Rate limiting: 60 requests per minute per user = 1 request per second max
# Use 0.5s to stay safely under the limit while being faster
API_CALL_DELAY = 0.5  # seconds

# OAuth2 scopes for Google Drive and Sheets APIs
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


def load_manifest() -> dict:
    """Load folder→sheets manifest from cache.

    Returns:
        Dict with structure: {"folders": {folder_id: {"scanned_at": iso_ts, "sheets": [...]}}}.
    """
    if MANIFEST_PATH.exists():
        try:
            with open(MANIFEST_PATH, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load manifest: {e}")
    return {"version": 1, "folders": {}}


def save_manifest(manifest: dict) -> None:
    """Save manifest to cache file.

    Args:
        manifest: Manifest dict to save.
    """
    try:
        MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(MANIFEST_PATH, "w") as f:
            json.dump(manifest, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not save manifest: {e}")


def is_manifest_stale(scanned_at_iso: str) -> bool:
    """Check if folder scan is older than cache TTL.

    Args:
        scanned_at_iso: ISO timestamp string from manifest.

    Returns:
        True if stale (should refresh), False if fresh.
    """
    try:
        scanned_at = datetime.fromisoformat(scanned_at_iso.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        age_hours = (now - scanned_at).total_seconds() / 3600
        return age_hours > MANIFEST_CACHE_TTL_HOURS
    except Exception:
        return True  # If unparseable, treat as stale


def get_cached_sheets_for_folder(manifest: dict, folder_id: str) -> tuple:
    """Get cached sheets for a folder if fresh, else None.

    Args:
        manifest: Manifest dict.
        folder_id: ID of folder to query.

    Returns:
        Tuple of (sheets_list, is_fresh) or (None, False) if not cached/stale.
    """
    if folder_id not in manifest.get("folders", {}):
        return None, False

    folder_entry = manifest["folders"][folder_id]
    if is_manifest_stale(folder_entry.get("scanned_at", "")):
        return None, False

    return folder_entry.get("sheets", []), True


def update_manifest_for_folder(manifest: dict, folder_id: str, sheets: list) -> None:
    """Update manifest with freshly scanned folder data.

    Args:
        manifest: Manifest dict to update (modified in place).
        folder_id: ID of folder.
        sheets: List of sheet dicts (id, name, modifiedTime).
    """
    if "folders" not in manifest:
        manifest["folders"] = {}

    manifest["folders"][folder_id] = {
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "sheets": sheets,
    }


def clear_manifest() -> None:
    """Clear entire manifest cache (force full refresh on next run)."""
    if MANIFEST_PATH.exists():
        MANIFEST_PATH.unlink()
        logger.info(f"Cleared manifest cache at {MANIFEST_PATH}")


def authenticate_google():
    """Authenticate with Google API using OAuth2.

    Returns:
        Credentials object for Google API calls.
    """
    credentials_path = Path("credentials.json")
    token_path = Path("token.json")

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not credentials_path.exists():
                raise FileNotFoundError(
                    "credentials.json not found. "
                    "Please download it from Google Cloud Console."
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                str(credentials_path), SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open(token_path, "w") as token_file:
            token_file.write(creds.to_json())

    return creds


def connect_to_drive():
    """Connect to Google Drive and Sheets APIs.

    Returns:
        Tuple of (drive_service, sheets_service).
    """
    creds = authenticate_google()
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service


def find_year_folders(drive_service):
    """Find all year folders in Google Drive.

    Args:
        drive_service: Google Drive API service object.

    Returns:
        Dict of {folder_name: folder_id}.
    """
    query = (
        "name contains 'TỔNG HỢP 202' and mimeType='application/vnd.google-apps.folder'"
    )
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    time.sleep(API_CALL_DELAY)
    folders = results.get("files", [])
    return {folder["name"]: folder["id"] for folder in folders}


def find_sheets_in_folder(drive_service, folder_id):
    """Find all Google Sheets in a folder.

    Args:
        drive_service: Google Drive API service object.
        folder_id: ID of the folder to search.

    Returns:
        List of sheet metadata dicts (id, name, modifiedTime).
    """
    query = (
        f"'{folder_id}' in parents "
        "and mimeType='application/vnd.google-apps.spreadsheet' "
        "and trashed=false"
    )
    try:
        results = (
            drive_service.files()
            .list(q=query, fields="files(id, name, modifiedTime)")
            .execute()
        )
        time.sleep(API_CALL_DELAY)
        return results.get("files", [])
    except HttpError as e:
        logger.error(f"Failed to list sheets in folder {folder_id}: {e}")
        return []


def get_sheet_tabs(sheets_service, spreadsheet_id):
    """Get all sheet tab names from a spreadsheet.

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of the spreadsheet.

    Returns:
        List of tab names.
    """
    try:
        result = (
            sheets_service.spreadsheets()
            .get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title")
            .execute()
        )
        time.sleep(API_CALL_DELAY)
        return [sheet["properties"]["title"] for sheet in result.get("sheets", [])]
    except HttpError as e:
        logger.error(f"Failed to get tabs from spreadsheet {spreadsheet_id}: {e}")
        return []


def read_sheet_data(sheets_service, spreadsheet_id, sheet_name):
    """Read data from a sheet tab with retry logic.

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of the spreadsheet.
        sheet_name: Name of the sheet tab.

    Returns:
        List of rows (lists of values).
    """
    max_retries = 3
    # Quote sheet name for proper parsing (handles spaces and special characters)
    # Use just sheet name in quotes - API will read all data
    quoted_range = f"'{sheet_name}'"

    for attempt in range(max_retries):
        try:
            result = (
                sheets_service.spreadsheets()
                .values()
                .get(
                    spreadsheetId=spreadsheet_id,
                    range=quoted_range,
                    valueRenderOption="UNFORMATTED_VALUE",
                )
                .execute()
            )
            time.sleep(API_CALL_DELAY)
            return result.get("values", [])
        except HttpError as e:
            if e.resp.status == 429:  # Rate limited
                wait_time = 2**attempt  # 1s, 2s, 4s...
                logger.warning(
                    f"Rate limited reading {sheet_name}, retrying in {wait_time}s "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait_time)
            else:
                logger.error(
                    f"Failed to read sheet {sheet_name} from {spreadsheet_id}: {e}"
                )
                return []

    logger.error(f"Failed to read sheet {sheet_name} after {max_retries} retries")
    return []


def export_tab_to_csv(
    sheets_service, spreadsheet_id: str, sheet_name: str, csv_path: Path
) -> bool:
    """Export a sheet tab to a CSV file.

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of the spreadsheet.
        sheet_name: Name of the sheet tab.
        csv_path: Path object for output CSV file.

    Returns:
        True if successful, False otherwise.
    """
    values = read_sheet_data(sheets_service, spreadsheet_id, sheet_name)
    if not values:
        logger.warning(f"No data to export for {sheet_name}")
        return False

    try:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(values)
        return True
    except IOError as e:
        logger.error(f"Failed to write CSV {csv_path}: {e}")
        return False


def parse_file_metadata(file_name: str) -> tuple:
    """Extract year and month from filename.

    Args:
        file_name: Filename like "XUẤT NHẬP TỒN TỔNG T01.23".

    Returns:
        Tuple of (year, month) or (None, None) if parsing fails.
    """
    match = re.search(r"T(\d+)\.(\d+)$", file_name)
    if match:
        month = int(match.group(1))
        year = 2000 + int(match.group(2))
        return year, month
    return None, None


def should_ingest_file(csv_path: Path, remote_modified_time: str) -> bool:
    """Check if remote file is newer than local CSV.

    Args:
        csv_path: Path to local CSV file.
        remote_modified_time: ISO 8601 timestamp from Google Drive.

    Returns:
        True if file doesn't exist or remote is newer, False otherwise.
    """
    if not csv_path.exists():
        return True

    try:
        local_mtime = csv_path.stat().st_mtime
        local_dt = datetime.fromtimestamp(local_mtime, tz=timezone.utc)
        remote_dt = datetime.fromisoformat(remote_modified_time.replace("Z", "+00:00"))
        should_ingest = remote_dt > local_dt
        if not should_ingest:
            logger.debug(f"Skipped {csv_path} (local is up-to-date)")
        return should_ingest
    except Exception as e:
        logger.warning(f"Could not compare timestamps for {csv_path}: {e}")
        return False


def should_ingest_import_export(
    csv_path: Path, remote_modified_time: str, current_month: int, current_year: int
) -> bool:
    """Check if import/export file should be re-ingested.

    Always re-ingest current month files. For other months, check if remote is newer.

    Args:
        csv_path: Path to local CSV file.
        remote_modified_time: ISO 8601 timestamp from Google Drive.
        current_month: Current month (1-12).
        current_year: Current year (e.g., 2025).

    Returns:
        True if file should be re-ingested, False otherwise.
    """
    # Extract year and month from filename (e.g., "2025_01_CT.NHAP.csv")
    try:
        stem = csv_path.stem  # Remove .csv extension
        parts = stem.split("_")
        if len(parts) < 2:
            return True

        file_year = int(parts[0])
        file_month = int(parts[1])

        # Always re-ingest current month
        if file_year == current_year and file_month == current_month:
            logger.debug(f"Always re-ingest current month: {csv_path}")
            return True

        # For other months, check timestamp
        return should_ingest_file(csv_path, remote_modified_time)
    except Exception as e:
        logger.warning(f"Could not parse filename {csv_path}: {e}")
        return should_ingest_file(csv_path, remote_modified_time)
