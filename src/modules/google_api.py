"""Google API utilities for Drive and Sheets services."""

import csv
import functools
import logging
import re
import socket
import ssl
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict

import pandas as pd

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

WORKSPACE_ROOT = Path(__file__).parent.parent.parent


class SheetMetadata(TypedDict):
    """Sheet info from Google Drive API."""

    id: str
    name: str
    modifiedTime: str


# Rate limiting: 60 requests per minute per user = 1 request per second max
# Use 0.5s to stay safely under the limit while being faster
API_CALL_DELAY = 0.5  # seconds


def retry_api_call(func):
    """Decorator for retrying Google API calls with exponential backoff.

    Retries on transient errors (429 rate limit, 500-504 server errors,
    timeouts, connection errors) but not on permanent errors (400, 401, 403, 404).

    Max retries: 3 with exponential backoff (2^attempt seconds: 1, 2, 4).

    Args:
        func: Function to decorate.

    Returns:
        Decorated function that retries on transient failures.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        for attempt in range(3):  # Max 3 retries
            try:
                return func(*args, **kwargs)
            except HttpError as e:
                # Don't retry on permanent errors (client errors)
                if e.status_code in (400, 401, 403, 404):
                    raise  # Permanent error, don't retry
                # Retry on rate limit (429) or server errors (500-504)
                if e.status_code in (429, 500, 501, 502, 503, 504):
                    if attempt < 2:  # Don't wait after last attempt
                        wait_time = 2**attempt
                        logger.warning(
                            f"API error {e.status_code} on {func.__name__}, "
                            f"retrying in {wait_time}s (attempt {attempt + 1}/3)"
                        )
                        time.sleep(wait_time)
                        continue
                raise  # Other HTTP errors or final attempt
            except (
                TimeoutError,
                socket.timeout,
                ssl.SSLError,
                ConnectionResetError,
                OSError,
            ) as e:
                if attempt < 2:
                    wait_time = 2**attempt
                    logger.warning(
                        f"Network error on {func.__name__}, "
                        f"retrying in {wait_time}s (attempt {attempt + 1}/3): {e}"
                    )
                    time.sleep(wait_time)
                    continue
                raise  # Final attempt
        return None  # Should never reach here

    return wrapper


# OAuth2 scopes for Google Drive and Sheets APIs
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


def authenticate_google():
    """Authenticate with Google API using OAuth2.

    Returns:
        Credentials object for Google API calls.
    """
    credentials_path = WORKSPACE_ROOT / "credentials.json"
    token_path = WORKSPACE_ROOT / "token.json"

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


@retry_api_call
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


@retry_api_call
def find_spreadsheet_in_folder(drive_service, folder_id, spreadsheet_name):
    """Find a specific spreadsheet by name within a folder.

    Args:
        drive_service: Google Drive API service object.
        folder_id: ID of the parent folder.
        spreadsheet_name: Name of the spreadsheet to find (with or without .xlsx extension).

    Returns:
        Spreadsheet ID if found, None otherwise.
    """
    # Handle both with and without .xlsx extension
    if not spreadsheet_name.endswith(".xlsx"):
        spreadsheet_name = f"{spreadsheet_name}.xlsx"

    query = (
        f"'{folder_id}' in parents "
        f"and name = '{spreadsheet_name}' "
        "and mimeType='application/vnd.google-apps.spreadsheet' "
        "and trashed=false"
    )

    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    time.sleep(API_CALL_DELAY)

    files = results.get("files", [])
    if files:
        return files[0]["id"]
    return None


@retry_api_call
def find_subfolder_in_folder(drive_service, folder_id, subfolder_name):
    """Find a specific subfolder by name within a folder.

    Args:
        drive_service: Google Drive API service object.
        folder_id: ID of the parent folder.
        subfolder_name: Name of the subfolder to find.

    Returns:
        Folder ID if found, None otherwise.
    """
    query = (
        f"'{folder_id}' in parents "
        f"and name = '{subfolder_name}' "
        "and mimeType='application/vnd.google-apps.folder' "
        "and trashed=false"
    )

    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    time.sleep(API_CALL_DELAY)

    folders = results.get("files", [])
    if folders:
        return folders[0]["id"]
    return None


@retry_api_call
def find_year_folders_in_receipts_folder(drive_service, receipts_folder_id):
    """Find all year folders within the Import Export Receipts folder.

    Args:
        drive_service: Google Drive API service object.
        receipts_folder_id: ID of the Import Export Receipts folder.

    Returns:
        Dict of {year: folder_id} for year folders (2020, 2021, etc.).
    """
    query = (
        f"'{receipts_folder_id}' in parents "
        "and name contains '202' "  # Match folders containing years starting with 202
        "and mimeType='application/vnd.google-apps.folder' "
        "and trashed=false"
    )

    results = (
        drive_service.files()
        .list(
            q=query,
            fields="files(id, name)",
            orderBy="name",  # Sort by name to get chronological order
        )
        .execute()
    )
    time.sleep(API_CALL_DELAY)

    year_folders = {}
    for folder in results.get("files", []):
        folder_name = folder["name"]
        # Extract year from folder names like "Xuất Nhập Tồn 2020"
        import re

        year_match = re.search(r"(\d{4})", folder_name)
        if year_match:
            try:
                year = int(year_match.group(1))
                if 2020 <= year <= 2030:  # Reasonable year range
                    year_folders[year] = folder["id"]
            except ValueError:
                continue  # Skip folders that aren't valid years

    return year_folders


def find_receipt_sheets(
    drive_service,
    source_config: Dict[str, str],
    year_list: Optional[List[int]] = None,
    month_list: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """Discover import/export receipts spreadsheets with filtering.

    This is the single source of truth for finding monthly receipts
    spreadsheets. Used by both ingest.py and check_reconciliation_discrepancies.py.

    Args:
        drive_service: Google Drive API service object.
        source_config: Dict with 'root_folder_id' and 'receipts_subfolder_name'
            from pipeline.toml sources section.
        year_list: Optional list of years to filter (e.g., [2024, 2025]).
            If None, all years are included.
        month_list: Optional list of months to filter (e.g., [1, 2, 12]).
            If None, all months are included.

    Returns:
        List of sheet dicts with 'id', 'name', 'modifiedTime', 'year', 'month' keys.
        Sorted by (year, month).

    Raises:
        FileNotFoundError: If receipts subfolder is not found.
    """
    root_folder_id = source_config["root_folder_id"]
    receipts_subfolder_name = source_config["receipts_subfolder_name"]

    # Step 1: Find receipts subfolder
    receipts_folder_id = find_subfolder_in_folder(
        drive_service, root_folder_id, receipts_subfolder_name
    )
    if not receipts_folder_id:
        raise FileNotFoundError(
            f"Could not find '{receipts_subfolder_name}' in root folder {root_folder_id}"
        )

    # Step 2: Discover year folders
    year_folders = find_year_folders_in_receipts_folder(
        drive_service, receipts_folder_id
    )
    if not year_folders:
        return []

    # Step 3: Collect all sheets from all year folders with filtering
    all_sheets = []
    for year_num, year_folder_id in sorted(year_folders.items()):
        # Skip year if not in filter
        if year_list is not None and year_num not in year_list:
            continue

        # Get sheets from this year folder
        sheets = find_sheets_in_folder(drive_service, year_folder_id)

        for sheet in sheets:
            file_name = sheet["name"]

            # Parse year/month from filename
            parsed_year, parsed_month = parse_file_metadata(file_name)

            # Skip if metadata invalid
            if parsed_year is None or parsed_month is None:
                continue

            # Skip month if not in filter
            if month_list is not None and parsed_month not in month_list:
                continue

            # Enrich sheet with parsed metadata
            sheet["year"] = parsed_year
            sheet["month"] = parsed_month
            all_sheets.append(sheet)

    # Sort by year, month
    all_sheets.sort(key=lambda s: (s["year"], s["month"]))

    return all_sheets


@retry_api_call
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


@retry_api_call
def get_sheet_tabs(sheets_service, spreadsheet_id):
    """Get all sheet tab names from a spreadsheet.

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of the spreadsheet.

    Returns:
        List of tab names.
    """
    result = (
        sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title")
        .execute()
    )
    time.sleep(API_CALL_DELAY)
    return [sheet["properties"]["title"] for sheet in result.get("sheets", [])]


@retry_api_call
def get_sheet_id_by_name(sheets_service, spreadsheet_id, sheet_name):
    """Get sheet ID by sheet name.

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of spreadsheet.
        sheet_name: Name of the sheet tab.

    Returns:
        Sheet ID if found, None otherwise.
    """
    result = (
        sheets_service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets.properties(sheetId,title)",
        )
        .execute()
    )
    time.sleep(API_CALL_DELAY)
    for sheet in result.get("sheets", []):
        if sheet["properties"]["title"] == sheet_name:
            return sheet["properties"]["sheetId"]
    return None


@retry_api_call
def get_sheet_name_by_id(sheets_service, spreadsheet_id, sheet_id):
    """Get sheet name by sheet ID.

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of spreadsheet.
        sheet_id: ID of sheet tab.

    Returns:
        Sheet name if found, None otherwise.
    """
    result = (
        sheets_service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets.properties(sheetId,title)",
        )
        .execute()
    )
    time.sleep(API_CALL_DELAY)
    for sheet in result.get("sheets", []):
        if sheet["properties"]["sheetId"] == sheet_id:
            return sheet["properties"]["title"]
    return None


@retry_api_call
def read_sheet_data(sheets_service, spreadsheet_id, sheet_name):
    """Read data from a sheet tab.

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of the spreadsheet.
        sheet_name: Name of the sheet tab.

    Returns:
        List of rows (lists of values).
    """
    # Quote sheet name for proper parsing (handles spaces and special characters)
    # Use just sheet name in quotes - API will read all data
    quoted_range = f"'{sheet_name}'"

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


@retry_api_call
def write_sheet_data(
    sheets_service, spreadsheet_id: str, sheet_name: str, values: list
) -> bool:
    """Write data to a sheet tab.

    Creates the sheet if it doesn't exist. Clears existing content before writing.

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of the spreadsheet.
        sheet_name: Name of the sheet tab.
        values: List of rows (lists of values) to write.

    Returns:
        True if successful, False otherwise.
    """
    spreadsheet = (
        sheets_service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute()
    )

    existing_sheet_id = None
    for sheet in spreadsheet.get("sheets", []):
        if sheet["properties"]["title"] == sheet_name:
            existing_sheet_id = sheet["properties"]["sheetId"]
            break

    if existing_sheet_id is not None:
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A:Z",
        ).execute()
    else:
        request = {"addSheet": {"properties": {"title": sheet_name}}}
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [request]}
        ).execute()

    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()

    logger.info(f"Successfully wrote {len(values)} rows to '{sheet_name}'")
    return True


@retry_api_call
def upload_dataframe_to_sheet(
    sheets_service,
    spreadsheet_id: str,
    sheet_name: str,
    df: pd.DataFrame,
    raw_columns: list[int] = None,
    move_to_first: bool = True,
) -> bool:
    """Upload DataFrame to Google Sheet with optional raw column handling.

    Creates sheet if it doesn't exist. Clears existing content before writing.
    Specified columns (raw_columns) are uploaded with valueInputOption="RAW"
    to preserve formatting (e.g., leading zeros in product codes).
    All other columns use valueInputOption="USER_ENTERED" for formula parsing.

    By default, creates or moves the sheet to index 0 (first position).

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of the spreadsheet.
        sheet_name: Name of the sheet tab.
        df: pandas DataFrame to upload.
        raw_columns: Optional list of column indices (0-based) to upload as RAW.
                    e.g., [0] for first column, [1] for "Mã khách hàng".
        move_to_first: If True, create new sheets at index 0 or move existing
                      sheets to index 0. Default True.

    Returns:
        True if successful, False otherwise.
    """
    spreadsheet = (
        sheets_service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title,index))",
        )
        .execute()
    )

    existing_sheet_id = None
    existing_sheet_index = None
    for sheet in spreadsheet.get("sheets", []):
        if sheet["properties"]["title"] == sheet_name:
            existing_sheet_id = sheet["properties"]["sheetId"]
            existing_sheet_index = sheet["properties"]["index"]
            break

    if existing_sheet_id is not None:
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A:Z",
        ).execute()

        if move_to_first and existing_sheet_index != 0:
            move_request = {
                "updateSheetProperties": {
                    "properties": {"sheetId": existing_sheet_id, "index": 0},
                    "fields": "index",
                }
            }
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id, body={"requests": [move_request]}
            ).execute()
            time.sleep(API_CALL_DELAY)
    else:
        new_sheet_props = {"title": sheet_name}
        if move_to_first:
            new_sheet_props["index"] = 0
        request = {"addSheet": {"properties": new_sheet_props}}
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [request]}
        ).execute()
        time.sleep(API_CALL_DELAY)

    if raw_columns:
        for raw_col_idx in raw_columns:
            col_letter = chr(65 + raw_col_idx)
            col_name = df.columns[raw_col_idx]
            col_data = df.iloc[:, raw_col_idx].astype(str).tolist()
            # Prepend header row
            col_values = [[col_name]] + [[v] for v in col_data]

            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"'{sheet_name}'!{col_letter}1:{col_letter}",
                valueInputOption="RAW",
                body={"values": col_values},
            ).execute()
            time.sleep(API_CALL_DELAY)

        non_raw_cols = [i for i in range(len(df.columns)) if i not in raw_columns]
        if non_raw_cols:
            for non_raw_col_idx in non_raw_cols:
                col_letter = chr(65 + non_raw_col_idx)
                col_name = df.columns[non_raw_col_idx]
                col_series = df.iloc[:, non_raw_col_idx]

                is_numeric = pd.api.types.is_numeric_dtype(col_series)

                if is_numeric:
                    col_data = col_series.fillna(0).tolist()
                else:
                    col_data = (
                        col_series.astype(object)
                        .infer_objects(copy=False)
                        .fillna("")
                        .tolist()
                    )

                col_values = [[col_name]] + [[v] for v in col_data]

                sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"'{sheet_name}'!{col_letter}1:{col_letter}",
                    valueInputOption="USER_ENTERED",
                    body={"values": col_values},
                ).execute()
                time.sleep(API_CALL_DELAY)
    else:
        values = [df.columns.tolist()] + df.astype(object).infer_objects(
            copy=False
        ).fillna("").values.tolist()

        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()

    logger.info(f"Successfully uploaded {len(df)} rows to '{sheet_name}'")
    if raw_columns:
        logger.info(f"  Raw columns (preserved): {raw_columns}")
    return True


@retry_api_call
def copy_sheet_to_spreadsheet(
    sheets_service,
    source_spreadsheet_id: str,
    source_sheet_id: int,
    destination_spreadsheet_id: str,
    move_to_first: bool = True,
) -> Optional[dict]:
    """Copy a sheet from one spreadsheet to another, preserving formulas.

    Uses the spreadsheets.sheets.copyTo API method which copies all sheet
    properties including formulas, formatting, and conditional formatting.

    Args:
        sheets_service: Google Sheets API service object.
        source_spreadsheet_id: ID of the source spreadsheet.
        source_sheet_id: ID of the sheet to copy.
        destination_spreadsheet_id: ID of the destination spreadsheet.
        move_to_first: If True, move the copied sheet to the first position.

    Returns:
        Sheet properties of the newly created sheet if successful, None otherwise.
    """
    copy_request = {"destinationSpreadsheetId": destination_spreadsheet_id}

    result = (
        sheets_service.spreadsheets()
        .sheets()
        .copyTo(
            spreadsheetId=source_spreadsheet_id,
            sheetId=source_sheet_id,
            body=copy_request,
        )
        .execute()
    )
    time.sleep(API_CALL_DELAY)

    new_sheet_id = result["sheetId"]
    logger.info(
        f"Copied sheet {source_sheet_id} from {source_spreadsheet_id} "
        f"to {destination_spreadsheet_id} as new sheet {new_sheet_id}"
    )

    if move_to_first:
        update_request = {
            "updateSheetProperties": {
                "properties": {"sheetId": new_sheet_id, "index": 0},
                "fields": "index",
            }
        }
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=destination_spreadsheet_id,
            body={"requests": [update_request]},
        ).execute()
        time.sleep(API_CALL_DELAY)
        logger.info(f"Moved sheet {new_sheet_id} to first position")

    return result


@retry_api_call
def rename_sheet(
    sheets_service, spreadsheet_id: str, sheet_id: int, new_name: str
) -> bool:
    """Rename a sheet tab.

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of the spreadsheet.
        sheet_id: ID of the sheet to rename.
        new_name: New name for the sheet.

    Returns:
        True if successful, False otherwise.
    """
    try:
        update_request = {
            "updateSheetProperties": {
                "properties": {"sheetId": sheet_id, "title": new_name},
                "fields": "title",
            }
        }
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [update_request]}
        ).execute()
        time.sleep(API_CALL_DELAY)
        logger.info(f"Renamed sheet {sheet_id} to '{new_name}'")
        return True
    except HttpError as e:
        logger.error(f"Failed to rename sheet {sheet_id} to '{new_name}': {e}")
        return False


@retry_api_call
def delete_sheet(sheets_service, spreadsheet_id: str, sheet_id: int) -> bool:
    """Delete a sheet tab.

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of the spreadsheet.
        sheet_id: ID of the sheet to delete.

    Returns:
        True if successful, False otherwise.
    """
    try:
        update_request = {"deleteSheet": {"sheetId": sheet_id}}
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [update_request]}
        ).execute()
        time.sleep(API_CALL_DELAY)
        logger.info(f"Deleted sheet {sheet_id}")
        return True
    except HttpError as e:
        logger.error(f"Failed to delete sheet {sheet_id}: {e}")
        return False


@retry_api_call
def export_tab_to_csv(
    sheets_service, spreadsheet_id: str, sheet_name: str, csv_path: Path
) -> bool:
    """Export a sheet tab to a CSV file.

    Reads sheet data and writes to CSV with logging. Handles directory creation.

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
            csv.writer(f).writerows(values)
        logger.info(f"Exported {csv_path}")
        return True
    except IOError as e:
        logger.error(f"Failed to write CSV {csv_path}: {e}")
        return False


def parse_file_metadata(file_name: str) -> tuple:
    """Extract year and month from filename.

    Args:
        file_name: Filename like "Xuất Nhập Tồn 2025-01" or legacy "XUẤT NHẬP TỒN TỔNG T01.23".

    Returns:
        Tuple of (year, month) or (None, None) if parsing fails.
    """
    # Try new format first: "Xuất Nhập Tồn 2025-01"
    match = re.search(r"(\d{4})-(\d{1,2})$", file_name)
    if match:
        year = int(match.group(1))
        month = int(match.group(2))
        return year, month

    # Fall back to legacy format: "XUẤT NHẬP TỒN TỔNG T01.23"
    match = re.search(r"T(\d+)\.(\d+)$", file_name)
    if match:
        month = int(match.group(1))
        year = 2000 + int(match.group(2))
        return year, month

    return None, None


def validate_years(years_str: str) -> List[str]:
    """Validate and parse comma-separated year list.

    Args:
        years_str: Comma-separated string of years (e.g., "2024,2025")

    Returns:
        List of validated 4-digit year strings (sorted)

    Raises:
        ValueError: If any year is not a valid 4-digit year
    """
    years = [y.strip() for y in years_str.split(",")]
    for year in years:
        if not year.isdigit() or len(year) != 4:
            raise ValueError(
                f"Invalid year '{year}'. Years must be 4-digit numbers (e.g., 2024, 2025)"
            )
    return sorted(years)


def validate_months(months_str: str) -> List[str]:
    """Validate and parse comma-separated month list.

    Args:
        months_str: Comma-separated string of months (e.g., "1,2,12" or "01,02")

    Returns:
        List of validated 2-digit month strings (e.g., "01", "02", "12") (sorted)

    Raises:
        ValueError: If any month is not a valid month (1-12)
    """
    months = [m.strip() for m in months_str.split(",")]
    validated_months = []
    for month in months:
        if not month.isdigit():
            raise ValueError(
                f"Invalid month '{month}'. Months must be numbers (e.g., 1, 2, 12)"
            )
        month_num = int(month)
        if month_num < 1 or month_num > 12:
            raise ValueError(
                f"Invalid month '{month}'. Months must be between 1 and 12 (e.g., 1, 2, 12)"
            )
        validated_months.append(f"{month_num:02d}")
    return sorted(validated_months)
