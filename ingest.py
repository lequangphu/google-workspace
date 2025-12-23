"""Ingest Google Sheets data to raw CSV files."""

import csv
import logging
import os
import re
import sys
from datetime import datetime, timezone

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# Shared folder containing "XUẤT NHẬP TỒN TỔNG T*" files
SHARED_FOLDER_ID = "16CXAGzxxoBU8Ui1lXPxZoLVbDdsgwToj"

# Desired sheet tabs to export
DESIRED_TABS = ["CT.NHAP", "CT.XUAT", "XNT"]

# Output directory for raw CSV files
RAW_DATA_DIR = "data/raw"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)


def authenticate_google():
    """Authenticate with Google API using OAuth2."""
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())
    return creds


def connect_to_drive():
    """Connect to Google Drive and Sheets APIs."""
    creds = authenticate_google()
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service


def find_year_folders(drive_service):
    """Find all year folders in Google Drive."""
    query = (
        "name contains 'TỔNG HỢP 202' and mimeType='application/vnd.google-apps.folder'"
    )
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    folders = results.get("files", [])
    return {folder["name"]: folder["id"] for folder in folders}


def find_sheets_in_folder(drive_service, folder_id):
     """Find all Google Sheets in a folder."""
     query = f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.spreadsheet'"
     try:
         results = drive_service.files().list(q=query, fields="files(id, name, modifiedTime)").execute()
         sheets = results.get("files", [])
         return sheets
     except HttpError as e:
         logging.error(f"Failed to list sheets in folder {folder_id}: {e}")
         return []


def get_sheet_tabs(sheets_service, spreadsheet_id):
     """Get all sheet tab names from a spreadsheet."""
     try:
         result = (
             sheets_service.spreadsheets()
             .get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title")
             .execute()
         )
         tabs = [sheet["properties"]["title"] for sheet in result["sheets"]]
         return tabs
     except HttpError as e:
         logging.error(f"Failed to get tabs from spreadsheet {spreadsheet_id}: {e}")
         return []


def read_sheet_data(sheets_service, spreadsheet_id, sheet_name):
     """Read data from a sheet tab."""
     try:
         result = (
             sheets_service.spreadsheets()
             .values()
             .get(
                 spreadsheetId=spreadsheet_id,
                 range=sheet_name,
                 valueRenderOption="UNFORMATTED_VALUE",
             )
             .execute()
         )
         values = result.get("values", [])
         return values
     except HttpError as e:
         logging.error(f"Failed to read sheet {sheet_name} from {spreadsheet_id}: {e}")
         return []


def export_tab_to_csv(sheets_service, spreadsheet_id, sheet_name, csv_path):
     """Export a sheet tab to a CSV file."""
     values = read_sheet_data(sheets_service, spreadsheet_id, sheet_name)
     if not values:
         logging.warning(f"No data to export for {sheet_name}")
         return False
     try:
         with open(csv_path, "w", newline="", encoding="utf-8") as f:
             writer = csv.writer(f)
             writer.writerows(values)
         return True
     except IOError as e:
         logging.error(f"Failed to write CSV {csv_path}: {e}")
         return False


def parse_file_metadata(file_name):
     """
     Extract year and month from filename.
 
     Example: "XUẤT NHẬP TỒN TỔNG T01.23" -> year=2023, month=1
     """
     match = re.search(r"T(\d+)\.(\d+)$", file_name)
     if match:
         month = int(match.group(1))
         year = 2000 + int(match.group(2))
         return year, month
     return None, None


def should_ingest_file(csv_path, remote_modified_time):
     """
     Check if remote file is newer than local CSV.
 
     Args:
         csv_path: Path to local CSV file
         remote_modified_time: ISO 8601 timestamp from Google Drive (e.g., "2025-12-23T10:30:00.000Z")
 
     Returns:
         True if file doesn't exist or remote is newer, False otherwise
     """
     if not os.path.exists(csv_path):
         return True  # File doesn't exist, ingest it
 
     try:
         # Get local CSV modification time (UTC)
         local_mtime = os.path.getmtime(csv_path)
         local_dt = datetime.fromtimestamp(local_mtime, tz=timezone.utc)
 
         # Parse Google Drive's ISO 8601 timestamp
         remote_dt = datetime.fromisoformat(remote_modified_time.replace('Z', '+00:00'))
 
         # Ingest if remote is newer
         should_ingest = remote_dt > local_dt
         if not should_ingest:
             logging.debug(f"Skipped {csv_path} (local is up-to-date)")
         return should_ingest
 
     except Exception as e:
         logging.warning(f"Could not compare timestamps for {csv_path}: {e}")
         return False  # Play it safe, don't re-ingest on error


def ingest_from_drive(test_mode=False, clean_up=False):
     """
     Download Google Sheets from Drive and export to data/raw/ directory.
 
     Args:
         test_mode: If True, stop after downloading one of each tab type
         clean_up: If True, remove existing data/raw/ directory before ingesting
 
     Returns:
         Number of files ingested
     """
     import shutil
 
     logging.info("Connecting to Google Drive...")
     try:
         drive_service, sheets_service = connect_to_drive()
     except Exception as e:
         logging.error(f"Failed to connect to Google Drive: {e}")
         return 0
 
     logging.info("Connected successfully!")
 
     # Find year folders
     year_folders = find_year_folders(drive_service)
     if not year_folders:
         logging.warning("No year folders found")
     else:
         logging.info(f"Found {len(year_folders)} year folders")
 
     # Optionally clear and recreate raw directory
     if clean_up and os.path.exists(RAW_DATA_DIR):
         shutil.rmtree(RAW_DATA_DIR)
         logging.info(f"Cleared {RAW_DATA_DIR}/")
 
     os.makedirs(RAW_DATA_DIR, exist_ok=True)
 
     tabs_processed = set()
     files_ingested = 0
 
     # Helper function to process sheets from a folder
     def process_sheets_from_folder(folder_id, source_name):
         nonlocal tabs_processed, files_ingested
         
         sheets = find_sheets_in_folder(drive_service, folder_id)
         if not sheets:
             logging.debug(f"No sheets found in {source_name}")
             return False
 
         for sheet in sheets:
             file_name = sheet["name"]
             file_id = sheet["id"]
             remote_modified_time = sheet.get("modifiedTime")
             year_num, month = parse_file_metadata(file_name)
             if year_num is None or month is None:
                 logging.debug(f"Skipping {file_name}: invalid metadata")
                 continue
         
             tabs = get_sheet_tabs(sheets_service, file_id)
             if not tabs:
                 logging.warning(f"No tabs found in {file_name}")
                 continue
         
             for tab in set(tabs) & set(DESIRED_TABS):
                 csv_path = f"{RAW_DATA_DIR}/{year_num}_{month}_{tab}.csv"
                 
                 # Check if file needs to be ingested based on timestamp
                 if not should_ingest_file(csv_path, remote_modified_time):
                     continue
                 
                 if export_tab_to_csv(sheets_service, file_id, tab, csv_path):
                     logging.info(f"Exported {csv_path}")
                     tabs_processed.add(tab)
                     files_ingested += 1
         
                     if test_mode and len(tabs_processed) >= len(DESIRED_TABS):
                         logging.info(f"Test mode: downloaded one of each tab type {sorted(tabs_processed)}")
                         return True
         
         return False
 
     # Process year folders
     for year_name, folder_id in year_folders.items():
         if process_sheets_from_folder(folder_id, year_name):
             return files_ingested
     
     # Process shared folder
     logging.info("Processing shared folder...")
     if process_sheets_from_folder(SHARED_FOLDER_ID, "shared"):
         return files_ingested
 
     # Print summary
     logging.info(f"Ingestion complete: {files_ingested} files, {len(tabs_processed)} tab types")
     if tabs_processed:
         logging.info(f"Tab types: {sorted(tabs_processed)}")
 
     return files_ingested


if __name__ == "__main__":
     ingest_from_drive(test_mode=False, clean_up=False)
