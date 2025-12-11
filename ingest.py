"""Ingest Google Sheets data to raw CSV files."""

import csv
import os
import re

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


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
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    sheets = results.get("files", [])
    return sheets


def get_sheet_tabs(sheets_service, spreadsheet_id):
    """Get all sheet tab names from a spreadsheet."""
    result = (
        sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title")
        .execute()
    )
    tabs = [sheet["properties"]["title"] for sheet in result["sheets"]]
    return tabs


def read_sheet_data(sheets_service, spreadsheet_id, sheet_name):
    """Read data from a sheet tab."""
    range_name = sheet_name  # Read entire sheet
    result = (
        sheets_service.spreadsheets()
        .values()
        .get(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
    )
    values = result.get("values", [])
    return values


def export_tab_to_csv(sheets_service, spreadsheet_id, sheet_name, csv_path):
    """Export a sheet tab to a CSV file."""
    values = read_sheet_data(sheets_service, spreadsheet_id, sheet_name)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(values)


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


def ingest_from_drive(test_mode=False, clean_up=False):
    """
    Download Google Sheets from Drive and export to data/raw/ directory.

    Args:
        test_mode: If True, stop after downloading one of each tab type (CT.NHAP, CT.XUAT, XNT)
        clean_up: If True, remove existing data/raw/ directory before ingesting

    Returns:
        Number of files ingested
    """
    import shutil

    print("Connecting to Google Drive...")
    drive_service, sheets_service = connect_to_drive()
    print("Connected successfully!")

    # Find year folders
    year_folders = find_year_folders(drive_service)
    print(f"Found year folders: {list(year_folders.keys())}")

    # Desired tabs
    desired_tabs = ["CT.NHAP", "CT.XUAT", "XNT"]

    # Optionally clear and recreate raw directory
    if clean_up and os.path.exists("data/raw"):
        shutil.rmtree("data/raw")
        print("Cleared data/raw/")

    os.makedirs("data/raw", exist_ok=True)

    tabs_processed = set()
    files_ingested = 0

    for year_name, folder_id in year_folders.items():
        sheets = find_sheets_in_folder(drive_service, folder_id)
        for sheet in sheets:
            file_name = sheet["name"]
            file_id = sheet["id"]
            year_num, month = parse_file_metadata(file_name)
            if year_num is None or month is None:
                print(f"Skipping {file_name}: invalid metadata")
                continue

            try:
                tabs = get_sheet_tabs(sheets_service, file_id)
                for tab in set(tabs) & set(desired_tabs):
                    csv_path = f"data/raw/{year_num}_{month}_{tab}.csv"
                    export_tab_to_csv(sheets_service, file_id, tab, csv_path)
                    print(f"✓ Exported {csv_path}")
                    tabs_processed.add(tab)
                    files_ingested += 1

                    if test_mode and len(tabs_processed) >= len(desired_tabs):
                        print(f"\nTest mode: downloaded one of each tab type {sorted(tabs_processed)}")
                        return files_ingested

            except Exception as e:
                print(f"✗ Error exporting {file_name}: {e}")
                if test_mode:
                    return files_ingested

    print(f"\n{'=' * 60}")
    print(f"Ingestion Summary")
    print(f"{'=' * 60}")
    print(f"Files ingested:  {files_ingested}")
    print(f"Tab types found: {sorted(tabs_processed)}")
    print(f"{'=' * 60}\n")

    return files_ingested


if __name__ == "__main__":
    ingest_from_drive()
