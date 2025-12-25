# -*- coding: utf-8 -*-
"""Batch upload remaining customer IDs to Google Sheets."""

import csv
import json
from pathlib import Path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# Configuration
SPREADSHEET_ID = "1nulVkpFU1MihYvJDvHfj53cyNvJQhRQbSm_8Ru0IGOU"
SHEET_NAME = "Mã khách hàng mới"
CREDENTIALS_FILE = Path.cwd() / "credentials.json"
TOKEN_FILE = Path.cwd() / "token.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

DATA_FILE = Path.cwd() / "data" / "final" / "Mã khách hàng mới.csv"


def get_credentials():
    """Get or refresh Google Sheets credentials."""
    creds = None
    
    # Load existing token
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    
    # Refresh if needed
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    
    # Get new credentials if needed
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(
            CREDENTIALS_FILE, SCOPES
        )
        creds = flow.run_local_server(port=0)
    
    # Save token
    with open(TOKEN_FILE, "w") as f:
        f.write(creds.to_json())
    
    return creds


def upload_data():
    """Upload customer IDs to Google Sheets."""
    creds = get_credentials()
    service = build("sheets", "v4", credentials=creds)
    
    # Read CSV
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    
    # All rows including header
    all_rows = rows
    
    # Clear all data in sheet first
    print(f"Clearing existing data in {SHEET_NAME}...")
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:Z"
        ).execute()
        print("  ✓ Sheet cleared")
    except Exception as e:
        print(f"  ✗ Error clearing sheet: {e}")
    
    print(f"Uploading {len(all_rows)} rows (including header) to Google Sheets...")
    
    # Split into chunks for batch processing (max 10,000 cells per request)
    chunk_size = 200  # 200 rows × 6 columns = 1,200 cells
    chunks = []
    for i in range(0, len(all_rows), chunk_size):
        chunks.append(all_rows[i:i+chunk_size])
    
    print(f"Split into {len(chunks)} chunks")
    
    # Upload each chunk
    for idx, chunk in enumerate(chunks):
        start_row = 1 + (idx * chunk_size)  # Row 1 (including header)
        end_row = start_row + len(chunk) - 1
        
        range_name = f"{SHEET_NAME}!A{start_row}:F{end_row}"
        
        try:
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=range_name,
                valueInputOption="USER_ENTERED",
                body={"values": chunk}
            ).execute()
            
            print(f"  ✓ Chunk {idx+1}/{len(chunks)}: Rows {start_row}-{end_row} ({len(chunk)} rows)")
        except Exception as e:
            print(f"  ✗ Chunk {idx+1}/{len(chunks)}: Error - {e}")
    
    print("\nUpload complete!")


if __name__ == "__main__":
    upload_data()
