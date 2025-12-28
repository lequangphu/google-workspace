# -*- coding: utf-8 -*-
"""
⚠️ DEPRECATED: This script has been migrated to src/modules/receivable/clean_customers.py

Use the new module instead:
    from src.modules.receivable.clean_customers import process

This legacy file is kept for reference only.
---

Clean and transform customer information (Thông tin khách hàng).

This script:
1. Reads 'Thong tin KH' tab from source Google Sheets
2. Selects and renames specific columns
3. Removes rows with empty customer code
4. Exports to CSV and uploads to destination Google Sheets as a new tab
"""

import logging
import os
from pathlib import Path

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ============================================================================
# CONFIGURATION
# ============================================================================

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# Source spreadsheet containing customer information
SOURCE_SPREADSHEET_ID = "1kouZwJy8P_zZhjjn49Lfbp3KN81mhHADV7VKDhv5xkM"
SOURCE_SHEET_NAME = "Thong tin KH"

# Destination spreadsheet for upload
DEST_SPREADSHEET_ID = "1nulVkpFU1MihYvJDvHfj53cyNvJQhRQbSm_8Ru0IGOU"
DEST_SHEET_NAME = "Thông tin khách hàng"

# Output directory for CSV
REPORTS_DIR = Path.cwd() / "data" / "reports"
OUTPUT_FILENAME = "Thông tin khách hàng.csv"

# Column selection and renaming mapping
COLUMN_MAPPING = {
    "MÃ KH": "Mã khách hàng",
    "TÊN KHÁCH HÀNG": "Tên khách hàng",
    "Địa chỉ ": "Địa chỉ",
    "Tel": "Điện thoại",
}

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ============================================================================
# AUTHENTICATION
# ============================================================================


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


def connect_to_sheets():
    """Connect to Google Sheets API."""
    creds = authenticate_google()
    return build("sheets", "v4", credentials=creds)


# ============================================================================
# SHEET OPERATIONS
# ============================================================================


def read_sheet_data(sheets_service, spreadsheet_id: str, sheet_name: str) -> list:
    """Read all data from a sheet tab."""
    try:
        result = (
            sheets_service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=sheet_name,
                valueRenderOption="FORMATTED_VALUE",
            )
            .execute()
        )
        values = result.get("values", [])
        return values
    except HttpError as e:
        logger.error(f"Failed to read sheet {sheet_name}: {e}")
        return []


def sheet_exists(sheets_service, spreadsheet_id: str, sheet_name: str) -> bool:
    """Check if a sheet tab exists in the spreadsheet."""
    try:
        result = (
            sheets_service.spreadsheets()
            .get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title")
            .execute()
        )
        tabs = [sheet["properties"]["title"] for sheet in result["sheets"]]
        return sheet_name in tabs
    except HttpError as e:
        logger.error(f"Failed to check sheet existence: {e}")
        return False


def create_sheet(sheets_service, spreadsheet_id: str, sheet_name: str) -> bool:
    """Create a new sheet tab."""
    try:
        request_body = {
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": sheet_name,
                        }
                    }
                }
            ]
        }
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body=request_body
        ).execute()
        logger.info(f"Created new sheet: {sheet_name}")
        return True
    except HttpError as e:
        logger.error(f"Failed to create sheet: {e}")
        return False


def clear_sheet(sheets_service, spreadsheet_id: str, sheet_name: str) -> bool:
    """Clear all data from a sheet tab."""
    try:
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=sheet_name
        ).execute()
        logger.info(f"Cleared sheet: {sheet_name}")
        return True
    except HttpError as e:
        logger.error(f"Failed to clear sheet: {e}")
        return False


def write_sheet_data(
    sheets_service, spreadsheet_id: str, sheet_name: str, data: list
) -> bool:
    """Write data to a sheet tab."""
    try:
        request_body = {
            "values": data,
        }
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="USER_ENTERED",
            body=request_body,
        ).execute()
        logger.info(f"Wrote {len(data)} rows to {sheet_name}")
        return True
    except HttpError as e:
        logger.error(f"Failed to write to sheet: {e}")
        return False


# ============================================================================
# DATA PROCESSING
# ============================================================================


def load_and_clean_data(raw_data: list) -> pd.DataFrame:
    """Load raw data and perform initial cleaning.

    Args:
        raw_data: List of lists from Google Sheets (rows)

    Returns:
        pd.DataFrame: DataFrame with header in row 0
    """
    if not raw_data:
        logger.error("No data received from Google Sheets")
        return pd.DataFrame()

    # Find header row (should be row 3 in the original sheet, index 2)
    header_row_idx = None
    for idx, row in enumerate(raw_data):
        if row and row[0] == "STT":  # Header starts with STT
            header_row_idx = idx
            break

    if header_row_idx is None:
        logger.error("Could not find header row (STT column)")
        return pd.DataFrame()

    logger.info(f"Found header at row {header_row_idx + 1}")

    # Extract header and data
    header_row = raw_data[header_row_idx]
    data_rows = raw_data[header_row_idx + 1 :]

    # Create DataFrame
    df = pd.DataFrame(data_rows, columns=header_row)

    # Remove rows with completely empty values
    df = df.dropna(how="all")

    # Remove sub-header row (contains column numbers like "1", "2", "3", etc.)
    if not df.empty and df.iloc[0, 0] in ["1", "2", "3", "4"]:
        # Check if this looks like a sub-header row (all numeric values)
        if all(str(val).strip().isdigit() or pd.isna(val) for val in df.iloc[0]):
            df = df.iloc[1:].reset_index(drop=True)
            logger.info("Removed sub-header row with column numbers")

    return df


def clean_phone_number(phone: str) -> str:
    """Clean a single phone number by removing trailing dots, commas, and spaces.

    Args:
        phone: Raw phone number string

    Returns:
        Cleaned phone number
    """
    if not phone:
        return ""
    # Strip whitespace and trailing punctuation
    phone = phone.strip().rstrip(".,;:")
    return phone


def split_phone_numbers(phone_str: str) -> list:
    """Split phone numbers by common delimiters and clean them.

    Handles delimiters: /, -, and multiple spaces.

    Args:
        phone_str: Raw phone string potentially containing multiple numbers

    Returns:
        List of cleaned phone numbers
    """
    if not phone_str or pd.isna(phone_str):
        return []

    phone_str = str(phone_str).strip()
    if not phone_str or phone_str == "None":
        return []

    # Split by common delimiters: /, -, and multiple spaces
    # First normalize spaces (replace multiple spaces with single delimiter)
    import re

    # Split by / or -
    if "/" in phone_str or " - " in phone_str:
        phones = re.split(r"\s*[/-]\s*", phone_str)
    else:
        # If only spaces, treat as single number
        phones = [phone_str]

    # Clean each phone number
    cleaned = [clean_phone_number(p) for p in phones]
    # Remove empty strings
    cleaned = [p for p in cleaned if p]

    return cleaned


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Transform data according to specifications.

    1. Select only required columns
    2. Rename columns
    3. Remove rows with empty MÃ KH
    4. Transform phone numbers: format as text, split multiple numbers, clean formatting
    5. Clean up whitespace
    """
    if df.empty:
        return df

    # Select only columns that exist in the mapping
    available_cols = [col for col in COLUMN_MAPPING.keys() if col in df.columns]
    df = df[available_cols].copy()

    # Rename columns
    df = df.rename(columns=COLUMN_MAPPING)

    # Drop rows where Mã khách hàng is empty, NaN, or just numeric (sub-header)
    df = df.dropna(subset=["Mã khách hàng"])
    df = df[df["Mã khách hàng"].astype(str).str.strip() != ""]
    # Remove rows where Mã khách hàng is a single digit (likely sub-header)
    df = df[~df["Mã khách hàng"].astype(str).str.match(r"^\d+$")]

    # Process phone numbers
    if "Điện thoại" in df.columns:
        # Split phone numbers into separate columns
        phone_lists = df["Điện thoại"].apply(split_phone_numbers)

        # Find max number of phone numbers
        max_phones = (
            max(len(phones) for phones in phone_lists) if phone_lists.any() else 0
        )

        # Create new columns for each phone number
        for i in range(1, max_phones + 1):
            col_name = f"Điện thoại {i}" if i > 1 else "Điện thoại"
            df[col_name] = phone_lists.apply(
                lambda phones: phones[i - 1] if i <= len(phones) else ""
            )

        # Format phone columns as text (leading zeros preserved)
        for col in df.columns:
            if col.startswith("Điện thoại"):
                df[col] = (
                    df[col]
                    .astype(str)
                    .apply(lambda x: f"'{x}" if x and x != "" else "")
                )

    # Clean up whitespace in all columns and replace None with empty string
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace("None", "")

    logger.info(f"After transformation: {len(df)} rows with {len(df.columns)} columns")

    return df


# ============================================================================
# MAIN EXECUTION
# ============================================================================


def main() -> None:
    """Main processing pipeline."""
    logger.info("Starting customer information processing")

    # Connect to Google Sheets
    try:
        sheets_service = connect_to_sheets()
        logger.info("Connected to Google Sheets")
    except Exception as e:
        logger.error(f"Failed to connect to Google Sheets: {e}")
        return

    # Read data from source sheet
    logger.info(f"Reading from {SOURCE_SHEET_NAME}...")
    raw_data = read_sheet_data(sheets_service, SOURCE_SPREADSHEET_ID, SOURCE_SHEET_NAME)

    if not raw_data:
        logger.error("No data received from source sheet")
        return

    logger.info(f"Received {len(raw_data)} rows from source")

    # Load and clean
    df = load_and_clean_data(raw_data)
    if df.empty:
        logger.error("Failed to load data from source")
        return

    logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")

    # Transform
    df = transform_data(df)
    if df.empty:
        logger.warning("No data after transformation")
        return

    # Save to CSV
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = REPORTS_DIR / OUTPUT_FILENAME
    df.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info(f"Saved to CSV: {csv_path}")

    # Upload to destination sheet
    logger.info(f"Uploading to destination sheet: {DEST_SHEET_NAME}...")

    # Check if destination sheet exists, create if not
    if not sheet_exists(sheets_service, DEST_SPREADSHEET_ID, DEST_SHEET_NAME):
        if not create_sheet(sheets_service, DEST_SPREADSHEET_ID, DEST_SHEET_NAME):
            logger.error("Failed to create destination sheet")
            return
    else:
        # Clear existing data
        if not clear_sheet(sheets_service, DEST_SPREADSHEET_ID, DEST_SHEET_NAME):
            logger.error("Failed to clear destination sheet")
            return

    # Prepare data for upload (header + rows)
    upload_data = [df.columns.tolist()] + df.values.tolist()

    # Write to destination sheet
    if write_sheet_data(
        sheets_service, DEST_SPREADSHEET_ID, DEST_SHEET_NAME, upload_data
    ):
        logger.info(f"Successfully uploaded {len(df)} rows to destination sheet")
    else:
        logger.error("Failed to upload to destination sheet")
        return

    logger.info("Processing complete")


if __name__ == "__main__":
    main()
