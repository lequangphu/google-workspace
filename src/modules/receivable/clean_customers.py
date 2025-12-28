# -*- coding: utf-8 -*-
"""Clean and transform customer information (Thông tin khách hàng).

This module:
1. Reads customer information from Google Sheets
2. Selects and renames specific columns
3. Removes rows with empty customer code
4. Splits multiple phone numbers into separate columns
5. Exports to CSV in staging directory

Raw source: Thông tin khách hàng from Google Sheets (Thông tin KH tab)
Module: receivable
Pipeline stage: Google Sheets → data/01-staging/
"""

import logging
import os
import re
from pathlib import Path
from typing import List, Optional

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

# Column selection and renaming mapping (Vietnamese: exact case-sensitive)
COLUMN_MAPPING = {
    "MÃ KH": "Mã khách hàng",
    "TÊN KHÁCH HÀNG": "Tên khách hàng",
    "Địa chỉ ": "Địa chỉ",
    "Tel": "Điện thoại",
}

# ============================================================================
# LOGGING SETUP
# ============================================================================

logger = logging.getLogger(__name__)


# ============================================================================
# AUTHENTICATION
# ============================================================================


def authenticate_google() -> Credentials:
    """Authenticate with Google API using OAuth2.

    Returns:
        Authorized credentials object
    """
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
    """Connect to Google Sheets API.

    Returns:
        Sheets service object
    """
    creds = authenticate_google()
    return build("sheets", "v4", credentials=creds)


# ============================================================================
# SHEET OPERATIONS
# ============================================================================


def read_sheet_data(sheets_service, spreadsheet_id: str, sheet_name: str) -> list:
    """Read all data from a sheet tab.

    Args:
        sheets_service: Google Sheets API service
        spreadsheet_id: ID of the spreadsheet
        sheet_name: Name of the sheet tab

    Returns:
        List of lists containing sheet data
    """
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


# ============================================================================
# DATA PROCESSING
# ============================================================================


def load_and_clean_data(raw_data: list) -> pd.DataFrame:
    """Load raw data and perform initial cleaning.

    Args:
        raw_data: List of lists from Google Sheets (rows)

    Returns:
        DataFrame with header in row 0
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


def split_phone_numbers(phone_str: str) -> List[str]:
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
    3. Remove rows with empty Mã khách hàng
    4. Split multiple phone numbers into separate columns
    5. Format phone columns as text (preserve leading zeros)
    6. Clean up whitespace

    Args:
        df: Raw DataFrame from Google Sheets

    Returns:
        Transformed DataFrame
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


def process(staging_dir: Optional[Path] = None) -> Optional[Path]:
    """Process customer data from Google Sheets and save to staging directory.

    Args:
        staging_dir: Directory to save staged data (defaults to data/01-staging/receivable)

    Returns:
        Path to output file or None if failed
    """
    if staging_dir is None:
        staging_dir = Path.cwd() / "data" / "01-staging" / "receivable"

    staging_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("STARTING CUSTOMER INFORMATION PROCESSING")
    logger.info("=" * 70)

    try:
        # Connect to Google Sheets
        sheets_service = connect_to_sheets()
        logger.info("Connected to Google Sheets")

        # Read data from source sheet
        logger.info(f"Reading from {SOURCE_SHEET_NAME}...")
        raw_data = read_sheet_data(
            sheets_service, SOURCE_SPREADSHEET_ID, SOURCE_SHEET_NAME
        )

        if not raw_data:
            logger.error("No data received from source sheet")
            return None

        logger.info(f"Received {len(raw_data)} rows from source")

        # Load and clean
        df = load_and_clean_data(raw_data)
        if df.empty:
            logger.error("Failed to load data from source")
            return None

        logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")

        # Transform
        df = transform_data(df)
        if df.empty:
            logger.warning("No data after transformation")
            return None

        # Save to CSV
        output_filename = "clean_customers.csv"
        output_path = staging_dir / output_filename
        df.to_csv(output_path, index=False, encoding="utf-8")
        logger.info(f"Saved to CSV: {output_path}")
        logger.info(f"Output: {len(df)} rows with {len(df.columns)} columns")

        logger.info("=" * 70)
        logger.info("CUSTOMER PROCESSING COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)

        return output_path

    except Exception as e:
        logger.error(f"Customer information processing failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    process()
