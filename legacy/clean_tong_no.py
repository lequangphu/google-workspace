# -*- coding: utf-8 -*-
"""
⚠️ DEPRECATED: This script has been migrated to src/modules/receivable/clean_debts.py

Original status: Debt information processing
Migration completed: T-019b624e
This legacy file is kept for reference only.
---

Clean and transform total debt information (Tổng công nợ).

This script:
1. Reads 'TỔNG CÔNG NỢ' tab from source Google Sheets
2. Reads cleaned 'Thông tin khách hàng' tab from destination
3. Joins to get customer codes (Mã khách hàng)
4. Selects, renames, and reorders columns
5. Removes rows with empty customer names
6. Exports to CSV and uploads to destination Google Sheets
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

# Source spreadsheet containing debt information
SOURCE_SPREADSHEET_ID = "1kouZwJy8P_zZhjjn49Lfbp3KN81mhHADV7VKDhv5xkM"
SOURCE_SHEET_NAME = "TỔNG CÔNG NỢ"

# Destination spreadsheet for upload
DEST_SPREADSHEET_ID = "1nulVkpFU1MihYvJDvHfj53cyNvJQhRQbSm_8Ru0IGOU"
DEST_SHEET_NAME_KH = "Thông tin khách hàng"  # To get customer codes
DEST_SHEET_NAME_NO = "Tổng nợ"  # Output sheet

# Output directory for CSV
REPORTS_DIR = Path.cwd() / "data" / "reports"
OUTPUT_FILENAME = "Tổng nợ.csv"

# Column selection and renaming mapping
# Note: Google Sheets may have trailing spaces in column names
COLUMN_MAPPING = {
    "TÊN KHÁCH HÀNG": "Tên khách hàng",
    " TỔNG NỢ ": "Nợ",
    " ĐÃ THANH TOÁN ": "Nợ đã thu",
    " NỢ CÒN LẠI  ": "Nợ cần thu hiện tại",
}

# Final column order (with Mã khách hàng as first)
FINAL_COLUMNS = [
    "Mã khách hàng",
    "Tên khách hàng",
    "Nợ",
    "Nợ đã thu",
    "Nợ cần thu hiện tại",
]

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

    # Find header row (should start with "STT")
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

    # Pad rows to match header length
    max_cols = len(header_row)
    padded_rows = []
    for row in data_rows:
        if len(row) < max_cols:
            row = row + [""] * (max_cols - len(row))
        padded_rows.append(row[:max_cols])

    # Create DataFrame
    df = pd.DataFrame(padded_rows, columns=header_row)

    # Remove rows with completely empty values
    df = df.dropna(how="all")

    return df


def get_customer_mapping(sheets_service) -> pd.DataFrame:
    """Read cleaned customer info to get Mã khách hàng mapping.

    Returns:
        pd.DataFrame: DataFrame with Tên khách hàng and Mã khách hàng
    """
    raw_data = read_sheet_data(sheets_service, DEST_SPREADSHEET_ID, DEST_SHEET_NAME_KH)

    if not raw_data:
        logger.error("No data received from customer info sheet")
        return pd.DataFrame()

    # First row should be header
    if raw_data:
        header_row = raw_data[0]
        data_rows = raw_data[1:]
        df = pd.DataFrame(data_rows, columns=header_row)
        df = df.dropna(how="all")

        # Select only Mã khách hàng and Tên khách hàng
        required_cols = ["Mã khách hàng", "Tên khách hàng"]
        available_cols = [col for col in required_cols if col in df.columns]

        if available_cols:
            return df[available_cols].copy()

    return pd.DataFrame()


def get_raw_customer_data(sheets_service) -> pd.DataFrame:
    """Read raw customer info tab from source.

    Returns:
        pd.DataFrame: DataFrame with MÃ KH and TÊN KHÁCH HÀNG
    """
    raw_data = read_sheet_data(sheets_service, SOURCE_SPREADSHEET_ID, "Thong tin KH")

    if not raw_data:
        logger.warning("No data received from raw customer info sheet")
        return pd.DataFrame()

    # Find header row
    header_row_idx = None
    for idx, row in enumerate(raw_data):
        if row and row[0] == "STT":
            header_row_idx = idx
            break

    if header_row_idx is None:
        logger.warning("Could not find header in raw customer info sheet")
        return pd.DataFrame()

    header_row = raw_data[header_row_idx]
    data_rows = raw_data[header_row_idx + 1 :]

    df = pd.DataFrame(data_rows, columns=header_row)
    df = df.dropna(how="all")

    # Select only MÃ KH and TÊN KHÁCH HÀNG if they exist
    if "MÃ KH" in df.columns and "TÊN KHÁCH HÀNG" in df.columns:
        return df[["MÃ KH", "TÊN KHÁCH HÀNG"]].copy()

    return pd.DataFrame()


def transform_data(
    df: pd.DataFrame,
    customer_mapping: pd.DataFrame,
    raw_customer_data: pd.DataFrame = None,
) -> pd.DataFrame:
    """Transform data according to specifications.

    1. Select only required columns
    2. Rename columns
    3. Remove rows with empty TÊN KHÁCH HÀNG
    4. Join with customer codes (two-level join strategy)
    5. Reorder columns
    """
    if df.empty:
        return df

    # Drop rows where TÊN KHÁCH HÀNG is empty
    df = df.dropna(subset=["TÊN KHÁCH HÀNG"])
    df = df[df["TÊN KHÁCH HÀNG"].astype(str).str.strip() != ""]

    # Select only columns that exist in the mapping
    available_cols = [col for col in COLUMN_MAPPING.keys() if col in df.columns]
    df = df[available_cols].copy()

    # Rename columns
    df = df.rename(columns=COLUMN_MAPPING)

    # Clean numeric columns
    numeric_cols = ["Nợ", "Nợ đã thu", "Nợ cần thu hiện tại"]
    for col in numeric_cols:
        if col in df.columns:
            # Convert to string and strip whitespace
            df[col] = df[col].astype(str).str.strip()

            # Handle different formats
            def parse_numeric(x):
                if x == "-":
                    return "0"
                # Remove spaces and Vietnamese thousands separator
                x = x.replace(" ", "").replace(".", "")
                # Handle parentheses as negative (e.g., "(30000)" = -30000)
                if x.startswith("(") and x.endswith(")"):
                    x = "-" + x[1:-1]
                return x

            df[col] = df[col].apply(parse_numeric)
            # Convert to numeric
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Drop rows where all numeric columns are 0
    zero_mask = df[numeric_cols].eq(0).all(axis=1)
    rows_before = len(df)
    df = df[~zero_mask]
    rows_after = len(df)
    if rows_before > rows_after:
        logger.info(
            f"Dropped {rows_before - rows_after} rows with all zero values in debt columns"
        )

    # Primary join: try joining on cleaned customer names
    if not customer_mapping.empty:
        df = df.merge(
            customer_mapping,
            on="Tên khách hàng",
            how="left",
        )
        logger.info(f"Primary join: matched {df['Mã khách hàng'].notna().sum()} rows")

    # Secondary join: for rows with NaN Mã khách hàng, try joining with raw data
    if raw_customer_data is not None and not raw_customer_data.empty:
        # Find rows with missing Mã khách hàng
        missing_mask = df["Mã khách hàng"].isna()
        missing_count = missing_mask.sum()

        if missing_count > 0:
            logger.info(
                f"Secondary join: attempting to fill {missing_count} missing values"
            )

            # Join raw data on TÊN KHÁCH HÀNG
            unmatched_df = df[missing_mask].copy()
            unmatched_df = unmatched_df.merge(
                raw_customer_data,
                left_on="Tên khách hàng",
                right_on="TÊN KHÁCH HÀNG",
                how="left",
            )

            # Update the original dataframe with matched codes
            matched_mask = unmatched_df["MÃ KH"].notna()
            matched_indices = df[missing_mask].index[unmatched_df.index[matched_mask]]

            for idx in matched_indices:
                row_idx = (df[missing_mask].index == idx).argmax()
                if row_idx < len(unmatched_df) and pd.notna(
                    unmatched_df.iloc[row_idx]["MÃ KH"]
                ):
                    df.at[idx, "Mã khách hàng"] = unmatched_df.iloc[row_idx]["MÃ KH"]

            filled = df["Mã khách hàng"].notna().sum() - customer_mapping.shape[0]
            logger.info(
                f"Secondary join: filled {filled} additional rows"
            )

    # Clean up whitespace in all columns and replace None with empty string
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace("None", "")
            df[col] = df[col].replace("nan", "")

    # Reorder columns to final order (only include columns that exist)
    final_cols = [col for col in FINAL_COLUMNS if col in df.columns]
    df = df[final_cols]

    logger.info(f"After transformation: {len(df)} rows with {len(df.columns)} columns")
    logger.info(
        f"Mã khách hàng filled: {df['Mã khách hàng'].notna().sum()} / {len(df)}"
    )

    return df


# ============================================================================
# MAIN EXECUTION
# ============================================================================


def main() -> None:
    """Main processing pipeline."""
    logger.info("Starting debt information processing")

    # Connect to Google Sheets
    try:
        sheets_service = connect_to_sheets()
        logger.info("Connected to Google Sheets")
    except Exception as e:
        logger.error(f"Failed to connect to Google Sheets: {e}")
        return

    # Read customer mapping from destination
    logger.info(f"Reading customer mapping from {DEST_SHEET_NAME_KH}...")
    customer_mapping = get_customer_mapping(sheets_service)
    if customer_mapping.empty:
        logger.warning("No customer mapping available, will proceed with partial data")
    else:
        logger.info(f"Loaded {len(customer_mapping)} customer records")

    # Read raw customer data for secondary join
    logger.info("Reading raw customer data from source...")
    raw_customer_data = get_raw_customer_data(sheets_service)
    if raw_customer_data.empty:
        logger.warning("No raw customer data available for secondary join")
    else:
        logger.info(f"Loaded {len(raw_customer_data)} raw customer records")

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
    df = transform_data(df, customer_mapping, raw_customer_data)
    if df.empty:
        logger.warning("No data after transformation")
        return

    # Save to CSV
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = REPORTS_DIR / OUTPUT_FILENAME
    df.to_csv(csv_path, index=False, encoding="utf-8")
    logger.info(f"Saved to CSV: {csv_path}")

    # Upload to destination sheet
    logger.info(f"Uploading to destination sheet: {DEST_SHEET_NAME_NO}...")

    # Check if destination sheet exists, create if not
    if not sheet_exists(sheets_service, DEST_SPREADSHEET_ID, DEST_SHEET_NAME_NO):
        if not create_sheet(sheets_service, DEST_SPREADSHEET_ID, DEST_SHEET_NAME_NO):
            logger.error("Failed to create destination sheet")
            return
    else:
        # Clear existing data
        if not clear_sheet(sheets_service, DEST_SPREADSHEET_ID, DEST_SHEET_NAME_NO):
            logger.error("Failed to clear destination sheet")
            return

    # Prepare data for upload (header + rows)
    upload_data = [df.columns.tolist()] + df.values.tolist()

    # Write to destination sheet
    if write_sheet_data(
        sheets_service, DEST_SPREADSHEET_ID, DEST_SHEET_NAME_NO, upload_data
    ):
        logger.info(f"Successfully uploaded {len(df)} rows to destination sheet")
    else:
        logger.error("Failed to upload to destination sheet")
        return

    logger.info("Processing complete")


if __name__ == "__main__":
    main()
