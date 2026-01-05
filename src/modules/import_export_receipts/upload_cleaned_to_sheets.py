"""Upload cleaned data to source Google Sheets for reconciliation.

This script:
1. Cleans empty rows and columns from cleaned data
2. Splits merged cleaned data by period (Year, Month)
3. Maps each period to its corresponding Google Sheet
4. Replaces existing tabs (or creates at beginning if doesn't exist)
5. Handles rate limiting with delays between API calls
6. Uploads all periods (2020-2025) in one run

New Tab Names (replacing if exists):
- "Chi tiết nhập"     ← Chi tiết nhập*.csv (purchase)
- "Chi tiết xuất"     ← Chi tiết xuất*.csv (sale)
- "Xuất nhập tồn"     ← xuat_nhap_ton*.csv (inventory)
- "Báo cáo tài chính" ← bao_cao_tai_chinh.csv (financial report)

Note: Summary files ("Tổng hợp nhập" and "Tổng hợp xuất") are generated but NOT uploaded
to preserve only detail-level data in Google Sheets. Summary files remain in staging directory
for local reconciliation purposes.
"""

import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.modules.google_api import (
    connect_to_drive,
    get_sheets_for_folder,
    load_manifest,
    save_manifest,
)
import tomllib

# ============================================================================
# CONFIGURATION
# ============================================================================

with open("pipeline.toml", "rb") as f:
    CONFIG = tomllib.load(f)

FOLDER_IDS = CONFIG["sources"]["import_export_receipts"]["folder_ids"]

CLEANED_FILE_TO_TAB = {
    "Chi tiết nhập": "Chi tiết nhập",
    "Chi tiết xuất": "Chi tiết xuất",
    "xuat_nhap_ton": "Xuất nhập tồn",
    "bao_cao_tai_chinh": "Báo cáo tài chính",
}

API_CALL_DELAY = 0.5

# ============================================================================
# LOGGING SETUP
# ============================================================================

logger = logging.getLogger(__name__)

# ============================================================================
# DATA CLEANING FUNCTIONS
# ============================================================================


def clean_dataframe_for_upload(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    stats = {
        "original_rows": len(df),
        "original_cols": len(df.columns),
        "empty_rows_removed": 0,
        "empty_cols_removed": 0,
    }

    df_clean = df.copy()

    rows_before = len(df_clean)
    df_clean = df_clean.dropna(how="all")
    df_clean = df_clean[~(df_clean.map(lambda x: x == "" or pd.isna(x))).all(axis=1)]
    stats["empty_rows_removed"] = rows_before - len(df_clean)

    cols_before = len(df_clean.columns)
    df_clean = df_clean.dropna(axis=1, how="all")
    df_clean = df_clean.loc[:, ~(df_clean == "").all()]
    stats["empty_cols_removed"] = cols_before - len(df_clean.columns)

    logger.info(
        f"Cleaning: Removed {stats['empty_rows_removed']} empty rows, "
        f"{stats['empty_cols_removed']} empty columns"
    )
    logger.info(
        f"  Before: {stats['original_rows']} rows, {stats['original_cols']} cols"
    )
    logger.info(f"  After: {len(df_clean)} rows, {len(df_clean.columns)} cols")

    return df_clean, stats


def split_cleaned_data_by_period(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    if "Năm" not in df.columns or "Tháng" not in df.columns:
        logger.error("DataFrame missing 'Năm' or 'Tháng' columns for period splitting")
        return {}

    periods = df[["Năm", "Tháng"]].drop_duplicates().sort_values(["Năm", "Tháng"])

    period_dfs = {}

    for _, row in periods.iterrows():
        year = int(row["Năm"])
        month = int(row["Tháng"])
        period_key = f"{year}_{month:02d}"

        period_mask = (df["Năm"] == year) & (df["Tháng"] == month)
        period_df = df[period_mask].copy()

        cols_to_keep = [col for col in period_df.columns if col not in ["Năm", "Tháng"]]
        period_df = period_df[cols_to_keep]

        period_dfs[period_key] = period_df
        logger.info(f"Split period {period_key}: {len(period_df)} rows")

    return period_dfs


# ============================================================================
# GOOGLE SHEETS MAPPING FUNCTIONS
# ============================================================================


def find_spreadsheet_for_period(
    period: str, sheets_metadata: List[Dict]
) -> Optional[str]:
    year, month = period.split("_")
    month_num = int(month)

    pattern = f"T{month_num:02d}.{year[-2:]}"
    pattern_alt = f"T{month_num}.{year[-2:]}"

    for sheet in sheets_metadata:
        filename = sheet["name"]

        if pattern in filename or pattern_alt in filename:
            logger.debug(f"Matched period {period} -> file: {filename}")
            return sheet["id"]

    logger.warning(f"No spreadsheet found for period: {period}")
    return None


def load_cleaned_files(staging_dir: Path) -> Dict[str, Path]:
    cleaned_files = {}

    cleaned_file_patterns = [
        ("Chi tiết nhập", "Chi tiết nhập"),
        ("Chi tiết xuất", "Chi tiết xuất"),
    ]

    for pattern_key, tab_name in cleaned_file_patterns:
        matching_files = list(staging_dir.glob(f"{pattern_key}*.csv"))
        if matching_files:
            latest_file = sorted(matching_files)[-1]
            cleaned_files[pattern_key] = latest_file
            logger.info(f"Found cleaned file: {latest_file.name}")

    xnt_files = list(staging_dir.glob("xuat_nhap_ton*.csv"))
    xnt_files = [f for f in xnt_files if "adjustments" not in f.name.lower()]
    if xnt_files:
        xnt_path = sorted(xnt_files)[-1]
        cleaned_files["xuat_nhap_ton"] = xnt_path
        logger.info(f"Found XNT file: {xnt_path.name}")

    financial_file = staging_dir / "bao_cao_tai_chinh.csv"
    if financial_file.exists():
        cleaned_files["bao_cao_tai_chinh"] = financial_file
        logger.info("Found financial report: bao_cao_tai_chinh.csv")

    return cleaned_files


def move_sheet_to_beginning(sheets_service, spreadsheet_id: str, sheet_id: int) -> bool:
    try:
        request = {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "index": 0,
                }
            }
        }
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": [request]}
        ).execute()
        time.sleep(API_CALL_DELAY)
        return True
    except Exception as e:
        logger.error(f"Failed to move sheet to beginning: {e}")
        return False


def upload_to_spreadsheet(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    df: pd.DataFrame,
    replace: bool = True,
    dry_run: bool = False,
) -> bool:
    if dry_run:
        logger.info(
            f"[DRY RUN] Would upload to {tab_name} in spreadsheet {spreadsheet_id}"
        )
        logger.info(f"[DRY RUN]   Rows: {len(df)}, Columns: {len(df.columns)}")
        return True

    try:
        spreadsheet = (
            sheets_service.spreadsheets()
            .get(
                spreadsheetId=spreadsheet_id,
                fields="sheets(properties(sheetId,title))",
            )
            .execute()
        )
        time.sleep(API_CALL_DELAY)

        existing_sheet_id = None
        for sheet in spreadsheet.get("sheets", []):
            if sheet["properties"]["title"] == tab_name:
                existing_sheet_id = sheet["properties"]["sheetId"]
                break

        if existing_sheet_id is not None:
            logger.info(f"Clearing existing sheet: '{tab_name}'")
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f"'{tab_name}'!A:Z",
            ).execute()
            time.sleep(API_CALL_DELAY)
        else:
            logger.info(f"Creating new sheet at beginning: '{tab_name}'")
            request = {
                "addSheet": {
                    "properties": {
                        "title": tab_name,
                        "index": 0,
                    }
                }
            }
            response = (
                sheets_service.spreadsheets()
                .batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [request]})
                .execute()
            )
            time.sleep(API_CALL_DELAY)

            new_sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]
            move_sheet_to_beginning(sheets_service, spreadsheet_id, new_sheet_id)

        # Ensure Mã hàng is treated as text to preserve leading zeros
        df_upload = df.copy()
        if "Mã hàng" in df_upload.columns:
            df_upload["Mã hàng"] = df_upload["Mã hàng"].astype(str)

        # Convert all values to strings, preserving empty values as empty strings
        values = [df_upload.columns.tolist()]
        for _, row in df_upload.iterrows():
            row_values = ["" if pd.isna(v) else str(v) for v in row]
            values.append(row_values)

        logger.info(f"Writing {len(df)} rows to '{tab_name}'...")
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{tab_name}'!A1",
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()
        time.sleep(API_CALL_DELAY)

        logger.info(f"Uploaded {len(df)} rows to '{tab_name}'")
        return True

    except Exception as e:
        logger.error(f"Failed to upload to '{tab_name}': {e}")
        import traceback

        logger.error(traceback.format_exc())
        return False


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================


def upload_all_periods(
    staging_dir: Path,
    dry_run: bool = False,
) -> Tuple[int, int]:
    logger.info("=" * 70)
    logger.info("UPLOADING CLEANED DATA FOR ALL PERIODS")
    if dry_run:
        logger.info("MODE: DRY RUN")
    logger.info("=" * 70)

    try:
        drive_service, sheets_service = connect_to_drive()
        logger.info("Connected to Google Drive")
    except Exception as e:
        logger.error(f"Failed to connect to Google Drive: {e}")
        return 0, 0

    manifest = load_manifest()

    all_sheets = []
    logger.info("Scanning folders for Google Sheets...")
    for idx, folder_id in enumerate(FOLDER_IDS, 1):
        sheets, calls_saved = get_sheets_for_folder(manifest, drive_service, folder_id)
        if calls_saved > 0:
            logger.debug(f"Folder {idx}: Using cached sheets")
        all_sheets.extend(sheets)

    logger.info(f"Found {len(all_sheets)} sheets across {len(FOLDER_IDS)} folders")

    cleaned_files = load_cleaned_files(staging_dir)

    if not cleaned_files:
        logger.error("No cleaned files found")
        return 0, 0

    total_uploads = 0
    successful_uploads = 0

    for file_type, filepath in cleaned_files.items():
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"PROCESSING: {filepath.name}")
        logger.info("=" * 70)

        try:
            df = pd.read_csv(filepath)
        except Exception as e:
            logger.error(f"Failed to load {filepath.name}: {e}")
            continue

        if df.empty:
            logger.warning(f"Skipping empty DataFrame: {filepath.name}")
            continue

        logger.info("Splitting data by period...")
        period_dfs = split_cleaned_data_by_period(df)

        if not period_dfs:
            logger.warning("No periods found, skipping this file")
            continue

        logger.info(f"Found {len(period_dfs)} periods: {sorted(period_dfs.keys())}")

        for period in sorted(period_dfs.keys()):
            period_df = period_dfs[period]

            spreadsheet_id = find_spreadsheet_for_period(period, all_sheets)

            if not spreadsheet_id:
                logger.warning(f"No spreadsheet found for period {period}, skipping")
                continue

            df_clean, cleaning_stats = clean_dataframe_for_upload(period_df)

            if file_type == "xuat_nhap_ton":
                tab_name = "Xuất nhập tồn"
            elif file_type == "bao_cao_tai_chinh":
                tab_name = "Báo cáo tài chính"
            else:
                tab_name = CLEANED_FILE_TO_TAB.get(file_type, filepath.stem)

            logger.info("")
            logger.info("-" * 70)
            logger.info(f"Period: {period}")
            logger.info(f"Tab: '{tab_name}'")
            logger.info(f"Sheet ID: {spreadsheet_id}")
            logger.info(f"Rows: {len(df_clean)}")

            if upload_to_spreadsheet(
                sheets_service,
                spreadsheet_id,
                tab_name,
                df_clean,
                replace=True,
                dry_run=dry_run,
            ):
                total_uploads += 1
                successful_uploads += 1
                logger.info(f"Uploaded to '{tab_name}' for period {period}")
            else:
                total_uploads += 1
                logger.error(f"Failed to upload to '{tab_name}' for period {period}")

            time.sleep(API_CALL_DELAY)

    save_manifest(manifest)

    logger.info("")
    logger.info("=" * 70)
    logger.info("UPLOAD SUMMARY")
    logger.info(f"  Total uploads attempted: {total_uploads}")
    logger.info(f"  Successful: {successful_uploads}")
    logger.info(f"  Failed: {total_uploads - successful_uploads}")
    logger.info(f"  Success rate: {successful_uploads / total_uploads * 100:.1f}%")
    if dry_run:
        logger.info("DRY RUN completed (no actual changes made)")
    else:
        logger.info("Upload process completed")
    logger.info("=" * 70)

    return total_uploads, successful_uploads


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Upload cleaned data to Google Sheets for ALL periods"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview uploads without actually modifying Google Sheets",
    )

    args = parser.parse_args()

    staging_dir = Path.cwd() / "data" / "01-staging" / "import_export"

    total, success = upload_all_periods(staging_dir, dry_run=args.dry_run)

    sys.exit(0 if success == total else 1)
