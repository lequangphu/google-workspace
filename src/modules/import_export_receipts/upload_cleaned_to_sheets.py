"""Upload cleaned data to source Google Sheets for reconciliation.

This script:
1. Splits merged cleaned data by period (Year, Month)
2. Maps each period to its corresponding Google Sheet
3. Replaces existing tabs (or creates if doesn't exist)
4. Handles rate limiting with delays between API calls
5. Uploads all periods (2020-2025) in one run

New Tab Names (replacing if exists):
- "Chi tiết nhập"     ← Chi tiết nhập*.csv (purchase)
- "Chi tiết xuất"     ← Chi tiết xuất*.csv (sale)
- "Xuất nhập tồn"     ← Xuất nhập tồn*.csv (inventory)
- "Chi tiết chi phí" ← Chi tiết chi phí*.csv (expense detail)

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
    API_CALL_DELAY,
    connect_to_drive,
    get_sheets_for_folder,
    load_manifest,
    save_manifest,
    write_sheet_data,
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
    "bao_cao_tai_chinh": "Chi tiết chi phí",
}

# ============================================================================
# LOGGING SETUP
# ============================================================================

logger = logging.getLogger(__name__)

# ============================================================================
# DATA PROCESSING FUNCTIONS
# ============================================================================


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

    xnt_files = list(staging_dir.glob("Xuất nhập tồn*.csv"))
    xnt_files = [f for f in xnt_files if "adjustments" not in f.name.lower()]
    if xnt_files:
        xnt_path = sorted(xnt_files)[-1]
        cleaned_files["xuat_nhap_ton"] = xnt_path
        logger.info(f"Found XNT file: {xnt_path.name}")

    financial_files = list(staging_dir.glob("Chi tiết chi phí*.csv"))
    if financial_files:
        financial_path = sorted(financial_files)[-1]
        cleaned_files["bao_cao_tai_chinh"] = financial_path
        logger.info(f"Found financial report: {financial_path.name}")

    return cleaned_files


def upload_to_spreadsheet(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    df: pd.DataFrame,
    dry_run: bool = False,
) -> bool:
    """Upload DataFrame to Google Sheet using write_sheet_data wrapper.

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of the spreadsheet.
        tab_name: Name of the sheet tab.
        df: pandas DataFrame to upload.
        dry_run: If True, log without uploading.

    Returns:
        True if successful (or dry run), False otherwise.
    """
    if dry_run:
        logger.info(
            f"[DRY RUN] Would upload to {tab_name} in spreadsheet {spreadsheet_id}"
        )
        logger.info(f"[DRY RUN]   Rows: {len(df)}, Columns: {len(df.columns)}")
        return True

    # Convert all values to strings, preserving empty values as empty strings
    values = [df.columns.tolist()]
    for _, row in df.iterrows():
        row_values = ["" if pd.isna(v) else str(v) for v in row]
        values.append(row_values)

    logger.info(f"Writing {len(df)} rows to '{tab_name}'...")
    success = write_sheet_data(sheets_service, spreadsheet_id, tab_name, values)

    if success:
        logger.info(f"Uploaded {len(df)} rows to '{tab_name}'")
    else:
        logger.error(f"Failed to upload to '{tab_name}'")

    return success


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

            if file_type == "xuat_nhap_ton":
                tab_name = "Xuất nhập tồn"
            elif file_type == "bao_cao_tai_chinh":
                tab_name = "Chi tiết chi phí"
            else:
                tab_name = CLEANED_FILE_TO_TAB.get(file_type, filepath.stem)

            logger.info("")
            logger.info("-" * 70)
            logger.info(f"Period: {period}")
            logger.info(f"Tab: '{tab_name}'")
            logger.info(f"Sheet ID: {spreadsheet_id}")
            logger.info(f"Rows: {len(period_df)}")

            if upload_to_spreadsheet(
                sheets_service,
                spreadsheet_id,
                tab_name,
                period_df,
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
