"""Upload cleaned data to source Google Sheets for reconciliation.

This script:
1. Splits merged cleaned data by period (Year, Month)
2. Maps each period to its corresponding Google Sheet
3. Prepares data for upload (moves Ngày to first position, drops Năm/Tháng columns)
4. All columns are preserved as-is
5. Replaces existing tabs (or creates if doesn't exist)
6. Handles rate limiting with delays between API calls
7. Uploads all periods (2020-2025) in one run

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
    upload_dataframe_to_sheet,
    validate_months,
    validate_years,
)
import tomllib

# ============================================================================
# CONFIGURATION
# ============================================================================

with open("pipeline.toml", "rb") as f:
    CONFIG = tomllib.load(f)

# Support both old config (folder_ids array) and new config (root_folder_id)
# TODO: Remove fallback after full config migration
ier_config = CONFIG["sources"]["import_export_receipts"]
if "folder_ids" in ier_config:
    FOLDER_IDS = ier_config["folder_ids"]
else:
    FOLDER_IDS = [ier_config["root_folder_id"]]

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


def validate_file_types(file_types_str: str) -> List[str]:
    """Validate and parse comma-separated file type list.

    Args:
        file_types_str: Comma-separated string of file types.
                       Options: "nhap", "xuat", "xnt", "chiphin".

    Returns:
        List of validated file type keys.

    Raises:
        ValueError: If any file type is not valid.
    """
    file_type_map = {
        "nhap": "Chi tiết nhập",
        "xuat": "Chi tiết xuất",
        "xnt": "xuat_nhap_ton",
        "chiphin": "bao_cao_tai_chinh",
    }

    file_types = [ft.strip().lower() for ft in file_types_str.split(",")]

    validated_types = []
    for ft in file_types:
        if ft not in file_type_map:
            raise ValueError(
                f"Invalid file type '{ft}'. "
                f"Valid options: {', '.join(file_type_map.keys())}"
            )
        validated_types.append(file_type_map[ft])

    return validated_types


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
    """Find spreadsheet ID for a given period.

    Matches spreadsheet name pattern: "Xuất Nhập Tồn YYYY-MM"

    Args:
        period: Period string in format "YYYY_MM" (e.g., "2023_01")
        sheets_metadata: List of sheet metadata dicts from Drive API.

    Returns:
        Spreadsheet ID if found, None otherwise.
    """
    year, month = period.split("_")
    year_full = int(year)
    month_num = int(month)

    pattern = f"{year_full}-{month_num:02d}"

    for sheet in sheets_metadata:
        filename = sheet["name"]

        if pattern in filename:
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
    # Exclude refined files (from refine_product_master.py) and adjustment files
    xnt_files = [
        f
        for f in xnt_files
        if "adjustments" not in f.name.lower() and "refined" not in f.name.lower()
    ]
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


def get_raw_columns(df: pd.DataFrame, tab_name: str) -> List[int]:
    """Identify columns that should use RAW input to preserve formatting.

    Mã hàng (product code) uses RAW to preserve leading zeros.
    All other columns use USER_ENTERED for proper numeric parsing.

    Args:
        df: DataFrame being uploaded.
        tab_name: Name of the sheet tab.

    Returns:
        List of column indices (0-based) to upload as RAW.
    """
    raw_columns = []

    for idx, col in enumerate(df.columns):
        if col == "Mã hàng":
            raw_columns.append(idx)

    return raw_columns


def prepare_df_for_upload(df: pd.DataFrame, file_type: str) -> pd.DataFrame:
    """Prepare DataFrame for upload by reconfiguring columns.

    This function:
    1. Moves 'Ngày' column to first position (if exists)
    2. Drops 'Năm' and 'Tháng' columns (required for period splitting only)
    3. Cleans product names (strip spaces, collapse multiple spaces)

    Args:
        df: DataFrame to prepare for upload.
        file_type: Type of file being processed ('Chi tiết nhập', 'Chi tiết xuất',
                  'xuat_nhap_ton', 'bao_cao_tai_chinh').

    Returns:
        DataFrame with columns reconfigured for upload.
    """
    df_copy = df.copy()

    # Clean product names
    if "Tên hàng" in df_copy.columns:
        df_copy["Tên hàng"] = (
            df_copy["Tên hàng"]
            .astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
        )

    if "Ngày" in df_copy.columns:
        cols = df_copy.columns.tolist()
        cols.insert(0, cols.pop(cols.index("Ngày")))
        df_copy = df_copy[cols]

    period_cols = ["Năm", "Tháng"]
    df_copy = df_copy.drop(
        columns=[c for c in period_cols if c in df_copy.columns], errors="ignore"
    )

    logger.info(f"  Prepared {len(df_copy)} rows with columns: {list(df_copy.columns)}")

    return df_copy


def upload_to_spreadsheet(
    sheets_service,
    spreadsheet_id: str,
    tab_name: str,
    df: pd.DataFrame,
    dry_run: bool = False,
    move_to_first: bool = True,
) -> bool:
    """Upload DataFrame to Google Sheet preserving data types.

    Uses upload_dataframe_to_sheet with:
    - RAW mode for Mã hàng to preserve leading zeros
    - USER_ENTERED for other columns to parse numeric values correctly
    - Default: moves sheet to first position

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of the spreadsheet.
        tab_name: Name of the sheet tab.
        df: pandas DataFrame to upload.
        dry_run: If True, log without uploading.
        move_to_first: If True, move sheet to first position. Default True.

    Returns:
        True if successful (or dry run), False otherwise.
    """
    raw_columns = get_raw_columns(df, tab_name)

    if dry_run:
        logger.info(
            f"[DRY RUN] Would upload to {tab_name} in spreadsheet {spreadsheet_id}"
        )
        logger.info(f"[DRY RUN]   Rows: {len(df)}, Columns: {len(df.columns)}")
        logger.info(f"[DRY RUN]   Raw columns (preserve text): {raw_columns}")
        return True

    logger.info(f"Writing {len(df)} rows to '{tab_name}'...")
    logger.info(f"  Raw columns (preserve text): {raw_columns}")

    success = upload_dataframe_to_sheet(
        sheets_service,
        spreadsheet_id,
        tab_name,
        df,
        raw_columns=raw_columns if raw_columns else None,
        move_to_first=move_to_first,
    )

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
    years_filter: Optional[List[str]] = None,
    months_filter: Optional[List[str]] = None,
    file_types_filter: Optional[List[str]] = None,
    move_to_first: bool = True,
) -> Tuple[int, int]:
    logger.info("=" * 70)
    logger.info("UPLOADING CLEANED DATA FOR ALL PERIODS")
    if dry_run:
        logger.info("MODE: DRY RUN")
    if years_filter:
        logger.info(f"YEAR FILTER: {', '.join(years_filter)}")
    if months_filter:
        logger.info(f"MONTH FILTER: {', '.join(months_filter)}")
    if file_types_filter:
        readable_types = [CLEANED_FILE_TO_TAB.get(t, t) for t in file_types_filter]
        logger.info(f"FILE TYPE FILTER: {', '.join(readable_types)}")
    logger.info("=" * 70)

    try:
        drive_service, sheets_service = connect_to_drive()
        logger.info("Connected to Google Drive")
    except Exception as e:
        logger.error(f"Failed to connect to Google Drive: {e}")
        return 0, 0

    # Search across entire Drive for spreadsheets matching "Xuất Nhập Tồn YYYY-MM" pattern
    # This is more reliable than scanning folders since spreadsheets may be in various locations
    logger.info("Searching Drive for 'Xuất Nhập Tồn' spreadsheets...")

    query = (
        "name contains 'Xuất Nhập Tồn' "
        "and mimeType='application/vnd.google-apps.spreadsheet' "
        "and trashed=false"
    )
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    all_sheets = results.get("files", [])
    time.sleep(API_CALL_DELAY)

    logger.info(
        f"Found {len(all_sheets)} spreadsheets matching 'Xuất Nhập Tồn' pattern"
    )

    cleaned_files = load_cleaned_files(staging_dir)

    if not cleaned_files:
        logger.error("No cleaned files found")
        return 0, 0

    if file_types_filter:
        original_count = len(cleaned_files)
        cleaned_files = {
            ft: fp for ft, fp in cleaned_files.items() if ft in file_types_filter
        }
        filtered_count = len(cleaned_files)
        logger.info(
            f"Filtered to {filtered_count} file types (from {original_count}): "
            f"{list(cleaned_files.keys())}"
        )

    total_uploads = 0
    successful_uploads = 0

    for file_type, filepath in cleaned_files.items():
        logger.info("")
        logger.info("=" * 70)
        logger.info(f"PROCESSING: {filepath.name}")
        logger.info("=" * 70)

        try:
            # Read CSV with dtype to preserve Mã hàng as string (leading zeros)
            df = pd.read_csv(filepath, dtype={"Mã hàng": str})
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

        if years_filter or months_filter:
            original_count = len(period_dfs)
            filtered_periods = []

            for period in period_dfs.keys():
                period_year, period_month = period.split("_")

                year_match = period_year in years_filter if years_filter else True

                month_match = period_month in months_filter if months_filter else True

                if year_match and month_match:
                    filtered_periods.append(period)

            period_dfs = {period: period_dfs[period] for period in filtered_periods}
            filtered_count = len(period_dfs)

            if filtered_count == 0:
                filter_desc = []
                if years_filter:
                    filter_desc.append(f"years {', '.join(years_filter)}")
                if months_filter:
                    filter_desc.append(f"months {', '.join(months_filter)}")
                logger.warning(
                    f"No periods found for {', '.join(filter_desc)}, skipping this file"
                )
                continue

            filter_desc = []
            if years_filter:
                filter_desc.append(f"years {', '.join(years_filter)}")
            if months_filter:
                filter_desc.append(f"months {', '.join(months_filter)}")

            logger.info(
                f"Filtered to {filtered_count} periods (from {original_count}) for {', '.join(filter_desc)}"
            )
        else:
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

            period_df = prepare_df_for_upload(period_df, file_type)

            if upload_to_spreadsheet(
                sheets_service,
                spreadsheet_id,
                tab_name,
                period_df,
                dry_run=dry_run,
                move_to_first=move_to_first,
            ):
                total_uploads += 1
                successful_uploads += 1
                logger.info(f"Uploaded to '{tab_name}' for period {period}")
            else:
                total_uploads += 1
                logger.error(f"Failed to upload to '{tab_name}' for period {period}")

            # Rate limiting: Google Sheets has 60 write requests/minute per user
            # upload_dataframe_to_sheet makes multiple API calls per upload (~5-10 calls)
            # Using 2 second delay to stay safely under the limit
            time.sleep(2.0)

    logger.info("")
    logger.info("=" * 70)
    logger.info("UPLOAD SUMMARY")
    logger.info(f"  Total uploads attempted: {total_uploads}")
    logger.info(f"  Successful: {successful_uploads}")
    logger.info(f"  Failed: {total_uploads - successful_uploads}")
    if total_uploads > 0:
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
        description="Upload cleaned data to Google Sheets for ALL periods",
        epilog="""
 Examples:
   # Upload all periods and file types
   uv run src/modules/import_export_receipts/upload_cleaned_to_sheets.py

   # Upload only purchase receipts (Chi tiết nhập)
   uv run src/modules/import_export_receipts/upload_cleaned_to_sheets.py --file-type nhap

   # Upload only sales receipts (Chi tiết xuất)
   uv run src/modules/import_export_receipts/upload_cleaned_to_sheets.py --file-type xuat

   # Upload only inventory (Xuất nhập tồn)
   uv run src/modules/import_export_receipts/upload_cleaned_to_sheets.py --file-type xnt

   # Upload purchase and sales only
   uv run src/modules/import_export_receipts/upload_cleaned_to_sheets.py --file-type nhap,xuat

   # Upload only 2025 inventory
   uv run src/modules/import_export_receipts/upload_cleaned_to_sheets.py --file-type xnt --year 2025

   # Upload only January 2025 purchase receipts
   uv run src/modules/import_export_receipts/upload_cleaned_to_sheets.py --file-type nhap --year 2025 --month 1

   # Dry run for specific file type
   uv run src/modules/import_export_receipts/upload_cleaned_to_sheets.py --file-type nhap --year 2025 --dry-run
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview uploads without actually modifying Google Sheets",
    )
    parser.add_argument(
        "--file-type",
        type=str,
        help="Filter uploads to specific file type(s). Comma-separated for multiple types. "
        "Options: nhap (purchase receipts), xuat (sales receipts), xnt (inventory), chiphin (expense details).",
    )
    parser.add_argument(
        "--year",
        type=str,
        help="Filter uploads to specific year(s). Comma-separated for multiple years (e.g., 2025 or 2024,2025). Years must be 4-digit numbers.",
    )
    parser.add_argument(
        "--month",
        type=str,
        help="Filter uploads to specific month(s). Comma-separated for multiple months (e.g., 1,2 or 01,02). Months must be 1-12.",
    )

    args = parser.parse_args()

    file_types_filter = None
    if args.file_type:
        try:
            file_types_filter = validate_file_types(args.file_type)
        except ValueError as e:
            logger.error(f"Invalid file-type argument: {e}")
            sys.exit(1)

    years_filter = None
    if args.year:
        try:
            years_filter = validate_years(args.year)
        except ValueError as e:
            logger.error(f"Invalid year argument: {e}")
            sys.exit(1)

    months_filter = None
    if args.month:
        try:
            months_filter = validate_months(args.month)
        except ValueError as e:
            logger.error(f"Invalid month argument: {e}")
            sys.exit(1)

    staging_dir = Path.cwd() / "data" / "01-staging" / "import_export"

    total, success = upload_all_periods(
        staging_dir,
        dry_run=args.dry_run,
        years_filter=years_filter,
        months_filter=months_filter,
        file_types_filter=file_types_filter,
        move_to_first=True,
    )

    sys.exit(0 if success == total else 1)
