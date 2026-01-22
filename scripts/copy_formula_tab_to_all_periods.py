"""Copy formula tab to all period spreadsheets (one-time operation).

This script copies a tab with formulas from a source spreadsheet to all
"Xuất Nhập Tồn YYYY-MM" period spreadsheets. The copied tab preserves all formulas
and formatting. The tab is placed as the first tab in each target spreadsheet.

Source:
- Spreadsheet: 1O_XmlU_gAdPyyszu9jdFVfltjFv8OpqqAEmIBykVVzI
- Tab Name: Đối chiếu dữ liệu (looked up dynamically)

Target: All "Xuất Nhập Tồn YYYY-MM" spreadsheets in Google Drive

Behavior:
- If tab already exists in target spreadsheet → skip
- If tab doesn't exist → copy and rename to "Đối chiếu dữ liệu"

Usage:
  # Dry run (preview only)
  uv run scripts/copy_formula_tab_to_all_periods.py --dry-run

  # Execute actual copy
  uv run scripts/copy_formula_tab_to_all_periods.py
"""

import logging
import sys
import time
from typing import Dict, List, Optional, Tuple

from src.modules.google_api import (
    API_CALL_DELAY,
    connect_to_drive,
    copy_sheet_to_spreadsheet,
    get_sheet_id_by_name,
    rename_sheet,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

SOURCE_SPREADSHEET_ID = "1O_XmlU_gAdPyyszu9jdFVfltjFv8OpqqAEmIBykVVzI"
SOURCE_TAB_NAME = "Đối chiếu dữ liệu"


def get_source_sheet_id(
    sheets_service, spreadsheet_id: str, tab_name: str
) -> Optional[int]:
    """Find sheet ID by tab name in source spreadsheet.

    Args:
        sheets_service: Google Sheets API service.
        spreadsheet_id: Source spreadsheet ID.
        tab_name: Name of the tab to find.

    Returns:
        Sheet ID if found, None otherwise.
    """
    spreadsheet = (
        sheets_service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title))",
        )
        .execute()
    )

    for sheet in spreadsheet.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            sheet_id = props.get("sheetId")
            logger.info(f"Found source tab '{tab_name}' with ID: {sheet_id}")
            return sheet_id

    logger.error(f"Source tab '{tab_name}' not found in spreadsheet")
    return None


def find_spreadsheets_by_pattern(
    drive_service, pattern: str = "Xuất Nhập Tồn"
) -> List[Dict]:
    """Find all spreadsheets matching a name pattern.

    Args:
        drive_service: Google Drive API service.
        pattern: Name pattern to search for.

    Returns:
        List of dicts with 'id' and 'name' keys.
    """
    query = (
        f"name contains '{pattern}' "
        "and mimeType='application/vnd.google-apps.spreadsheet' "
        "and trashed=false"
    )
    results = (
        drive_service.files()
        .list(q=query, fields="files(id,name)", orderBy="name")
        .execute()
    )

    spreadsheets = results.get("files", [])
    logger.info(f"Found {len(spreadsheets)} spreadsheets matching '{pattern}'")

    return spreadsheets


def extract_period_from_name(name: str) -> Optional[str]:
    """Extract YYYY-MM period from spreadsheet name.

    Args:
        name: Spreadsheet name (e.g., "Xuất Nhập Tồn 2023-05")

    Returns:
        Period string (e.g., "2023_05") or None if not found.
    """
    import re

    # Match pattern like "Xuất Nhập Tồn 2023-05" or "Xuất Nhập Tồn 2023-5"
    match = re.search(r"Xuất Nhập Tồn\s+(\d{4})[-/](\d{1,2})", name)
    if match:
        year, month = match.groups()
        return f"{year}_{int(month):02d}"

    return None


def build_period_to_spreadsheet_map(
    spreadsheets: List[Dict],
) -> Dict[str, Dict]:
    """Build a mapping from period to spreadsheet info.

    Args:
        spreadsheets: List of spreadsheet dicts with 'id' and 'name'.

    Returns:
        Dict mapping period (e.g., "2023_05") -> {'id': spreadsheet_id, 'name': name}
    """
    period_map = {}

    for sheet in spreadsheets:
        period = extract_period_from_name(sheet["name"])
        if period:
            period_map[period] = sheet

    return period_map


def copy_to_all_periods(
    drive_service,
    sheets_service,
    source_sheet_id: int,
    source_name: str,
    year_filter: Optional[int] = None,
    dry_run: bool = False,
) -> Tuple[int, int]:
    """Copy formula tab to all "Xuất Nhập Tồn" period spreadsheets.

    For each target spreadsheet:
    - If tab already exists → skip
    - If tab doesn't exist → copy and rename to original name
    - Places tab in first position

    Args:
        drive_service: Google Drive API service.
        sheets_service: Google Sheets API service.
        source_sheet_id: ID of source tab to copy.
        source_name: Name of source tab.
        dry_run: If True, log without executing copy operations.

    Returns:
        Tuple of (total_attempts, successful_copies).
    """
    logger.info("=" * 70)
    logger.info("COPYING FORMULA TAB TO ALL PERIOD SPREADSHEETS")
    if dry_run:
        logger.info("MODE: DRY RUN (no changes made)")
    if year_filter:
        logger.info(f"FILTER: Year {year_filter} only")
    logger.info("=" * 70)
    logger.info(
        f"Source: Spreadsheet {SOURCE_SPREADSHEET_ID}, Tab '{source_name}' (ID: {source_sheet_id})"
    )

    # Find all "Xuất Nhập Tồn" spreadsheets
    spreadsheets = find_spreadsheets_by_pattern(drive_service)

    if not spreadsheets:
        logger.warning("No 'Xuất Nhập Tồn' spreadsheets found")
        return 0, 0

    # Build period mapping
    period_map = build_period_to_spreadsheet_map(spreadsheets)

    if year_filter:
        period_map = {
            k: v for k, v in period_map.items() if k.startswith(f"{year_filter}_")
        }
        logger.info(f"Filtered to year {year_filter}: {len(period_map)} spreadsheets")

    logger.info(f"Found {len(period_map)} period spreadsheets with valid periods")

    # Log periods found (sorted)
    periods = sorted(period_map.keys())
    logger.info(f"Periods: {', '.join(periods)}")

    total_attempts = 0
    successful_copies = 0

    for period in periods:
        sheet_info = period_map[period]
        sheet_id = sheet_info["id"]
        sheet_name = sheet_info["name"]

        # Skip source spreadsheet (copying to itself is pointless)
        if sheet_id == SOURCE_SPREADSHEET_ID:
            logger.info(f"  Skipping source spreadsheet: {sheet_name}")
            continue

        logger.info("")
        logger.info("-" * 70)
        logger.info(f"Period: {period} -> {sheet_name}")
        logger.info(f"Spreadsheet ID: {sheet_id}")
        logger.info("-" * 70)

        if dry_run:
            logger.info(
                f"  [DRY RUN] Would copy tab '{source_name}' to this spreadsheet"
            )
            total_attempts += 1
            successful_copies += 1
            continue

        if get_sheet_id_by_name(sheets_service, sheet_id, source_name):
            logger.info(f"  Skipping - tab '{source_name}' already exists")
            continue

        result = copy_sheet_to_spreadsheet(
            sheets_service,
            SOURCE_SPREADSHEET_ID,
            source_sheet_id,
            sheet_id,
        )

        if not result:
            logger.error("  Failed to copy")
            total_attempts += 1
            continue

        new_sheet_id = result.get("sheetId")
        logger.info(f"  Copied as new sheet {new_sheet_id}")

        if rename_sheet(sheets_service, sheet_id, new_sheet_id, source_name):
            logger.info(f"  Renamed to '{source_name}'")
        else:
            logger.error("  Failed to rename sheet")
            total_attempts += 1
            continue

        total_attempts += 1
        successful_copies += 1

        time.sleep(API_CALL_DELAY)

    logger.info("")
    logger.info("=" * 70)
    logger.info("SUMMARY")
    logger.info(f"  Total attempts: {total_attempts}")
    logger.info(f"  Successful: {successful_copies}")
    logger.info(f"  Failed: {total_attempts - successful_copies}")
    if total_attempts > 0:
        logger.info(f"  Success rate: {successful_copies / total_attempts * 100:.1f}%")
    if dry_run:
        logger.info("DRY RUN completed (no actual changes made)")
    else:
        logger.info("Copy operation completed")
    logger.info("=" * 70)

    return total_attempts, successful_copies


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Copy formula tab to all period spreadsheets (one-time operation)",
        epilog="""
Examples:
  # Dry run
  uv run scripts/copy_formula_tab_to_all_periods.py --dry-run

  # Execute copy
  uv run scripts/copy_formula_tab_to_all_periods.py
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--year",
        type=int,
        help="Process only spreadsheets from this year (e.g., 2024)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview copy operations without executing",
    )

    args = parser.parse_args()

    try:
        drive_service, sheets_service = connect_to_drive()
        logger.info("Connected to Google Drive")
    except Exception as e:
        logger.error(f"Failed to connect to Google Drive: {e}")
        sys.exit(1)

    # Look up source sheet ID by tab name
    source_sheet_id = get_source_sheet_id(
        sheets_service, SOURCE_SPREADSHEET_ID, SOURCE_TAB_NAME
    )

    if source_sheet_id is None:
        logger.error(f"Could not find source tab '{SOURCE_TAB_NAME}'")
        sys.exit(1)

    total, success = copy_to_all_periods(
        drive_service,
        sheets_service,
        source_sheet_id,
        SOURCE_TAB_NAME,
        year_filter=args.year,
        dry_run=args.dry_run,
    )

    sys.exit(0 if success == total else 1)
