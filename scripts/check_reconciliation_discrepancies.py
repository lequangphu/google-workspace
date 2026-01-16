"""Check reconciliation discrepancies in Google Sheets.

This script scans all period spreadsheets in configured folders, reads the
"Đối chiếu dữ liệu" tab, and flags spreadsheets with any 'Chênh lệch*' column
values > 0.

Usage:
    # Scan all spreadsheets
    uv run scripts/check_reconciliation_discrepancies.py

    # Dry run preview (no API calls)
    uv run scripts/check_reconciliation_discrepancies.py --dry-run

    # Scan only 2025 data
    uv run scripts/check_reconciliation_discrepancies.py --year 2025

    # Scan January and February 2025 only
    uv run scripts/check_reconciliation_discrepancies.py --year 2025 --month 1,2

    # Scan January (all years)
    uv run scripts/check_reconciliation_discrepancies.py --month 1

    # Debug mode: show values that trigger discrepancy flag
    uv run scripts/check_reconciliation_discrepancies.py --debug
"""

import logging
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import tomllib

from src.modules.google_api import (
    API_CALL_DELAY,
    connect_to_drive,
    get_sheets_for_folder,
    load_manifest,
    parse_file_metadata,
    read_sheet_data,
    save_manifest,
)
from src.modules.google_api import get_sheet_tabs

logger = logging.getLogger(__name__)

# Tab name to check for discrepancies
TAB_NAME = "Đối chiếu dữ liệu"

# Pattern for discrepancy columns
DISCREPANCY_PATTERN = re.compile(r"^Chênh lệch.*")

# Tolerance for floating point comparison (values below this are treated as 0)
DISCREPANCY_TOLERANCE = 1e-5


def load_folder_ids() -> List[str]:
    """Load folder IDs from pipeline.toml."""
    config_path = Path("pipeline.toml")
    with open(config_path, "rb") as f:
        config = tomllib.load(f)
    return config["sources"]["import_export_receipts"]["folder_ids"]


def find_discrepancy_columns(headers: List[str]) -> List[str]:
    """Find all columns matching 'Chênh lệch*' pattern.

    Args:
        headers: List of column names from first row.

    Returns:
        List of column names matching pattern ^Chênh lệch.* (case-sensitive).
    """
    return [col for col in headers if DISCREPANCY_PATTERN.match(col)]


def parse_numeric_value(value) -> Optional[float]:
    """Parse a cell value to float for comparison.

    Args:
        value: Cell value (may be string, number, empty).

    Returns:
        Parsed float value, or None if unparseable.
    """
    if value is None or value == "":
        return 0.0

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return 0.0
        try:
            return float(value.replace(",", "").replace(" ", ""))
        except ValueError:
            return None

    return None


def has_discrepancies_in_column(
    values: List, column_name: str, debug: bool = False
) -> Tuple[bool, int, List[float]]:
    """Check if any value > 0 in a discrepancy column.

    Args:
        values: List of cell values (first element is column name).
        column_name: Name of the column (for debug logging).
        debug: If True, log values that are flagged.

    Returns:
        Tuple of (has_discrepancies: bool, count: int, flagged_values: list).
    """
    count = 0
    flagged_values = []
    # Skip header (first element)
    for idx, value in enumerate(values[1:], start=2):
        parsed = parse_numeric_value(value)
        if parsed is not None and abs(parsed) > DISCREPANCY_TOLERANCE:
            count += 1
            flagged_values.append((idx, parsed))
            if debug:
                logger.debug(
                    f"    Row {idx}, Column '{column_name}': value={value}, parsed={parsed}"
                )

    return count > 0, count, flagged_values


def check_spreadsheet_for_discrepancies(
    sheets_service, spreadsheet_id: str, spreadsheet_name: str, debug: bool = False
) -> Dict:
    """Check a single spreadsheet for discrepancies.

    Args:
        sheets_service: Google Sheets API service.
        spreadsheet_id: ID of spreadsheet to check.
        spreadsheet_name: Name of spreadsheet.
        debug: If True, log detailed values for discrepancies.

    Returns:
        Dict with keys:
            - spreadsheet_id: str
            - spreadsheet_name: str
            - success: bool
            - tab_exists: bool
            - has_discrepancies: bool
            - discrepancy_columns: list of column names with discrepancies
            - discrepancy_counts: dict mapping column -> count of >0 rows
            - discrepancy_details: dict mapping column -> list of (row, value) tuples
            - error: str (if success=False)
    """
    result = {
        "spreadsheet_id": spreadsheet_id,
        "spreadsheet_name": spreadsheet_name,
        "success": True,
        "tab_exists": False,
        "has_discrepancies": False,
        "discrepancy_columns": [],
        "discrepancy_counts": {},
        "discrepancy_details": {},
        "error": None,
    }

    try:
        # Check if tab exists
        tabs = get_sheet_tabs(sheets_service, spreadsheet_id)

        if TAB_NAME not in tabs:
            logger.warning(f"  Tab '{TAB_NAME}' not found in '{spreadsheet_name}'")
            result["tab_exists"] = False
            return result

        result["tab_exists"] = True

        # Read tab data
        data = read_sheet_data(sheets_service, spreadsheet_id, TAB_NAME)

        if not data:
            logger.warning(f"  No data in '{TAB_NAME}' tab of '{spreadsheet_name}'")
            return result

        if len(data) < 2:
            logger.warning(f"  '{TAB_NAME}' tab in '{spreadsheet_name}' has no data rows")
            return result

        # Find discrepancy columns
        headers = data[0]
        discrepancy_cols = find_discrepancy_columns(headers)

        if not discrepancy_cols:
            logger.debug(f"  No discrepancy columns found in '{spreadsheet_name}'")
            return result

        # Check each discrepancy column
        for col_name in discrepancy_cols:
            col_idx = headers.index(col_name)
            col_values = [row[col_idx] if col_idx < len(row) else "" for row in data]

            has_disc, count, flagged = has_discrepancies_in_column(
                col_values, col_name, debug
            )

            if has_disc:
                result["has_discrepancies"] = True
                result["discrepancy_columns"].append(col_name)
                result["discrepancy_counts"][col_name] = count
                result["discrepancy_details"][col_name] = flagged

                if debug and flagged:
                    logger.info(
                        f"  Discrepancy in '{col_name}': {count} row(s) flagged"
                    )

        return result

    except Exception as e:
        logger.error(f"  Error checking '{spreadsheet_name}': {e}")
        result["success"] = False
        result["error"] = str(e)
        return result


def scan_all_spreadsheets(
    folder_ids: List[str],
    dry_run: bool = False,
    debug: bool = False,
    years_filter: Optional[List[str]] = None,
    months_filter: Optional[List[str]] = None,
) -> Tuple[Dict, List[Dict]]:
    """Scan all spreadsheets for discrepancies.

    Args:
        folder_ids: List of folder IDs from pipeline.toml.
        dry_run: If True, preview without API calls.
        debug: If True, log detailed values for discrepancies.
        years_filter: Optional list of years (e.g., ["2025"]).
        months_filter: Optional list of months (e.g., ["01"]).

    Returns:
        Tuple of (scan_stats dict, discrepancy_results list).
    """
    logger.info("=" * 70)
    logger.info("RECONCILIATION DISCREPANCY SCAN")
    if dry_run:
        logger.info("MODE: DRY RUN")
    if debug:
        logger.info("DEBUG MODE: Will log discrepancy values")
    if years_filter:
        logger.info(f"YEAR FILTER: {', '.join(years_filter)}")
    if months_filter:
        logger.info(f"MONTH FILTER: {', '.join(months_filter)}")
    logger.info("=" * 70)

    # Stats tracking
    stats = {
        "folders_scanned": 0,
        "spreadsheets_scanned": 0,
        "spreadsheets_with_tab": 0,
        "spreadsheets_missing_tab": 0,
        "spreadsheets_with_discrepancies": 0,
        "spreadsheets_no_discrepancies": 0,
        "spreadsheets_read_errors": 0,
        "total_discrepancy_columns": 0,
    }

    discrepancy_results: List[Dict] = []

    if dry_run:
        logger.info("[DRY RUN] Would connect to Google APIs")
        logger.info(f"[DRY RUN] Would scan {len(folder_ids)} folder(s)")
    else:
        try:
            drive_service, sheets_service = connect_to_drive()
            logger.info("Connected to Google Drive")
        except Exception as e:
            logger.error(f"Failed to connect to Google Drive: {e}")
            return stats, discrepancy_results

    manifest = load_manifest()

    for folder_id in folder_ids:
        stats["folders_scanned"] += 1
        logger.info("")
        logger.info("-" * 70)
        logger.info(f"Scanning folder: {folder_id}")
        logger.info("-" * 70)

        if dry_run:
            sheets = []
        else:
            sheets, calls_saved = get_sheets_for_folder(manifest, drive_service, folder_id)
            if calls_saved > 0:
                logger.debug(f"  Using cached sheets (saved {calls_saved} API call(s))")

        if not sheets:
            logger.warning("  No spreadsheets found in folder")
            continue

        logger.info(f"  Found {len(sheets)} spreadsheet(s)")

        for sheet in sheets:
            spreadsheet_id = sheet["id"]
            spreadsheet_name = sheet["name"]

            # Parse period from filename for filtering
            file_year, file_month = parse_file_metadata(spreadsheet_name)

            # Apply filters
            if years_filter and file_year is not None:
                if str(file_year) not in years_filter:
                    continue

            if months_filter and file_month is not None:
                month_str = f"{file_month:02d}"
                if month_str not in months_filter:
                    continue

            stats["spreadsheets_scanned"] += 1

            if dry_run:
                logger.info(f"  [DRY RUN] Would check: {spreadsheet_name}")
                continue

            logger.info(f"  Checking: {spreadsheet_name}")

            result = check_spreadsheet_for_discrepancies(
                sheets_service, spreadsheet_id, spreadsheet_name, debug=debug
            )

            # Update stats
            if not result["success"]:
                stats["spreadsheets_read_errors"] += 1
            elif not result["tab_exists"]:
                stats["spreadsheets_missing_tab"] += 1
            elif result["has_discrepancies"]:
                stats["spreadsheets_with_discrepancies"] += 1
                stats["total_discrepancy_columns"] += len(result["discrepancy_columns"])
                discrepancy_results.append(result)
            else:
                stats["spreadsheets_no_discrepancies"] += 1

            if result["tab_exists"]:
                stats["spreadsheets_with_tab"] += 1

            time.sleep(API_CALL_DELAY)

    save_manifest(manifest)

    return stats, discrepancy_results


def print_summary_report(stats: Dict, discrepancy_results: List[Dict]) -> None:
    """Print summary report to console."""
    print("")
    print("=" * 70)
    print("RECONCILIATION DISCREPANCY SCAN SUMMARY")
    print("=" * 70)
    print(f"{'Folders scanned:':<35} {stats['folders_scanned']}")
    print(f"{'Spreadsheets scanned:':<35} {stats['spreadsheets_scanned']}")
    print(f"{'  With TAB_NAME tab:':<35} {stats['spreadsheets_with_tab']}")
    print(f"{'  ✗ Missing tab:':<35} {stats['spreadsheets_missing_tab']}")
    print(f"{'  ✓ No discrepancies:':<35} {stats['spreadsheets_no_discrepancies']}")
    print(f"{'  ⚠ WITH DISCREPANCIES:':<35} {stats['spreadsheets_with_discrepancies']}")
    print(f"{'  ✗ Read errors:':<35} {stats['spreadsheets_read_errors']}")
    print(f"{'Total discrepancy columns found:':<35} {stats['total_discrepancy_columns']}")
    print("=" * 70)


def print_detailed_report(discrepancy_results: List[Dict], debug: bool = False) -> None:
    """Print detailed report of spreadsheets with discrepancies."""
    if not discrepancy_results:
        print("\n✓ No discrepancies found in any spreadsheet.")
        return

    print("")
    print("=" * 70)
    print("DETAILED DISCREPANCY REPORT")
    print("=" * 70)

    for result in discrepancy_results:
        print("")
        print("-" * 70)
        print("⚠ SPREADSHEET WITH DISCREPANCIES")
        print(f"  Name: {result['spreadsheet_name']}")
        print(f"  ID:   {result['spreadsheet_id']}")
        print("")
        print("Discrepancy columns:")
        for col_name, count in result["discrepancy_counts"].items():
            print(f"  • {col_name}: {count} row(s) with value > 0")
            if debug and "discrepancy_details" in result:
                details = result["discrepancy_details"].get(col_name, [])
                if details:
                    print(f"    Flagged rows: {details[:10]}")  # Show first 10
                    if len(details) > 10:
                        print(f"    ... and {len(details) - 10} more")

    print("")
    print("=" * 70)


def validate_years(years_str: str) -> List[str]:
    """Validate and parse comma-separated year list."""
    years = [y.strip() for y in years_str.split(",")]
    for year in years:
        if not year.isdigit() or len(year) != 4:
            raise ValueError(
                f"Invalid year '{year}'. Years must be 4-digit numbers (e.g., 2024, 2025)"
            )
    return sorted(years)


def validate_months(months_str: str) -> List[str]:
    """Validate and parse comma-separated month list."""
    months = [m.strip() for m in months_str.split(",")]
    validated_months = []
    for month in months:
        if not month.isdigit():
            raise ValueError(
                f"Invalid month '{month}'. Months must be numbers (e.g., 1, 2, 12)"
            )
        month_num = int(month)
        if month_num < 1 or month_num > 12:
            raise ValueError(
                f"Invalid month '{month}'. Months must be between 1 and 12 (e.g., 1, 2, 12)"
            )
        validated_months.append(f"{month_num:02d}")
    return sorted(validated_months)


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Check reconciliation discrepancies in Google Sheets",
        epilog="""
Examples:
  # Scan all spreadsheets
  uv run scripts/check_reconciliation_discrepancies.py

  # Dry run preview (no API calls)
  uv run scripts/check_reconciliation_discrepancies.py --dry-run

  # Scan only 2025 data
  uv run scripts/check_reconciliation_discrepancies.py --year 2025

  # Scan January and February 2025
  uv run scripts/check_reconciliation_discrepancies.py --year 2025 --month 1,2

  # Scan January (all years)
  uv run scripts/check_reconciliation_discrepancies.py --month 1

  # Debug mode: show discrepancy values
  uv run scripts/check_reconciliation_discrepancies.py --year 2020 --debug
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview scan without actual API calls",
    )
    parser.add_argument(
        "--year",
        type=str,
        help="Filter to specific year(s). Comma-separated for multiple years (e.g., 2025 or 2024,2025).",
    )
    parser.add_argument(
        "--month",
        type=str,
        help="Filter to specific month(s). Comma-separated for multiple months (e.g., 1,2 or 01,02).",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Debug mode: log values that trigger discrepancy flag",
    )

    args = parser.parse_args()

    # Set debug log level if needed
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Parse filters
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

    # Load folder IDs and run scan
    folder_ids = load_folder_ids()
    logger.info(f"Loaded {len(folder_ids)} folder ID(s) from pipeline.toml")

    stats, discrepancy_results = scan_all_spreadsheets(
        folder_ids,
        dry_run=args.dry_run,
        debug=args.debug,
        years_filter=years_filter,
        months_filter=months_filter,
    )

    # Print reports
    print_summary_report(stats, discrepancy_results)
    print_detailed_report(discrepancy_results, debug=args.debug)

    if args.dry_run:
        logger.info("DRY RUN completed (no actual changes made)")

    sys.exit(0)
