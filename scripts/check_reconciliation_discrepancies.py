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

import json
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
    find_receipt_sheets,
    get_sheet_tabs,
    read_sheet_data,
    validate_months,
    validate_years,
)

logger = logging.getLogger(__name__)

# Tab name to check for discrepancies
TAB_NAME = "Đối chiếu dữ liệu"

# Pattern for discrepancy columns
DISCREPANCY_PATTERN = re.compile(r"^Chênh lệch.*")

# Tolerance for floating point comparison (values below this are treated as 0)
DISCREPANCY_TOLERANCE = 1e-5

CONTEXT_COLUMNS = [
    "Mã hàng",
    "Tên hàng",
    "Mã NCC",
    "Tên NCC",
    "Số lượng",
    "Đơn giá",
    "Thành tiền",
    "Ngày",
    "Số CT",
]


def load_folder_config() -> Dict[str, str]:
    """Load folder configuration from pipeline.toml.

    Returns:
        Dict with 'root_folder_id' and 'receipts_subfolder_name'.
    """
    config_path = Path("pipeline.toml")
    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    ie_config = config["sources"]["import_export_receipts"]
    return {
        "root_folder_id": ie_config["root_folder_id"],
        "receipts_subfolder_name": ie_config["receipts_subfolder_name"],
    }


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


def calculate_discrepancy_stats(
    flagged_values: List[Tuple[int, float, Dict[str, str]]],
) -> Dict:
    values = [v[1] for v in flagged_values]
    if not values:
        return {"total": 0, "min": 0, "max": 0, "sum": 0}

    return {
        "total": len(values),
        "min": min(values),
        "max": max(values),
        "sum": sum(values),
    }


def has_discrepancies_in_column(
    values: List,
    column_name: str,
    row_data: List[List],
    context_col_indices: Dict[str, int],
    debug: bool = False,
) -> Tuple[bool, int, List[Tuple[int, float, Dict[str, str]]]]:
    """Check if any value > 0 in a discrepancy column.

    Args:
        values: List of cell values (first element is column name).
        column_name: Name of the column (for debug logging).
        row_data: Full row data for extracting context.
        context_col_indices: Dict mapping context column names to indices.
        debug: If True, log values that are flagged.

    Returns:
        Tuple of (has_discrepancies: bool, count: int, flagged_values: list).
        flagged_values is a list of (row_idx, parsed_value, context_dict) tuples.
    """
    count = 0
    flagged_values = []
    for row_idx, value in enumerate(values[1:], start=2):
        parsed = parse_numeric_value(value)
        if parsed is not None and abs(parsed) > DISCREPANCY_TOLERANCE:
            count += 1
            data_idx = row_idx - 2  # Convert 1-based row index to 0-based data index
            context = {}
            if data_idx < len(row_data):
                for col_name, col_idx in context_col_indices.items():
                    if col_idx < len(row_data[data_idx]):
                        context[col_name] = row_data[data_idx][col_idx]
            flagged_values.append((row_idx, parsed, context))
            if debug:
                logger.debug(
                    f"    Row {row_idx}, Column '{column_name}': value={value}, parsed={parsed}"
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
            - discrepancy_details: dict mapping column -> list of (row, value, context) tuples
            - discrepancy_stats: dict mapping column -> {total, min, max, sum}
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
        "discrepancy_stats": {},
        "error": None,
    }

    try:
        tabs = get_sheet_tabs(sheets_service, spreadsheet_id)

        if TAB_NAME not in tabs:
            logger.warning(f"  Tab '{TAB_NAME}' not found in '{spreadsheet_name}'")
            result["tab_exists"] = False
            return result

        result["tab_exists"] = True

        data = read_sheet_data(sheets_service, spreadsheet_id, TAB_NAME)

        if not data:
            logger.warning(f"  No data in '{TAB_NAME}' tab of '{spreadsheet_name}'")
            return result

        if len(data) < 2:
            logger.warning(
                f"  '{TAB_NAME}' tab in '{spreadsheet_name}' has no data rows"
            )
            return result

        headers = data[0]
        discrepancy_cols = find_discrepancy_columns(headers)

        if not discrepancy_cols:
            logger.debug(f"  No discrepancy columns found in '{spreadsheet_name}'")
            return result

        context_col_indices = {}
        for ctx_col in CONTEXT_COLUMNS:
            if ctx_col in headers:
                context_col_indices[ctx_col] = headers.index(ctx_col)

        row_data = data[1:]

        for col_name in discrepancy_cols:
            col_idx = headers.index(col_name)
            col_values = [row[col_idx] if col_idx < len(row) else "" for row in data]

            has_disc, count, flagged = has_discrepancies_in_column(
                col_values, col_name, row_data, context_col_indices, debug
            )

            if has_disc:
                result["has_discrepancies"] = True
                result["discrepancy_columns"].append(col_name)
                result["discrepancy_counts"][col_name] = count
                result["discrepancy_details"][col_name] = flagged
                result["discrepancy_stats"][col_name] = calculate_discrepancy_stats(
                    flagged
                )

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

        result["tab_exists"] = True

        # Read tab data
        data = read_sheet_data(sheets_service, spreadsheet_id, TAB_NAME)

        if not data:
            logger.warning(f"  No data in '{TAB_NAME}' tab of '{spreadsheet_name}'")
            return result

        if len(data) < 2:
            logger.warning(
                f"  '{TAB_NAME}' tab in '{spreadsheet_name}' has no data rows"
            )
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
    folder_config: Dict[str, str],
    dry_run: bool = False,
    debug: bool = False,
    years_filter: Optional[List[str]] = None,
    months_filter: Optional[List[str]] = None,
) -> Tuple[Dict, List[Dict]]:
    """Scan all spreadsheets for discrepancies.

    Args:
        folder_config: Dict with 'root_folder_id' and 'receipts_subfolder_name'.
        dry_run: If True, preview without API calls.
        debug: If True, log detailed values for discrepancies.
        years_filter: Optional list of years as strings (e.g., ["2025"]).
        months_filter: Optional list of months as strings (e.g., ["01"]).

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
    else:
        try:
            drive_service, sheets_service = connect_to_drive()
            logger.info("Connected to Google Drive")
        except Exception as e:
            logger.error(f"Failed to connect to Google Drive: {e}")
            return stats, discrepancy_results

    # Convert string filters to int lists for find_receipt_sheets()
    year_list = [int(y) for y in years_filter] if years_filter else None
    month_list = [int(m) for m in months_filter] if months_filter else None

    # Discover spreadsheets using shared function
    logger.info("")
    logger.info("=" * 70)
    logger.info("Discovering receipts spreadsheets")
    logger.info("=" * 70)

    try:
        sheets = find_receipt_sheets(
            drive_service,
            folder_config,
            year_list,
            month_list,
        )
    except FileNotFoundError as e:
        logger.error(f"{e}")
        return stats, discrepancy_results

    if not sheets:
        logger.warning("No spreadsheets found matching filters")
        return stats, discrepancy_results

    logger.info(f"Found {len(sheets)} spreadsheet(s)")

    # Process each sheet
    for sheet in sheets:
        spreadsheet_id = sheet["id"]
        spreadsheet_name = sheet["name"]

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
    print(
        f"{'Total discrepancy columns found:':<35} {stats['total_discrepancy_columns']}"
    )

    if discrepancy_results:
        total_discrepancy_rows = sum(
            sum(r["discrepancy_counts"].values()) for r in discrepancy_results
        )
        print(f"{'Total rows with discrepancies:':<35} {total_discrepancy_rows}")

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
    print(
        f"{'Total discrepancy columns found:':<35} {stats['total_discrepancy_columns']}"
    )
    print("=" * 70)


def export_discrepancies_to_json(
    discrepancy_results: List[Dict], output_path: Path
) -> None:
    export_data = []
    for result in discrepancy_results:
        export_entry = {
            "spreadsheet_id": result["spreadsheet_id"],
            "spreadsheet_name": result["spreadsheet_name"],
            "discrepancy_columns": result["discrepancy_columns"],
            "discrepancy_counts": result["discrepancy_counts"],
            "discrepancy_stats": result["discrepancy_stats"],
            "discrepancy_details": result["discrepancy_details"],
        }
        export_data.append(export_entry)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)

    logger.info(f"Exported discrepancy details to: {output_path}")


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

        for col_name in result["discrepancy_columns"]:
            stats = result["discrepancy_stats"].get(col_name, {})
            count = result["discrepancy_counts"].get(col_name, 0)
            print(f"  Column: {col_name}")
            print(f"    Rows with discrepancies: {count}")
            if stats:
                print(
                    f"    Statistics: min={stats['min']:.2f}, max={stats['max']:.2f}, sum={stats['sum']:.2f}"
                )

            if debug and "discrepancy_details" in result:
                details = result["discrepancy_details"].get(col_name, [])
                if details:
                    print("    Discrepancy details (showing first 5):")
                    for row_idx, value, context in details[:5]:
                        context_str = ", ".join(
                            [f"{k}={v}" for k, v in context.items() if v]
                        )
                        print(
                            f"      Row {row_idx}: value={value:.2f}, [{context_str}]"
                        )
                    if len(details) > 5:
                        print(f"      ... and {len(details) - 5} more")
            print("")

    print("")
    print("=" * 70)


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

  # Export discrepancies to JSON for analysis
  uv run scripts/check_reconciliation_discrepancies.py --output-json discrepancies.json
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
    parser.add_argument(
        "--output-json",
        type=str,
        metavar="PATH",
        help="Export detailed discrepancy results to JSON file at PATH",
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

    # Load folder configuration and run scan
    folder_config = load_folder_config()
    logger.info("Loaded folder configuration from pipeline.toml")

    stats, discrepancy_results = scan_all_spreadsheets(
        folder_config,
        dry_run=args.dry_run,
        debug=args.debug,
        years_filter=years_filter,
        months_filter=months_filter,
    )

    # Print reports
    print_summary_report(stats, discrepancy_results)
    print_detailed_report(discrepancy_results, debug=args.debug)

    if args.output_json and not args.dry_run:
        output_path = Path(args.output_json)
        export_discrepancies_to_json(discrepancy_results, output_path)

    if args.dry_run:
        logger.info("DRY RUN completed (no actual changes made)")

    sys.exit(0)
