"""Remove formula and data tabs from all period spreadsheets.

This script removes:
- Formula tabs: Any tab containing "Đối chiếu dữ liệu" (partial match)
- Copy tabs: Any tab containing "Copy of Đối chiếu dữ liệu" (partial match, opt-in via --remove-copy-tabs)
- Data tabs: "Chi tiết nhập", "Chi tiết xuất", "Xuất nhập tồn", "Chi tiết chi phí" (exact match)

Use this before running copy_formula_tab_to_all_periods.py and upload_cleaned_to_sheets.py
to ensure clean slate for recreation.

Usage:
  # Dry run (preview only)
  uv run scripts/remove_uploaded_tabs.py --dry-run

  # Remove all tabs from all periods
  uv run scripts/remove_uploaded_tabs.py

  # Remove only from specific years
  uv run scripts/remove_uploaded_tabs.py --year 2025
  uv run scripts/remove_uploaded_tabs.py --year 2024,2025

  # Also remove "Copy of Đối chiếu dữ liệu" tabs
  uv run scripts/remove_uploaded_tabs.py --remove-copy-tabs
"""

import logging
import sys
import time

from src.modules.google_api import (
    API_CALL_DELAY,
    connect_to_drive,
    delete_sheet,
    get_sheet_id_by_name,
    get_sheet_tabs,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Tabs to remove (exact match)
DATA_TABS = [
    "Chi tiết nhập",
    "Chi tiết xuất",
    "Xuất nhập tồn",
    "Chi tiết chi phí",
]

# Formula tabs (partial match - any tab containing this string)
FORMULA_TAB_PATTERN = "Đối chiếu dữ liệu"
COPY_TAB_PATTERN = "Copy of Đối chiếu dữ liệu"


def validate_years(years_str: str) -> list[str]:
    """Validate and parse comma-separated year list."""
    years = [y.strip() for y in years_str.split(",")]
    for year in years:
        if not year.isdigit() or len(year) != 4:
            raise ValueError(f"Invalid year '{year}'. Must be 4-digit (e.g., 2025)")
    return years


def find_matching_spreadsheets(
    drive_service, years_filter: list[str] | None = None
) -> list[dict]:
    """Find all spreadsheets matching 'Xuất Nhập Tồn YYYY-MM' pattern.

    Args:
        drive_service: Google Drive API service object.
        years_filter: Optional list of years to filter (e.g., ["2025"]).

    Returns:
        List of spreadsheet dicts with id, name, year, month.
    """
    query = (
        "name contains 'Xuất Nhập Tồn' "
        "and mimeType='application/vnd.google-apps.spreadsheet' "
        "and trashed=false"
    )
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    all_sheets = results.get("files", [])

    matching_sheets = []
    for sheet in all_sheets:
        name = sheet["name"]
        # Expected pattern: "Xuất Nhập Tồn YYYY-MM" or similar
        parts = name.replace("Xuất Nhập Tồn", "").strip().split("-")
        if len(parts) == 2:
            year = parts[0].strip()
            month = parts[1].strip()
            # Validate it's a proper year-month pattern
            if year.isdigit() and len(year) == 4 and month.isdigit():
                if years_filter is None or year in years_filter:
                    matching_sheets.append(
                        {
                            "id": sheet["id"],
                            "name": name,
                            "year": year,
                            "month": month,
                        }
                    )
                    logger.debug(f"Matched: {name} ({year}-{month})")

    return matching_sheets


def get_tabs_to_delete(
    sheet_tabs: list[str], remove_copy_tabs: bool = False
) -> list[str]:
    """Determine which tabs should be deleted from a spreadsheet.

    Args:
        sheet_tabs: List of tab names in the spreadsheet.
        remove_copy_tabs: If True, also remove "Copy of Đối chiếu dữ liệu" tabs.

    Returns:
        List of tab names to delete.
    """
    tabs_to_delete = []

    for tab in sheet_tabs:
        if tab in DATA_TABS:
            tabs_to_delete.append(tab)
            continue

        if FORMULA_TAB_PATTERN in tab:
            tabs_to_delete.append(tab)
            continue

        if remove_copy_tabs and COPY_TAB_PATTERN in tab:
            tabs_to_delete.append(tab)

    return tabs_to_delete


def remove_tabs_from_spreadsheet(
    sheets_service,
    spreadsheet_id: str,
    spreadsheet_name: str,
    dry_run: bool = False,
    remove_copy_tabs: bool = False,
) -> tuple[int, int]:
    """Remove formula and data tabs from a spreadsheet.

    Args:
        sheets_service: Google Sheets API service object.
        spreadsheet_id: ID of the spreadsheet.
        spreadsheet_name: Name for logging.
        dry_run: If True, log without executing.
        remove_copy_tabs: If True, also remove "Copy of Đối chiếu dữ liệu" tabs.

    Returns:
        Tuple of (total_tabs_found, successful_deletions).
    """
    sheet_tabs = get_sheet_tabs(sheets_service, spreadsheet_id)
    tabs_to_delete = get_tabs_to_delete(sheet_tabs, remove_copy_tabs=remove_copy_tabs)

    if not tabs_to_delete:
        logger.debug(f"  No tabs to delete in {spreadsheet_name}")
        return 0, 0

    logger.info(f"  Found {len(tabs_to_delete)} tabs to delete: {tabs_to_delete}")

    if dry_run:
        logger.info(f"  [DRY RUN] Would delete {len(tabs_to_delete)} tab(s)")
        return len(tabs_to_delete), len(tabs_to_delete)

    successful = 0
    for tab_name in tabs_to_delete:
        sheet_id = get_sheet_id_by_name(sheets_service, spreadsheet_id, tab_name)
        if sheet_id is None:
            logger.warning(f"  Could not find sheet ID for '{tab_name}', skipping")
            continue

        if delete_sheet(sheets_service, spreadsheet_id, sheet_id):
            logger.info(f"  ✓ Deleted '{tab_name}'")
            successful += 1
        else:
            logger.error(f"  ✗ Failed to delete '{tab_name}'")

        time.sleep(API_CALL_DELAY)

    return len(tabs_to_delete), successful


def remove_all_tabs(
    dry_run: bool = False,
    years_filter: list[str] | None = None,
    remove_copy_tabs: bool = False,
) -> tuple[int, int]:
    """Remove tabs from all matching period spreadsheets.

    Args:
        dry_run: If True, preview without executing.
        years_filter: Optional list of years to filter.
        remove_copy_tabs: If True, also remove "Copy of Đối chiếu dữ liệu" tabs.

    Returns:
        Tuple of (total_spreadsheets, total_tabs_deleted).
    """
    logger.info("=" * 70)
    logger.info("REMOVING UPLOADED TABS FROM ALL PERIOD SPREADSHEETS")
    if dry_run:
        logger.info("MODE: DRY RUN (no changes made)")
    if years_filter:
        logger.info(f"YEAR FILTER: {', '.join(years_filter)}")
    logger.info("=" * 70)
    logger.info(f"Formula tabs (partial match): contains '{FORMULA_TAB_PATTERN}'")
    if remove_copy_tabs:
        logger.info(f"Copy tabs (partial match): contains '{COPY_TAB_PATTERN}'")
    logger.info(f"Data tabs (exact match): {', '.join(DATA_TABS)}")
    logger.info("=" * 70)

    try:
        drive_service, sheets_service = connect_to_drive()
        logger.info("Connected to Google Drive")
    except Exception as e:
        logger.error(f"Failed to connect to Google Drive: {e}")
        return 0, 0

    logger.info("Searching for 'Xuất Nhập Tồn' spreadsheets...")
    spreadsheets = find_matching_spreadsheets(drive_service, years_filter)

    if not spreadsheets:
        logger.warning("No matching spreadsheets found")
        return 0, 0

    logger.info(f"Found {len(spreadsheets)} spreadsheet(s)")

    total_spreadsheets = 0
    total_deletions = 0

    for sheet in sorted(spreadsheets, key=lambda s: (s["year"], int(s["month"]))):
        spreadsheet_name = sheet["name"]
        spreadsheet_id = sheet["id"]

        logger.info("")
        logger.info(f"  Processing: {spreadsheet_name}")
        logger.info(f"  Period: {sheet['year']}-{int(sheet['month']):02d}")
        logger.info(f"  ID: {spreadsheet_id}")

        total, successful = remove_tabs_from_spreadsheet(
            sheets_service,
            spreadsheet_id,
            spreadsheet_name,
            dry_run=dry_run,
            remove_copy_tabs=remove_copy_tabs,
        )

        if total > 0:
            total_spreadsheets += 1
            total_deletions += successful

    logger.info("")
    logger.info("=" * 70)
    logger.info("SUMMARY")
    logger.info(f"  Spreadsheets processed: {total_spreadsheets}")
    logger.info(f"  Total tabs deleted: {total_deletions}")
    if dry_run:
        logger.info("DRY RUN completed (no actual changes made)")
    else:
        logger.info("Remove operation completed")
    logger.info("=" * 70)

    return total_spreadsheets, total_deletions


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Remove formula and data tabs from all period spreadsheets",
        epilog="""
Examples:
  # Dry run preview
  uv run scripts/remove_uploaded_tabs.py --dry-run

  # Remove all tabs from all periods
  uv run scripts/remove_uploaded_tabs.py

  # Remove only from specific years
  uv run scripts/remove_uploaded_tabs.py --year 2025
  uv run scripts/remove_uploaded_tabs.py --year 2024,2025

  # Also remove "Copy of Đối chiếu dữ liệu" tabs
  uv run scripts/remove_uploaded_tabs.py --remove-copy-tabs
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview deletions without executing",
    )
    parser.add_argument(
        "--year",
        type=str,
        help="Filter to specific year(s). Comma-separated for multiple (e.g., 2025 or 2024,2025).",
    )
    parser.add_argument(
        "--remove-copy-tabs",
        action="store_true",
        help="Also remove tabs containing 'Copy of Đối chiếu dữ liệu'",
    )

    args = parser.parse_args()

    years_filter = None
    if args.year:
        try:
            years_filter = validate_years(args.year)
        except ValueError as e:
            logger.error(f"Invalid year argument: {e}")
            sys.exit(1)

    spreadsheets, deletions = remove_all_tabs(
        dry_run=args.dry_run,
        years_filter=years_filter,
        remove_copy_tabs=args.remove_copy_tabs,
    )

    sys.exit(0 if deletions > 0 else 1)
