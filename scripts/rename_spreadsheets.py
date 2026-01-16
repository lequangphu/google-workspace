"""One-time script to rename spreadsheets from old to new naming pattern.

OLD pattern: XUẤT NHẬP TỒN TỔNG TMM.YY (e.g., "XUẤT NHẬP TỒN TỔNG T01.23")
NEW pattern: Xuất Nhập Tồn YYYY-MM (e.g., "Xuất Nhập Tồn 2023-01")

Usage:
    # Preview all renames (dry-run, default)
    uv run scripts/rename_spreadsheets.py

    # Preview only specific year
    uv run scripts/rename_spreadsheets.py --year 2024

    # Execute renames for specific year
    uv run scripts/rename_spreadsheets.py --year 2024 --execute

    # Execute all years
    uv run scripts/rename_spreadsheets.py --execute

    # Execute with backup file creation
    uv run scripts/rename_spreadsheets.py --year 2024 --execute --backup

IMPORTANT: After renaming, update upload_cleaned_to_sheets.py::find_spreadsheet_for_period()
to support both old and new patterns during transition period.
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import tomllib

from src.modules.google_api import (
    API_CALL_DELAY,
    connect_to_drive,
    get_sheets_for_folder,
    load_manifest,
    save_manifest,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Pattern for old spreadsheet names
OLD_PATTERN = re.compile(r"^XUẤT NHẬP TỒN TỔNG T(\d{1,2})\.(\d{2})$")


def parse_config() -> dict:
    """Load configuration from pipeline.toml."""
    with open("pipeline.toml", "rb") as f:
        return tomllib.load(f)


def parse_old_filename(file_name: str) -> Optional[Tuple[int, int]]:
    """Extract year and month from old filename pattern.

    Args:
        file_name: e.g., "XUẤT NHẬP TỒN TỔNG T01.23"

    Returns:
        Tuple of (year, month) or None if pattern doesn't match.
    """
    match = OLD_PATTERN.match(file_name)
    if match:
        month = int(match.group(1))
        year = 2000 + int(match.group(2))
        return year, month
    return None


def generate_new_filename(year: int, month: int) -> str:
    """Generate new filename from year and month.

    Args:
        year: 4-digit year (e.g., 2023)
        month: Month number 1-12 (e.g., 1)

    Returns:
        New filename: "Xuất Nhập Tồn YYYY-MM"
    """
    return f"Xuất Nhập Tồn {year:04d}-{month:02d}"


def find_renamable_spreadsheets(
    drive_service,
    folder_ids: List[str],
    manifest: dict,
    year_filter: Optional[List[str]] = None,
) -> List[Dict]:
    """Find all spreadsheets matching old pattern in configured folders.

    Args:
        drive_service: Google Drive API service object.
        folder_ids: List of folder IDs to scan.
        manifest: Manifest dict for caching.
        year_filter: Optional list of years to include (e.g., ["2024", "2025"]).

    Returns:
        List of dicts with keys: id, old_name, new_name, year, month.
    """
    renamable = []
    seen_names = set()  # Avoid duplicates across folders

    for folder_id in folder_ids:
        sheets, calls_saved = get_sheets_for_folder(manifest, drive_service, folder_id)
        if calls_saved > 0:
            logger.debug(f"Folder: cached result used")

        for sheet in sheets:
            name = sheet["name"]

            # Skip duplicates (same file in multiple folders)
            if name in seen_names:
                continue

            parsed = parse_old_filename(name)
            if parsed:
                year, month = parsed

                # Apply year filter if specified
                if year_filter and str(year) not in year_filter:
                    continue

                new_name = generate_new_filename(year, month)

                renamable.append(
                    {
                        "id": sheet["id"],
                        "old_name": name,
                        "new_name": new_name,
                        "year": year,
                        "month": month,
                    }
                )
                seen_names.add(name)

    # Sort by year, month
    renamable.sort(key=lambda x: (x["year"], x["month"]))
    return renamable


def rename_spreadsheet(
    drive_service,
    file_id: str,
    new_name: str,
) -> bool:
    """Rename a spreadsheet on Google Drive.

    Args:
        drive_service: Google Drive API service object.
        file_id: ID of the spreadsheet to rename.
        new_name: New name for the spreadsheet.

    Returns:
        True if successful, False otherwise.
    """
    try:
        drive_service.files().update(
            fileId=file_id,
            body={"name": new_name},
            fields="id, name",
        ).execute()
        time.sleep(API_CALL_DELAY)
        logger.info(f"Renamed: {new_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to rename {file_id} to '{new_name}': {e}")
        return False


def create_backup(renames: List[Dict], backup_path: Path) -> None:
    """Create a backup JSON file of all renames.

    Args:
        renames: List of rename records.
        backup_path: Path for the backup file.
    """
    backup_data = {
        "created_at": datetime.now().isoformat(),
        "total_files": len(renames),
        "renames": renames,
    }

    backup_path.parent.mkdir(parents=True, exist_ok=True)
    with open(backup_path, "w", encoding="utf-8") as f:
        json.dump(backup_data, f, ensure_ascii=False, indent=2)

    logger.info(f"Backup created: {backup_path}")


def print_renames_table(renames: List[Dict], title: str = "PLANNED CHANGES") -> None:
    """Print renames in a formatted table.

    Args:
        renames: List of rename records.
        title: Table title.
    """
    if not renames:
        logger.info("No files to rename")
        return

    print(f"\n{'=' * 70}")
    print(f"{title}")
    print(f"{'=' * 70}")
    print(f"{'#':<3} {'Old Name':<30} {'New Name':<30} {'Year':<6}")
    print(f"{'-' * 70}")

    for i, r in enumerate(renames, 1):
        print(f"{i:<3} {r['old_name']:<30} {r['new_name']:<30} {r['year']:<6}")

    print(f"{'-' * 70}")
    print(f"Total: {len(renames)} files")
    print(f"{'=' * 70}\n")


def main():
    parser = argparse.ArgumentParser(
        description="Rename spreadsheets from old to new naming pattern.",
        epilog="""
Examples:
  # Preview all renames (dry-run)
  uv run scripts/rename_spreadsheets.py

  # Preview only 2024
  uv run scripts/rename_spreadsheets.py --year 2024

  # Execute 2024 renames
  uv run scripts/rename_spreadsheets.py --year 2024 --execute

  # Execute all with backup
  uv run scripts/rename_spreadsheets.py --execute --backup

IMPORTANT: After running with --execute, update
upload_cleaned_to_sheets.py::find_spreadsheet_for_period()
to support both old and new patterns during transition.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--year",
        type=str,
        help="Filter by year(s). Comma-separated for multiple (e.g., 2024 or 2024,2025).",
    )

    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually rename spreadsheets. Without this flag, runs in dry-run mode.",
    )

    parser.add_argument(
        "--backup",
        action="store_true",
        help="Create a backup JSON file of all renames. Recommended with --execute.",
    )

    args = parser.parse_args()

    # Validate
    year_filter = None
    if args.year:
        years = [y.strip() for y in args.year.split(",")]
        for y in years:
            if not y.isdigit() or len(y) != 4:
                logger.error(f"Invalid year '{y}'. Must be 4-digit (e.g., 2024)")
                sys.exit(1)
        year_filter = years

    # Load config
    try:
        config = parse_config()
        folder_ids = config["sources"]["import_export_receipts"]["folder_ids"]
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)

    # Connect to Drive
    try:
        drive_service, _ = connect_to_drive()
        logger.info("Connected to Google Drive")
    except Exception as e:
        logger.error(f"Failed to connect to Google Drive: {e}")
        sys.exit(1)

    # Load manifest for caching
    manifest = load_manifest()

    # Find renamable spreadsheets
    renames = find_renamable_spreadsheets(
        drive_service, folder_ids, manifest, year_filter=year_filter
    )

    if not renames:
        logger.info("No spreadsheets found matching the old pattern")
        if year_filter:
            logger.info(f"Checked for years: {year_filter}")
        sys.exit(0)

    # Get total count for context
    all_renames = find_renamable_spreadsheets(drive_service, folder_ids, manifest)

    # Print summary
    mode = "DRY RUN" if not args.execute else "EXECUTING"
    year_desc = f" (year filter: {year_filter})" if year_filter else ""
    print_renames_table(renames, f"{mode} MODE{year_desc}")

    logger.info(f"Total matching files: {len(renames)}")
    logger.info(f"Total files in all folders: {len(all_renames)}")

    if not args.execute:
        logger.info("\nDRY RUN: No changes made. Run with --execute to apply changes.")
        if year_filter:
            logger.info(
                f"To rename all {len(all_renames)} files, run without --year filter."
            )
    else:
        # Execute renames
        logger.info(f"\nStarting rename of {len(renames)} files...")

        successful = 0
        failed = 0

        for r in renames:
            if rename_spreadsheet(drive_service, r["id"], r["new_name"]):
                successful += 1
            else:
                failed += 1

        logger.info(f"\nRename complete: {successful} succeeded, {failed} failed")

        # Create backup if requested
        if args.backup:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = Path(f"data/spreadsheet_rename_backup_{timestamp}.json")
            create_backup(renames, backup_path)

        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("NEXT STEPS")
        logger.info("=" * 70)
        logger.info("1. Update find_spreadsheet_for_period() in:")
        logger.info("   src/modules/import_export_receipts/upload_cleaned_to_sheets.py")
        logger.info("2. Support both old and new patterns during transition:")
        logger.info("   - OLD: T{month:02d}.{year[-2:]}  (e.g., T01.23)")
        logger.info("   - NEW: {year}-{month:02d}  (e.g., 2023-01)")
        logger.info(
            "3. Test upload_cleaned_to_sheets.py to verify it finds renamed files"
        )
        logger.info("=" * 70)

    # Save manifest
    save_manifest(manifest)

    sys.exit(0 if not args.execute or failed == 0 else 1)


if __name__ == "__main__":
    main()
