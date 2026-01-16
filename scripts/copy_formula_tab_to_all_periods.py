"""Copy formula tab to all period spreadsheets (one-time operation).

This script copies a tab with formulas from a source spreadsheet to all
period spreadsheets in configured folders. The copied tab preserves all formulas
and formatting, uses the original tab name (not "Copy of ..."), and replaces
any existing tab with the same name. The tab is placed as the first tab
in each target spreadsheet.

Source:
- Spreadsheet: 1O_XmlU_gAdPyyszu9jdFVfltjFv8OpqqAEmIBykVVzI
- Tab ID: 2044382542

Target: All spreadsheets in configured folders from pipeline.toml (excluding source)

Usage:
  # Dry run (preview only)
  uv run scripts/copy_formula_tab_to_all_periods.py --dry-run

  # Execute actual copy
  uv run scripts/copy_formula_tab_to_all_periods.py
"""

import logging
import sys
import time
from pathlib import Path

import tomllib

from src.modules.google_api import (
    API_CALL_DELAY,
    connect_to_drive,
    copy_sheet_to_spreadsheet,
    delete_sheet,
    find_sheets_in_folder,
    get_sheet_id_by_name,
    get_sheet_name_by_id,
    load_manifest,
    rename_sheet,
    save_manifest,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

SOURCE_SPREADSHEET_ID = "1O_XmlU_gAdPyyszu9jdFVfltjFv8OpqqAEmIBykVVzI"
SOURCE_SHEET_ID = 2044382542

CONFIG_PATH = Path("pipeline.toml")


def load_folder_ids():
    """Load folder IDs from pipeline.toml."""
    with open(CONFIG_PATH, "rb") as f:
        config = tomllib.load(f)
    return config["sources"]["import_export_receipts"]["folder_ids"]


def copy_to_all_folders(folder_ids: list, dry_run: bool = False) -> tuple[int, int]:
    """Copy formula tab to all spreadsheets in configured folders.

    For each target spreadsheet:
    - Skips source spreadsheet
    - Checks if a tab with the source name already exists
    - If exists: copies source, renames to original name, deletes old tab
    - If not exists: copies source and renames to original name
    - Places tab in first position

    Args:
        folder_ids: List of folder IDs to scan.
        dry_run: If True, log without executing copy operations.

    Returns:
        Tuple of (total_attempts, successful_copies).
    """
    logger.info("=" * 70)
    logger.info("COPYING FORMULA TAB TO ALL PERIOD SPREADSHEETS")
    if dry_run:
        logger.info("MODE: DRY RUN (no changes made)")
    logger.info("=" * 70)
    logger.info(f"Source: Spreadsheet {SOURCE_SPREADSHEET_ID}, Tab {SOURCE_SHEET_ID}")

    source_name = None
    if not dry_run:
        drive_service, sheets_service = connect_to_drive()
        source_name = get_sheet_name_by_id(
            sheets_service, SOURCE_SPREADSHEET_ID, SOURCE_SHEET_ID
        )
        logger.info(f"Source tab name: {source_name}")
    else:
        logger.info("[DRY RUN] Would connect to Google APIs")

    logger.info("=" * 70)

    manifest = load_manifest()
    total_attempts = 0
    successful_copies = 0

    for idx, folder_id in enumerate(folder_ids, 1):
        logger.info("")
        logger.info("-" * 70)
        logger.info(f"FOLDER {idx}/{len(folder_ids)}: {folder_id}")
        logger.info("-" * 70)

        if not dry_run:
            sheets = find_sheets_in_folder(drive_service, folder_id)
        else:
            logger.info("[DRY RUN] Would list spreadsheets in folder")
            sheets = []

        if not sheets:
            logger.warning(f"No spreadsheets found in folder {folder_id}")
            continue

        logger.info(f"Found {len(sheets)} spreadsheet(s)")

        for sheet in sheets:
            sheet_id = sheet["id"]
            sheet_name = sheet["name"]

            if sheet_id == SOURCE_SPREADSHEET_ID:
                logger.info(f"  Skipping source spreadsheet: {sheet_name}")
                continue

            logger.info("")
            logger.info(f"  Processing: {sheet_name}")
            logger.info(f"  Spreadsheet ID: {sheet_id}")

            if dry_run:
                logger.info(
                    f"  [DRY RUN] Would copy tab '{source_name}' to this spreadsheet"
                )
                total_attempts += 1
                successful_copies += 1
                continue

            existing_sheet_id = get_sheet_id_by_name(
                sheets_service, sheet_id, source_name
            )

            if existing_sheet_id:
                logger.info(
                    f"  Existing tab '{source_name}' found (ID: {existing_sheet_id})"
                )

            result = copy_sheet_to_spreadsheet(
                sheets_service,
                SOURCE_SPREADSHEET_ID,
                SOURCE_SHEET_ID,
                sheet_id,
                move_to_first=True,
            )

            if not result:
                logger.error("  ✗ Failed to copy")
                total_attempts += 1
                continue

            new_sheet_id = result.get("sheetId")
            logger.info(f"  Copied as new sheet {new_sheet_id}")

            if existing_sheet_id:
                if delete_sheet(sheets_service, sheet_id, existing_sheet_id):
                    logger.info(f"  ✓ Deleted old tab (ID: {existing_sheet_id})")
                else:
                    logger.error("  ✗ Failed to delete old tab")

            if rename_sheet(sheets_service, sheet_id, new_sheet_id, source_name):
                logger.info(f"  ✓ Renamed to '{source_name}'")
            else:
                logger.error("  ✗ Failed to rename sheet")
                total_attempts += 1
                continue

            total_attempts += 1
            successful_copies += 1

            time.sleep(API_CALL_DELAY)

    save_manifest(manifest)

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
        "--dry-run",
        action="store_true",
        help="Preview copy operations without executing",
    )

    args = parser.parse_args()

    folder_ids = load_folder_ids()
    logger.info(f"Loaded {len(folder_ids)} folder IDs from pipeline.toml")

    total, success = copy_to_all_folders(folder_ids, dry_run=args.dry_run)

    sys.exit(0 if success == total else 1)
