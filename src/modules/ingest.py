"""Ingest Google Sheets data to raw CSV files in data/00-raw/.

Handles 4 raw sources from project_description.md:
1. Import/Export Receipts: Year/month files with CT.NHAP, CT.XUAT, XNT tabs
2. Receivable: Direct spreadsheet CONG NO HANG NGAY - MỚI
3. Payable: Direct spreadsheet BC CÔNG NỢ NCC
4. CashFlow: Direct spreadsheet SỔ QUỸ TIỀN MẶT + NGÂN HÀNG - 2025
"""

import logging
import sys
import tomllib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.modules.google_api import (
    clear_manifest,
    connect_to_drive,
    export_tab_to_csv,
    find_sheets_in_folder,
    find_year_folders,
    get_cached_sheets_for_folder,
    get_sheet_tabs,
    load_manifest,
    parse_file_metadata,
    read_sheet_data,
    save_manifest,
    should_ingest_import_export,
    update_manifest_for_folder,
)

logger = logging.getLogger(__name__)


def load_pipeline_config() -> Dict[str, Any]:
    """Load pipeline configuration from pipeline.toml (ADR-1: Configuration-Driven).

    Returns:
        Dict with dirs, pipeline, sources, and enrichment config.

    Raises:
        FileNotFoundError: If pipeline.toml not found.
    """
    config_path = Path("pipeline.toml")
    if not config_path.exists():
        raise FileNotFoundError(
            f"pipeline.toml not found at {config_path.resolve()}. "
            "See AGENTS.md and docs/architecture-decisions.md#adr-1 for setup."
        )
    with open(config_path, "rb") as f:
        return tomllib.load(f)


# Load config once at module import (ADR-1: Config-driven, not hardcoded)
_CONFIG = load_pipeline_config()
RAW_DATA_DIR = Path(_CONFIG["dirs"]["raw_data"])
RAW_SOURCES = {
    source_key: {
        "type": source_config.get("type"),
        "description": source_config.get("description", ""),
        "folder_ids": source_config.get("folder_ids", []),
        "spreadsheet_id": source_config.get("spreadsheet_id", ""),
        "tabs": source_config.get("tabs", []),
        "sheets": source_config.get("sheets", []),
        "output_subdir": source_config.get("output_subdir", ""),
    }
    for source_key, source_config in _CONFIG.get("sources", {}).items()
}


def ingest_direct_spreadsheet(
    sheets_service, spreadsheet_id: str, sheet_name: str, output_path: Path
) -> bool:
    """Ingest a single direct spreadsheet (receivable, payable, cashflow).

    Args:
        sheets_service: Google Sheets API service.
        spreadsheet_id: ID of the spreadsheet.
        sheet_name: Name of the sheet tab.
        output_path: Path to save CSV.

    Returns:
        True if successful, False otherwise.
    """
    values = read_sheet_data(sheets_service, spreadsheet_id, sheet_name)
    if not values:
        logger.warning(f"No data in {sheet_name}")
        return False

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            import csv

            writer = csv.writer(f)
            writer.writerows(values)
        logger.info(f"Exported {output_path}")
        return True
    except IOError as e:
        logger.error(f"Failed to write {output_path}: {e}")
        return False


def ingest_from_drive(
    sources: Optional[List[str]] = None,
    test_mode: bool = False,
    clean_up: bool = False,
) -> int:
    """Download Google Sheets from Drive and export to data/00-raw/.

    Args:
        sources: List of raw sources to ingest. If None, ingest all.
        test_mode: Stop after downloading one of each tab type.
        clean_up: Remove existing data/00-raw/ directory first.

    Returns:
        Number of files ingested.
    """
    import shutil

    if sources is None:
        sources = list(RAW_SOURCES.keys())

    invalid_sources = [s for s in sources if s not in RAW_SOURCES]
    if invalid_sources:
        logger.error(f"Invalid sources: {invalid_sources}")
        return 0

    logger.info("=" * 70)
    logger.info("Ingestion: Connecting to Google Drive...")

    try:
        drive_service, sheets_service = connect_to_drive()
    except Exception as e:
        logger.error(f"Failed to connect to Google Drive: {e}")
        return 0

    logger.info("Connected successfully!")

    if clean_up and RAW_DATA_DIR.exists():
        shutil.rmtree(RAW_DATA_DIR)
        logger.info(f"Cleared {RAW_DATA_DIR}/")

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Load manifest cache for folder→sheets lookups
    manifest = load_manifest()
    api_calls_saved = 0

    # Get current month/year for import_export decision logic
    now = datetime.now()
    current_month = now.month
    current_year = now.year

    files_ingested = 0

    # Process import_export_receipts (folder-based, multiple files/tabs)
    if "import_export_receipts" in sources:
        logger.info("=" * 70)
        logger.info(
            "Processing: import_export_receipts (7 shared folders + year folders)"
        )

        tabs_processed = set()
        desired_tabs = RAW_SOURCES["import_export_receipts"]["tabs"]

        def process_sheets_from_folder(folder_id, source_name):
            """Process all sheets in a folder, using manifest cache when available."""
            nonlocal files_ingested, tabs_processed, api_calls_saved

            # Try to get cached sheets first
            cached_sheets, is_fresh = get_cached_sheets_for_folder(manifest, folder_id)
            if cached_sheets is not None:
                sheets = cached_sheets
                api_calls_saved += 1
                logger.debug(f"{source_name}: Using cached sheets (saved 1 API call)")
            else:
                # Not cached or stale, fetch from Drive
                sheets = find_sheets_in_folder(drive_service, folder_id)
                if sheets:
                    update_manifest_for_folder(manifest, folder_id, sheets)

            if not sheets:
                logger.debug(f"No sheets found in {source_name}")
                return False

            for sheet in sheets:
                file_name = sheet["name"]
                file_id = sheet["id"]
                remote_modified_time = sheet.get("modifiedTime")
                year_num, month = parse_file_metadata(file_name)

                if year_num is None:
                    logger.debug(f"Skipping {file_name}: invalid metadata")
                    continue

                tabs = get_sheet_tabs(sheets_service, file_id)
                if not tabs:
                    logger.warning(f"No tabs found in {file_name}")
                    continue

                for tab in set(tabs) & set(desired_tabs):
                    csv_path = (
                        RAW_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"
                    )

                    if not should_ingest_import_export(
                        csv_path, remote_modified_time, current_month, current_year
                    ):
                        continue

                    if export_tab_to_csv(sheets_service, file_id, tab, csv_path):
                        logger.info(f"Exported {csv_path}")
                        files_ingested += 1
                        tabs_processed.add(tab)

                        if test_mode and len(tabs_processed) >= len(desired_tabs):
                            logger.info("Test mode: Downloaded all tab types")
                            return True

            return False

        # Process all shared folders (folders 1-7 from project_description.md)
        for idx, folder_id in enumerate(
            RAW_SOURCES["import_export_receipts"]["folder_ids"], 1
        ):
            logger.info(f"Scanning shared folder {idx}...")
            if process_sheets_from_folder(folder_id, f"Shared folder {idx}"):
                break

        # Process year folders (TỔNG HỢP 202X)
        year_folders = find_year_folders(drive_service)
        if year_folders:
            logger.info(f"Found {len(year_folders)} year folders, scanning...")
            for year_name, folder_id in year_folders.items():
                if process_sheets_from_folder(folder_id, year_name):
                    break

    # Process direct spreadsheets (receivable, payable, cashflow)
    for source_key in ["receivable", "payable", "cashflow"]:
        if source_key not in sources:
            continue

        source_config = RAW_SOURCES[source_key]
        logger.info("=" * 70)
        logger.info(f"Processing: {source_key}")

        spreadsheet_id = source_config["spreadsheet_id"]
        output_subdir = source_config["output_subdir"]

        # Handle both old format (single sheet_name) and new format (multiple sheets)
        if "sheets" in source_config:
            # New format: list of sheets with custom output filenames
            for sheet_info in source_config["sheets"]:
                sheet_name = sheet_info["name"]
                output_file = sheet_info["output_file"]
                csv_path = RAW_DATA_DIR / output_subdir / f"{output_file}.csv"
                if ingest_direct_spreadsheet(
                    sheets_service, spreadsheet_id, sheet_name, csv_path
                ):
                    files_ingested += 1
        else:
            # Legacy format: single sheet_name
            sheet_name = source_config.get("sheet_name")
            if sheet_name:
                csv_path = RAW_DATA_DIR / output_subdir / f"{source_key}.csv"
                if ingest_direct_spreadsheet(
                    sheets_service, spreadsheet_id, sheet_name, csv_path
                ):
                    files_ingested += 1

    # Save updated manifest for next run
    save_manifest(manifest)

    logger.info("=" * 70)
    logger.info(
        f"Ingestion complete: {files_ingested} files from {len(sources)} sources"
    )
    if api_calls_saved > 0:
        logger.info(
            f"Cache efficiency: Saved {api_calls_saved} API calls using manifest"
        )

    return files_ingested


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)-8s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    # Parse command line arguments for debugging/admin tasks
    if len(sys.argv) > 1 and sys.argv[1] == "--clear-cache":
        logger.info("Clearing manifest cache...")
        clear_manifest()
        sys.exit(0)

    ingest_from_drive(test_mode=False, clean_up=False)
