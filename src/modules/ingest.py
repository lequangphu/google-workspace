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
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.modules.google_api import (
    connect_to_drive,
    export_tab_to_csv,
    find_sheets_in_folder,
    get_sheet_tabs,
    parse_file_metadata,
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
        "nested_year_folders": source_config.get("nested_year_folders", {}),
    }
    for source_key, source_config in _CONFIG.get("sources", {}).items()
}


def ingest_from_drive(
    sources: Optional[List[str]] = None,
    test_mode: bool = False,
    clean_up: bool = False,
    year_list: Optional[List[int]] = None,
    month_list: Optional[List[int]] = None,
    tab_list: Optional[List[str]] = None,
) -> int:
    """Download Google Sheets from Drive and export to data/00-raw/.

    Args:
        sources: List of raw sources to ingest. If None, ingest all.
        test_mode: Stop after downloading one of each tab type.
        clean_up: Remove existing data/00-raw/ directory first.
        year_list: Filter by year folder (e.g., [2024, 2025]). If None, ingest all years.
        month_list: Filter by month (1-12). If None, ingest all months.
        tab_list: Filter by tab name (e.g., ["CT.NHAP", "CT.XUAT"]). If None, ingest all tabs.

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

    if year_list is not None:
        for year in year_list:
            if year < 2020 or year > 2026:
                logger.error(f"Invalid year: {year} (must be 2020-2026)")
                sys.exit(1)

    if month_list is not None:
        for month in month_list:
            if month < 1 or month > 12:
                logger.error(f"Invalid month: {month} (must be 1-12)")
                sys.exit(1)

    tab_shorthands = {
        "nhap": "CT.NHAP",
        "xuat": "CT.XUAT",
        "xnt": "XNT",
        "ct.nhap": "CT.NHAP",
        "ct.xuat": "CT.XUAT",
    }

    if tab_list is not None:
        expanded_tabs = []
        valid_tabs = RAW_SOURCES["import_export_receipts"]["tabs"]
        for tab in tab_list:
            tab_lower = tab.lower()
            if tab_lower in tab_shorthands:
                expanded_tabs.append(tab_shorthands[tab_lower])
            elif tab in valid_tabs:
                expanded_tabs.append(tab)
            else:
                logger.warning(
                    f"Tab '{tab}' not found in configuration (available: {valid_tabs})"
                )
        tab_list = expanded_tabs if expanded_tabs else None

    if clean_up and RAW_DATA_DIR.exists():
        shutil.rmtree(RAW_DATA_DIR)
        logger.info(f"Cleared {RAW_DATA_DIR}/")

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    files_ingested = 0
    error_count = 0

    # Process import_export_receipts (folder-based, multiple files/tabs)
    if "import_export_receipts" in sources:
        logger.info("=" * 70)
        logger.info(
            "Processing: import_export_receipts (7 shared folders + year folders)"
        )

        tabs_processed = set()
        desired_tabs = RAW_SOURCES["import_export_receipts"]["tabs"]

        def process_sheets_from_folder(
            folder_id,
            source_name,
            year_folder_name=None,
            year_list=None,
            month_list=None,
            tab_list=None,
        ):
            """Process all sheets in a folder, applying filters."""
            nonlocal files_ingested, tabs_processed, error_count

            folder_display = year_folder_name if year_folder_name else "Shared"
            logger.info(f"Processing folder: {folder_display}")
            logger.info(
                f"  Filters: year={year_list}, month={month_list}, tab={tab_list}"
            )

            # Get sheets from Drive API
            try:
                sheets = find_sheets_in_folder(drive_service, folder_id)
            except Exception as e:
                logger.error(f"Error listing sheets in {source_name}: {e}")
                error_count += 1
                return True  # Stop on first error (fail-fast)

            if not sheets:
                logger.debug(f"No sheets found in {source_name}")
                return False

            for sheet in sheets:
                file_name = sheet["name"]
                file_id = sheet["id"]

                # Parse year/month from filename
                year_num, month = parse_file_metadata(file_name)

                if year_num is None:
                    logger.debug(f"Skipping {file_name}: invalid metadata")
                    continue

                # Apply year filter (check both year folder name and parsed year)
                if year_list is not None:
                    year_from_folder = (
                        year_folder_name if year_folder_name else year_num
                    )
                    if year_from_folder not in year_list:
                        logger.debug(
                            f"Skipping {file_name}: year {year_from_folder} not in filter {year_list}"
                        )
                        continue

                # Apply month filter
                if month_list is not None and month not in month_list:
                    logger.debug(
                        f"Skipping {file_name}: month {month} not in filter {month_list}"
                    )
                    continue

                # Get available tabs
                try:
                    tabs = get_sheet_tabs(sheets_service, file_id)
                except Exception as e:
                    logger.error(f"Error getting tabs for {file_name}: {e}")
                    error_count += 1
                    return True  # Stop on first error (fail-fast)

                if not tabs:
                    logger.warning(f"No tabs found in {file_name}")
                    continue

                # Determine which tabs to process
                if tab_list is not None:
                    # Apply tab filter
                    tabs_to_process = set(tabs) & set(tab_list)
                else:
                    # Use configured desired tabs
                    tabs_to_process = set(tabs) & set(desired_tabs)

                for tab in tabs_to_process:
                    csv_path = (
                        RAW_DATA_DIR / "import_export" / f"{year_num}_{month}_{tab}.csv"
                    )

                    try:
                        if export_tab_to_csv(sheets_service, file_id, tab, csv_path):
                            logger.info(f"Exported {csv_path}")
                            files_ingested += 1
                            tabs_processed.add(tab)

                            if test_mode and len(tabs_processed) >= len(desired_tabs):
                                logger.info("Test mode: Downloaded all tab types")
                                return True
                    except Exception as e:
                        logger.error(f"Error exporting {tab} from {file_name}: {e}")
                        error_count += 1
                        return True  # Stop on first error (fail-fast)

            return False

        # Process import_export_receipts folders from config
        import_export_config = RAW_SOURCES["import_export_receipts"]
        nested_year_folders = import_export_config.get("nested_year_folders", {})

        for idx, folder_id in enumerate(import_export_config["folder_ids"], 1):
            logger.info(f"Scanning folder {idx}: {folder_id}...")

            # Check if this folder has nested year structure
            if folder_id in nested_year_folders:
                # Process nested year folders for this parent folder
                year_folders_map = nested_year_folders[folder_id]
                logger.info(
                    f"Folder has nested year structure: {len(year_folders_map)} year folders"
                )

                for year_num, year_folder_id in year_folders_map.items():
                    year_folder_name = f"Year {year_num}"
                    logger.info(f"Processing {year_folder_name} folder...")

                    if process_sheets_from_folder(
                        year_folder_id,
                        year_folder_name,
                        year_num,
                        year_list,
                        month_list,
                        tab_list,
                    ):
                        logger.info("Stopped processing year folders (fail-fast)")
                        break
            else:
                # Process as regular folder (no nested structure)
                if process_sheets_from_folder(
                    folder_id, f"Folder {idx}", None, None, None, None
                ):
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
                if export_tab_to_csv(
                    sheets_service, spreadsheet_id, sheet_name, csv_path
                ):
                    files_ingested += 1
        else:
            # Legacy format: single sheet_name
            sheet_name = source_config.get("sheet_name")
            if sheet_name:
                csv_path = RAW_DATA_DIR / output_subdir / f"{source_key}.csv"
                if export_tab_to_csv(
                    sheets_service, spreadsheet_id, sheet_name, csv_path
                ):
                    files_ingested += 1

    logger.info("=" * 70)
    logger.info(
        f"Ingestion complete: {files_ingested} files from {len(sources)} sources"
    )

    if error_count > 0:
        logger.error(f"{error_count} error(s) encountered during ingestion")
        sys.exit(1)

    return files_ingested


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)-8s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    manifest_path = Path("data/.drive_manifest.json")
    if manifest_path.exists():
        logger.info(f"Removing legacy manifest file: {manifest_path}")
        manifest_path.unlink()

    parser = argparse.ArgumentParser(
        description="Ingest Google Sheets data to raw CSV files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
 Examples:
   # Run all sources (default)
   uv run src/modules/ingest.py

   # Run only receivable and payable
   uv run src/modules/ingest.py --only receivable,payable

   # Skip import_export_receipts
   uv run src/modules/ingest.py --skip import_export_receipts

   # Filter by year (only 2024 files)
   uv run src/modules/ingest.py --only import_export_receipts --year 2024

   # Filter by year and month (only 2024, January files)
   uv run src/modules/ingest.py --only import_export_receipts --year 2024 --month 1

   # Filter by tab (only CT.NHAP and CT.XUAT)
   uv run src/modules/ingest.py --only import_export_receipts --tab nhap --tab xuat

   # Use tab shorthands (nhap→CT.NHAP, xuat→CT.XUAT, xnt→XNT)
   uv run src/modules/ingest.py --only import_export_receipts --tab nhap

   # Combine filters (year 2024, month 1, tab nhap)
   uv run src/modules/ingest.py --only import_export_receipts --year 2024 --month 1 --tab nhap

   Available sources: {}
        """.format(", ".join(sorted(RAW_SOURCES.keys()))),
    )

    parser.add_argument(
        "--only",
        type=str,
        default=None,
        help="Comma-separated list of sources to ingest (e.g., receivable,payable)",
    )
    parser.add_argument(
        "--skip",
        type=str,
        default=None,
        help="Comma-separated list of sources to skip (e.g., import_export_receipts)",
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Stop after downloading one of each tab type",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove existing data/00-raw/ directory first",
    )
    parser.add_argument(
        "--year",
        type=int,
        action="append",
        default=None,
        help="Filter by year folder (can be specified multiple times, e.g., --year 2024 --year 2025)",
    )
    parser.add_argument(
        "--month",
        type=int,
        action="append",
        default=None,
        help="Filter by month (1-12, can be specified multiple times, e.g., --month 1 --month 2)",
    )
    parser.add_argument(
        "--tab",
        type=str,
        action="append",
        default=None,
        help="Filter by tab name (case-insensitive shorthands: nhap→CT.NHAP, xuat→CT.XUAT, xnt→XNT; can be specified multiple times)",
    )
    parser.add_argument(
        "--validate-args",
        action="store_true",
        help="Validate arguments and exit without connecting to Drive",
    )

    args = parser.parse_args()

    # Handle conflicting options
    if args.only and args.skip:
        logger.error("Cannot use both --only and --skip simultaneously")
        sys.exit(1)

    # Determine sources to ingest
    sources_to_ingest = None

    if args.only:
        # Parse --only flag
        requested_sources = [s.strip() for s in args.only.split(",")]
        invalid_sources = [s for s in requested_sources if s not in RAW_SOURCES]
        if invalid_sources:
            logger.error(
                f"Invalid sources: {invalid_sources}. "
                f"Available: {', '.join(sorted(RAW_SOURCES.keys()))}"
            )
            sys.exit(1)
        sources_to_ingest = requested_sources
        logger.info(f"Ingesting only: {', '.join(sources_to_ingest)}")

    elif args.skip:
        # Parse --skip flag
        skip_sources = [s.strip() for s in args.skip.split(",")]
        invalid_sources = [s for s in skip_sources if s not in RAW_SOURCES]
        if invalid_sources:
            logger.error(
                f"Invalid sources to skip: {invalid_sources}. "
                f"Available: {', '.join(sorted(RAW_SOURCES.keys()))}"
            )
            sys.exit(1)
        sources_to_ingest = [s for s in RAW_SOURCES.keys() if s not in skip_sources]
        logger.info(f"Skipping: {', '.join(skip_sources)}")
        logger.info(f"Ingesting: {', '.join(sources_to_ingest)}")

    # Validate args mode: exit early without connecting to Drive
    if args.validate_args:
        logger.info("=" * 70)
        logger.info("ARGUMENT VALIDATION MODE")
        logger.info("=" * 70)
        if sources_to_ingest is None:
            sources_to_ingest = list(RAW_SOURCES.keys())
        logger.info(f"Sources to ingest: {sources_to_ingest}")
        logger.info(f"Test mode: {args.test_mode}")
        logger.info(f"Cleanup: {args.cleanup}")
        logger.info(f"Year filter: {args.year}")
        logger.info(f"Month filter: {args.month}")
        logger.info(f"Tab filter: {args.tab}")
        logger.info("=" * 70)
        logger.info("Arguments validated successfully. No API calls made.")
        sys.exit(0)

    # Run ingestion
    ingest_from_drive(
        sources=sources_to_ingest,
        test_mode=args.test_mode,
        clean_up=args.cleanup,
        year_list=args.year,
        month_list=args.month,
        tab_list=args.tab,
    )
