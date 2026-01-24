"""Ingest Google Sheets data to raw CSV files in data/00-raw/.

Handles 4 raw sources from project_description.md:
1. Import/Export Receipts: Year/month files with cleaned tabs (Chi tiết nhập, Chi tiết xuất, Xuất nhập tồn, Chi tiết chi phí) → data/00-raw/import_export/
2. Receivable: Direct spreadsheet CONG NO HANG NGAY - MỚI → data/00-raw/receivable/
3. Payable: Direct spreadsheet BC CÔNG NỢ NCC → data/00-raw/payable/
4. CashFlow: Direct spreadsheet SỔ QUỸ TIỀN MẶT + NGÂN HÀNG - 2025 → data/00-raw/cashflow/

Note: All sources write to raw directory. Source type configuration in pipeline.toml
determines whether transform step is needed (raw vs preprocessed).
See ADR-005 for source_type configuration details.
"""

import logging
import sys
import tomllib
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.modules.google_api import (
    connect_to_drive,
    export_tab_to_csv,
    find_receipt_sheets,
    find_spreadsheet_in_folder,
    get_sheet_tabs,
)
from src.pipeline.validation import validate_schema
from src.utils.path_config import PathConfig

logger = logging.getLogger(__name__)

path_config = PathConfig()


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
STAGING_DATA_DIR = Path(_CONFIG["dirs"]["staging"])

RAW_SOURCES = {
    source_key: {
        "type": source_config.get("type"),
        "description": source_config.get("description", ""),
        "root_folder_id": source_config.get("root_folder_id", ""),
        "receipts_subfolder_name": source_config.get("receipts_subfolder_name", ""),
        "spreadsheet_name": source_config.get("spreadsheet_name", ""),
        "tabs": source_config.get("tabs", []),
        "sheets": source_config.get("sheets", []),
        "output_subdir": source_config.get("output_subdir", ""),
    }
    for source_key, source_config in _CONFIG.get("sources", {}).items()
}


def _validate_year_month_filters(
    year_list: Optional[List[int]], month_list: Optional[List[int]]
) -> None:
    """Validate year (2020-2026) and month (1-12) ranges."""
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


def _validate_year_month(year_num: int, month: int) -> None:
    """Validate year and month ranges for path construction.

    Args:
        year_num: Year from filename (must be 2020-2030).
        month: Month from filename (must be 1-12).

    Raises:
        ValueError: If year or month is outside valid range.
    """
    if not (2020 <= year_num <= 2030):
        raise ValueError(f"Invalid year: {year_num} (must be 2020-2030)")
    if not (1 <= month <= 12):
        raise ValueError(f"Invalid month: {month} (must be 1-12)")


def _get_tabs_for_sheets(
    sheets_service, spreadsheet_ids: List[str]
) -> Dict[str, List[str]]:
    """Get tabs from spreadsheets using individual API calls."""
    tabs_dict = {}
    for spreadsheet_id in spreadsheet_ids:
        try:
            tabs = get_sheet_tabs(sheets_service, spreadsheet_id)
            tabs_dict[spreadsheet_id] = tabs
        except Exception as e:
            logger.error(f"Error getting tabs for {spreadsheet_id}: {e}")
            raise  # Re-raise to maintain fail-fast behavior
    return tabs_dict


def _find_spreadsheets_batch(
    drive_service, spreadsheet_sources: List[tuple]
) -> Dict[str, str]:
    """Find spreadsheets using individual API calls."""
    spreadsheet_ids = {}
    for source_key, folder_id, name in spreadsheet_sources:
        spreadsheet_ids[source_key] = find_spreadsheet_in_folder(
            drive_service, folder_id, name
        )
    return spreadsheet_ids


def _export_sheet_tabs(
    sheets_service,
    sheet_id: str,
    sheet_name: str,
    tabs_dict: Dict[str, List[str]],
    desired_tabs: List[str],
    output_subdir: str = "",
) -> int:
    """Export matching tabs to CSV files."""
    tabs = tabs_dict.get(sheet_id, [])
    if not tabs:
        logger.warning(f"No tabs found in {sheet_name}")
        return 0

    tabs_to_process = set(tabs) & set(desired_tabs)
    files_ingested = 0

    for tab in tabs_to_process:
        if output_subdir:
            csv_path = RAW_DATA_DIR / output_subdir / f"{tab}.csv"
        else:
            csv_path = path_config.import_export_staging_dir() / f"{tab}.csv"

        try:
            if export_tab_to_csv(sheets_service, sheet_id, tab, csv_path):
                logger.info(f"Exported {csv_path}")
                files_ingested += 1
        except Exception as e:
            logger.error(f"Error exporting {tab} from {sheet_name}: {e}")
            raise  # Re-raise to maintain fail-fast behavior

    return files_ingested


def _process_import_export_receipts(
    drive_service,
    sheets_service,
    year_list: Optional[List[int]],
    month_list: Optional[List[int]],
) -> tuple[int, int]:
    """Handle folder-based import_export_receipts source.

    Writes tabs to raw directory (data/00-raw/import_export/).
    Source type determines whether transform step is needed:
    - "preprocessed": Data already clean, transform skipped
    - "raw": Data needs transformation to staging

    See ADR-005 for source_type configuration details.
    """
    files_ingested = 0
    error_count = 0

    logger.info("=" * 70)
    logger.info("Processing: import_export_receipts (unified folder structure)")

    import_export_config = RAW_SOURCES["import_export_receipts"]
    desired_tabs = import_export_config["tabs"]

    logger.info(
        f"Discovering receipts spreadsheets from '{import_export_config['receipts_subfolder_name']}'"
    )

    # Discover spreadsheets with filtering using shared function
    try:
        sheets = find_receipt_sheets(
            drive_service,
            import_export_config,
            year_list,
            month_list,
        )
    except FileNotFoundError as e:
        logger.error(f"{e}")
        return 0, 1

    if not sheets:
        logger.warning("No receipts spreadsheets found")
        return 0, 0

    logger.info(f"Found {len(sheets)} spreadsheet(s) matching filters")

    # Get tabs for all sheets
    spreadsheet_ids = [sheet["id"] for sheet in sheets]
    try:
        tabs_dict = _get_tabs_for_sheets(sheets_service, spreadsheet_ids)
    except Exception as e:
        logger.error(
            f"Failed to get tabs for import_export_receipts: {e}", exc_info=True
        )
        error_count += 1
        return files_ingested, error_count

    # Process each sheet
    for sheet in sheets:
        file_id = sheet["id"]
        file_name = sheet["name"]
        year_num = sheet["year"]
        month = sheet["month"]

        # Validate year and month to prevent path traversal attacks
        try:
            _validate_year_month(year_num, month)
        except ValueError as e:
            logger.error(
                f"Skipping {file_name}: {e}. "
                f"This may indicate a path traversal attempt."
            )
            error_count += 1
            continue

        # Export tabs with year_month prefix for import_export
        tabs = tabs_dict.get(file_id, [])
        if not tabs:
            logger.warning(f"No tabs found in {file_name}")
            continue

        tabs_to_process = set(tabs) & set(desired_tabs)

        for tab in tabs_to_process:
            csv_path = (
                path_config.get_raw_output_dir("import_export_receipts")
                / f"{year_num}_{month}_{tab}.csv"
            ).resolve()

            raw_base_dir = path_config.get_raw_output_dir(
                "import_export_receipts"
            ).parent.resolve()

            if not str(csv_path).startswith(str(raw_base_dir)):
                raise ValueError(
                    f"Invalid path: {csv_path} escapes raw directory. "
                    f"This may indicate a path traversal attempt."
                )

            try:
                if export_tab_to_csv(sheets_service, file_id, tab, csv_path):
                    # Validate schema before accepting to raw
                    if not validate_schema(csv_path, tab):
                        logger.error(
                            f"Schema validation failed for {tab}: {csv_path}. "
                            f"Deleting invalid file to prevent corrupt data."
                        )
                        csv_path.unlink(missing_ok=True)
                        error_count += 1
                        continue

                    logger.info(f"Exported {csv_path}")
                    files_ingested += 1
            except Exception as e:
                logger.error(f"Error exporting {tab} from {file_name}: {e}")
                error_count += 1
                raise  # Maintain fail-fast behavior

    return files_ingested, error_count


def _process_spreadsheet_source(
    drive_service, sheets_service, source_key: str, spreadsheet_ids: Dict[str, str]
) -> tuple[int, int]:
    """Handle direct spreadsheet sources (receivable, payable, cashflow)."""
    files_ingested = 0
    error_count = 0

    source_config = RAW_SOURCES[source_key]
    logger.info("=" * 70)
    logger.info(f"Processing: {source_key}")

    spreadsheet_id = spreadsheet_ids.get(source_key)

    if not spreadsheet_id:
        logger.error(f"Could not find spreadsheet for {source_key}")
        error_count += 1
        return 0, error_count

    logger.info(f"Found {source_key} spreadsheet: {spreadsheet_id}")

    output_subdir = source_config["output_subdir"]

    # Handle sheets configuration
    if "sheets" in source_config:
        # New format: list of sheets with custom output filenames
        for sheet_info in source_config["sheets"]:
            sheet_name = sheet_info["name"]
            output_file = sheet_info["output_file"]
            csv_path = RAW_DATA_DIR / output_subdir / f"{output_file}.csv"
            try:
                if export_tab_to_csv(
                    sheets_service, spreadsheet_id, sheet_name, csv_path
                ):
                    files_ingested += 1
            except Exception as e:
                logger.error(f"Error exporting {sheet_name} from {source_key}: {e}")
                error_count += 1
                raise  # Maintain fail-fast behavior

    return files_ingested, error_count


def ingest_from_drive(
    sources: Optional[List[str]] = None,
    year_list: Optional[List[int]] = None,
    month_list: Optional[List[int]] = None,
) -> int:
    """Download Google Sheets from Drive and export to data/00-raw/.

    Args:
        sources: List of raw sources to ingest. If None, ingest all.
        year_list: Filter by year folder (e.g., [2024, 2025]). If None, ingest all years.
        month_list: Filter by month (1-12). If None, ingest all months.

    Returns:
        Number of files ingested.
    """
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

    # Validate filters
    _validate_year_month_filters(year_list, month_list)

    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
    STAGING_DATA_DIR.mkdir(parents=True, exist_ok=True)

    files_ingested = 0
    error_count = 0

    # Process import_export_receipts
    if "import_export_receipts" in sources:
        try:
            receipts_files, receipts_errors = _process_import_export_receipts(
                drive_service, sheets_service, year_list, month_list
            )
            files_ingested += receipts_files
            error_count += receipts_errors
        except Exception as e:
            logger.error(
                f"Failed to process import_export_receipts: {e}", exc_info=True
            )
            error_count += 1
            return files_ingested  # Fail-fast on import_export_receipts errors

    # Prepare spreadsheet sources for batch discovery
    spreadsheet_sources = []
    for source_key in ["receivable", "payable", "cashflow"]:
        if source_key not in sources:
            continue
        source_config = RAW_SOURCES[source_key]
        root_folder_id = source_config.get("root_folder_id")
        spreadsheet_name = source_config.get("spreadsheet_name")
        if root_folder_id and spreadsheet_name:
            spreadsheet_sources.append((source_key, root_folder_id, spreadsheet_name))

    # Batch discover spreadsheets
    spreadsheet_ids = _find_spreadsheets_batch(drive_service, spreadsheet_sources)

    # Process each spreadsheet source
    for source_key in ["receivable", "payable", "cashflow"]:
        if source_key not in sources:
            continue

        try:
            source_files, source_errors = _process_spreadsheet_source(
                drive_service, sheets_service, source_key, spreadsheet_ids
            )
            files_ingested += source_files
            error_count += source_errors
        except Exception as e:
            logger.error(f"Failed to process {source_key}: {e}", exc_info=True)
            error_count += 1

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

    # Suppress googleapiclient file_cache warning (oauth2client<4.0.0 required)
    logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)

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

    # Run ingestion
    ingest_from_drive(
        sources=sources_to_ingest,
        year_list=args.year,
        month_list=args.month,
    )
