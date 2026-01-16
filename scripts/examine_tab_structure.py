"""Examine structure of two tabs without comparison.

Usage:
    uv run scripts/examine_tab_structure.py <spreadsheet_id>
"""

import logging
import sys

from src.modules.google_api import connect_to_drive, read_sheet_data

logger = logging.getLogger(__name__)


def examine_tab(sheets_service, spreadsheet_id: str, tab_name: str):
    """Print tab structure."""
    logger.info(f"")
    logger.info(f"{'='*70}")
    logger.info(f"TAB: {tab_name}")
    logger.info(f"{'='*70}")

    values = read_sheet_data(sheets_service, spreadsheet_id, tab_name)

    if not values:
        logger.warning("No data")
        return

    logger.info(f"")
    logger.info(f"Total rows: {len(values)}")
    logger.info(f"")

    # Print headers
    if values:
        headers = values[0]
        logger.info(f"Headers ({len(headers)} columns):")
        for idx, header in enumerate(headers):
            logger.info(f"  [{idx}] {header}")

    # Print first few data rows
    logger.info(f"")
    logger.info(f"First 5 data rows:")
    for idx, row in enumerate(values[1:6], start=2):
        logger.info(f"  Row {idx}: {row}")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )

    if len(sys.argv) != 2:
        logger.error("Usage: uv run scripts/examine_tab_structure.py <spreadsheet_id>")
        sys.exit(1)

    spreadsheet_id = sys.argv[1]

    try:
        drive_service, sheets_service = connect_to_drive()
        logger.info("Connected to Google Drive")
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        sys.exit(1)

    examine_tab(sheets_service, spreadsheet_id, "Chi tiết xuất")
    examine_tab(sheets_service, spreadsheet_id, "CT.XUAT")


if __name__ == "__main__":
    main()
