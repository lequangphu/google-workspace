"""Find which column in CT.XUAT contains quantity values."""

import logging
import sys

import pandas as pd

from src.modules.google_api import connect_to_drive, read_sheet_data

logger = logging.getLogger(__name__)


def find_qty_column(sheets_service, spreadsheet_id: str):
    """Analyze CT.XUAT to find quantity column."""
    logger.info("=" * 70)
    logger.info("FINDING QUANTITY COLUMN IN CT.XUAT")
    logger.info("=" * 70)

    values = read_sheet_data(sheets_service, spreadsheet_id, "CT.XUAT")

    if not values:
        logger.error("No data")
        return

    # Skip first 4 header rows
    data = values[4:]

    # Look for first 20 rows with actual content
    logger.info("")
    logger.info("Analyzing column patterns in data rows:")
    logger.info("")

    valid_rows = []
    for row in data[:50]:
        if row and len(row) > 10:
            valid_rows.append(row)

    logger.info(f"Found {len(valid_rows)} valid data rows")
    logger.info("")

    # Analyze each column
    logger.info("Column analysis (showing first 10 columns):")
    for col_idx in range(10):
        col_name = f"Column {col_idx}"
        logger.info(f"")
        logger.info(f"{col_name}:")
        logger.info(f"  Header row value: {values[4][col_idx] if col_idx < len(values[4]) else 'N/A'}")
        logger.info(f"  First 10 data values:")

        values_in_col = []
        for row in valid_rows[:10]:
            if col_idx < len(row):
                val = row[col_idx]
                values_in_col.append(val)
                logger.info(f"    Row {valid_rows.index(row) + 1}: {repr(val)}")

        # Check if column looks like quantity
        numeric_count = 0
        for val in values_in_col:
            try:
                float(str(val).replace(",", "").replace(" ", ""))
                numeric_count += 1
            except:
                pass

        logger.info(f"  Numeric values: {numeric_count}/{len(values_in_col)}")

        if numeric_count == len(values_in_col) and numeric_count > 5:
            logger.info(f"  --> LIKELY QUANTITY COLUMN")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )

    spreadsheet_id = "1QJReHhfPo0EkTEwjHZFH3vEQXJj127nve2hf3Msm_Ko"

    try:
        drive_service, sheets_service = connect_to_drive()
        logger.info("Connected to Google Drive")
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        sys.exit(1)

    find_qty_column(sheets_service, spreadsheet_id)


if __name__ == "__main__":
    main()
