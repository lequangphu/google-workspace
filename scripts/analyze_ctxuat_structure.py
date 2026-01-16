"""Deep analysis of CT.XUAT structure to understand discrepancy."""

import logging
import sys
from collections import Counter

import pandas as pd

from src.modules.google_api import connect_to_drive, read_sheet_data

logger = logging.getLogger(__name__)


def analyze_ctxuat(sheets_service, spreadsheet_id: str):
    """Analyze CT.XUAT tab in detail."""
    logger.info("=" * 70)
    logger.info("ANALYZING CT.XUAT TAB")
    logger.info("=" * 70)

    values = read_sheet_data(sheets_service, spreadsheet_id, "CT.XUAT")

    if not values:
        logger.error("No data")
        return

    logger.info(f"")
    logger.info(f"Total rows: {len(values)}")
    logger.info(f"")

    # Analyze column structure
    logger.info("Column structure analysis:")
    for idx in range(min(30, len(values))):
        row = values[idx]
        if idx == 0:
            logger.info(f"  Row {idx+1} (Header): {len(row)} columns - {row[:5]}...")
        elif row:
            logger.info(f"  Row {idx+1}: {len(row)} columns - first 3: {row[:3]}")
            if idx > 10:
                break

    # Find data rows (skip headers)
    data_rows = []
    for row in values[4:]:  # Skip first 4 header rows
        if row and any(row):
            data_rows.append(row)

    logger.info(f"")
    logger.info(f"Data rows (excluding headers): {len(data_rows)}")

    # Analyze structure
    logger.info(f"")
    logger.info("First 10 data rows structure:")
    for idx, row in enumerate(data_rows[:10]):
        logger.info(f"  Row {idx}: col0={repr(row[0]) if row else 'empty'}, cols1-3={row[1:4] if len(row) > 3 else []}, col8={row[8] if len(row) > 8 else 'N/A'}, col22={row[22] if len(row) > 22 else 'N/A'}")

    # Find document types in column 0
    doc_types = Counter()
    for row in data_rows:
        if row and row[0]:
            doc_types[row[0]] += 1

    logger.info(f"")
    logger.info("Document types (col 0):")
    for doc_type, count in doc_types.most_common(20):
        logger.info(f"  {doc_type}: {count} rows")

    # Find unique products in columns 5-6 (MÃ SỐ + Chủng loại)
    products = []
    for row in data_rows:
        if len(row) > 6:
            product_code = row[5] if row[5] else ""
            product_name = row[6] if row[6] else ""
            if product_code or product_name:
                products.append((product_code, product_name))

    unique_products = set(products)
    logger.info(f"")
    logger.info(f"Unique product entries: {len(unique_products)}")
    logger.info(f"")
    logger.info("Sample products:")
    for i, (code, name) in enumerate(list(unique_products)[:10]):
        logger.info(f"  {i+1}. Code: {repr(code)}, Name: {repr(name)}")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )

    if len(sys.argv) != 2:
        logger.error("Usage: uv run scripts/analyze_ctxuat_structure.py <spreadsheet_id>")
        sys.exit(1)

    spreadsheet_id = sys.argv[1]

    try:
        drive_service, sheets_service = connect_to_drive()
        logger.info("Connected to Google Drive")
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        sys.exit(1)

    analyze_ctxuat(sheets_service, spreadsheet_id)


if __name__ == "__main__":
    main()
