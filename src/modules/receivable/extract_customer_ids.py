# -*- coding: utf-8 -*-
"""Extract and generate new customer IDs from sale receipt transaction data.

This module:
1. Reads cleaned sale receipt data (CT.XUAT) from staging
2. Groups transactions by customer name
3. Extracts first and last transaction dates for each customer
4. Ranks customers by first date, then by total transaction amount
5. Generates sequential customer IDs (KH000001, KH000002, ...)
6. Exports to CSV in staging directory

Raw source: CT.XUAT (cleaned sale receipts)
Module: receivable
Pipeline stage: data/01-staging/import_export → data/01-staging/receivable
Output: Customer ID mapping with transaction summary
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

# ============================================================================
# CONFIGURATION
# ============================================================================

# Data folder paths
DATA_STAGING_DIR = Path.cwd() / "data" / "01-staging"
IMPORT_EXPORT_STAGING = DATA_STAGING_DIR / "import_export"
RECEIVABLE_STAGING = DATA_STAGING_DIR / "receivable"

# Input file (cleaned sale receipts from clean_receipts_sale.py)
INPUT_FILE_PATTERN = "clean_receipts_sale_*.csv"

# Output file
OUTPUT_FILENAME = "extract_customer_ids.csv"

# ============================================================================
# LOGGING SETUP
# ============================================================================

logger = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def find_input_file(staging_dir: Path = IMPORT_EXPORT_STAGING) -> Optional[Path]:
    """Find the most recently modified cleaned sale receipt CSV file.

    Args:
        staging_dir: Directory to search for input files

    Returns:
        Path to the most recent file or None if not found
    """
    if not staging_dir.exists():
        logger.warning(f"Staging directory not found: {staging_dir}")
        return None

    csv_files = list(staging_dir.glob(INPUT_FILE_PATTERN))
    if not csv_files:
        logger.warning(
            f"No files matching pattern '{INPUT_FILE_PATTERN}' found in {staging_dir}"
        )
        return None

    # Return the most recently modified file
    return max(csv_files, key=lambda p: p.stat().st_mtime)


def read_sale_receipt_data(filepath: Path) -> pd.DataFrame:
    """Read cleaned sale receipt data.

    Args:
        filepath: Path to input CSV file

    Returns:
        DataFrame with sale receipt data
    """
    try:
        df = pd.read_csv(filepath, encoding="utf-8")
        logger.info(
            f"Loaded data from {filepath.name}: {len(df)} rows, {len(df.columns)} columns"
        )
        return df
    except Exception as e:
        logger.error(f"Error reading {filepath}: {e}")
        raise


def aggregate_customer_data(df: pd.DataFrame) -> pd.DataFrame:
    """Group by customer and aggregate transaction data.

    Args:
        df: DataFrame with sale receipt data

    Returns:
        DataFrame with customer aggregations
    """
    if "Tên khách hàng" not in df.columns or "Ngày" not in df.columns:
        logger.error("Required columns 'Tên khách hàng' or 'Ngày' not found")
        raise ValueError("Missing required columns for aggregation")

    logger.info(f"Before aggregation: {len(df)} total rows")

    # Ensure Ngày is datetime
    df["Ngày"] = pd.to_datetime(df["Ngày"], errors="coerce")

    # Calculate total amount (using Thành tiền if available)
    amount_col = "Thành tiền" if "Thành tiền" in df.columns else "Số lượng"

    # Group by customer and aggregate
    customer_summary = (
        df.groupby("Tên khách hàng", dropna=False)
        .agg(
            first_date=("Ngày", "min"),
            last_date=("Ngày", "max"),
            total_amount=(amount_col, "sum"),
            transaction_count=("Ngày", "count"),
        )
        .reset_index()
    )

    # Remove rows with NaN customer names
    customer_summary = customer_summary.dropna(subset=["Tên khách hàng"])

    logger.info(f"Aggregated {len(customer_summary)} unique customers")
    return customer_summary


def rank_and_generate_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Rank customers and generate sequential customer IDs.

    Sorting criteria:
    1. First transaction date (earliest first)
    2. Total transaction amount (highest first)

    Args:
        df: DataFrame with customer aggregations

    Returns:
        DataFrame with generated customer IDs
    """
    # Sort by first_date (earliest first), then by total_amount (highest first)
    df_sorted = df.sort_values(
        by=["first_date", "total_amount"], ascending=[True, False], na_position="last"
    ).reset_index(drop=True)

    # Generate sequential customer IDs (KH000001, KH000002, ...)
    df_sorted["Mã khách hàng mới"] = df_sorted.index.map(lambda x: f"KH{x + 1:06d}")

    # Reorder and rename columns for output
    output_df = df_sorted[
        [
            "Mã khách hàng mới",
            "Tên khách hàng",
            "first_date",
            "last_date",
            "total_amount",
            "transaction_count",
        ]
    ].copy()

    output_df.rename(
        columns={
            "Tên khách hàng": "Tên khách hàng",
            "first_date": "Ngày giao dịch đầu",
            "last_date": "Ngày giao dịch cuối",
            "total_amount": "Tổng tiền",
            "transaction_count": "Số lần giao dịch",
        },
        inplace=True,
    )

    logger.info(f"Generated {len(output_df)} customer IDs")
    return output_df


def save_to_csv(df: pd.DataFrame, output_path: Path) -> None:
    """Save customer ID mapping to CSV file.

    Args:
        df: DataFrame to save
        output_path: Path to output CSV file
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"Saved to CSV: {output_path}")


# ============================================================================
# MAIN PROCESSING
# ============================================================================


def process(staging_dir: Optional[Path] = None) -> Optional[Path]:
    """Process sale receipt data to generate customer IDs.

    Args:
        staging_dir: Directory to save output (defaults to data/01-staging/receivable)

    Returns:
        Path to output file or None if failed
    """
    if staging_dir is None:
        staging_dir = RECEIVABLE_STAGING

    logger.info("=" * 70)
    logger.info("STARTING CUSTOMER ID EXTRACTION")
    logger.info("=" * 70)

    try:
        # Find input file
        input_file = find_input_file()
        if not input_file:
            logger.error("No input file found for customer ID extraction")
            return None

        # Read sale receipt data
        df = read_sale_receipt_data(input_file)
        if df.empty:
            logger.error("No data loaded from input file")
            return None

        # Aggregate customer data
        customer_summary = aggregate_customer_data(df)
        if customer_summary.empty:
            logger.error("No customer summary generated")
            return None

        # Rank and generate IDs
        result_df = rank_and_generate_ids(customer_summary)

        # Save to CSV
        output_path = staging_dir / OUTPUT_FILENAME
        save_to_csv(result_df, output_path)

        logger.info("=" * 70)
        logger.info("CUSTOMER ID EXTRACTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)

        return output_path

    except Exception as e:
        logger.error(f"Customer ID extraction failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    process()
