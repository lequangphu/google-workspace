# -*- coding: utf-8 -*-
"""
⚠️ PENDING MIGRATION: This script is scheduled for migration to src/modules/receivable/extract_customer_ids.py

Current status: Pending refactoring to new modular structure.
This legacy file is kept for reference only.
---

Generate new customer IDs based on transaction history.

This script:
1. Reads cleaned export receipt data (CT.XUAT)
2. Groups transactions by customer name
3. Extracts first and last transaction dates for each customer
4. Ranks customers by first date, then by total transaction amount
5. Generates sequential customer IDs (KH000001, KH000002, ...)
6. Saves to CSV and uploads to Google Sheets
"""

import logging
from pathlib import Path
from typing import Optional
import pandas as pd

# Google Sheets upload will be handled via Amp MCP tools

# ============================================================================
# CONFIGURATION
# ============================================================================

DATA_DIR = Path.cwd() / "data" / "cleaned"
OUTPUT_DIR = Path.cwd() / "data" / "final"
OUTPUT_FILENAME = "Mã khách hàng mới.csv"
REPORTS_DIR = Path.cwd() / "data" / "reports"
DEBT_FILENAME = "Tổng nợ.csv"

# Google Sheets configuration
SPREADSHEET_ID = "1nulVkpFU1MihYvJDvHfj53cyNvJQhRQbSm_8Ru0IGOU"
SHEET_NAME = "Mã khách hàng mới"
CREDENTIALS_FILE = Path.cwd() / "credentials.json"

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def find_cleaned_export_file() -> Optional[Path]:
    """Find the cleaned export receipt CSV file in data/cleaned directory."""
    csv_files = list(DATA_DIR.glob("Chi tiết xuất*.csv"))
    if csv_files:
        # Return the most recently modified file
        return max(csv_files, key=lambda p: p.stat().st_mtime)
    return None


def get_debt_customers() -> set:
    """Get set of customer names from debt file (Tổng nợ.csv)."""
    debt_file = REPORTS_DIR / DEBT_FILENAME
    if not debt_file.exists():
        logger.warning(f"Debt file not found at {debt_file}")
        return set()

    try:
        df = pd.read_csv(debt_file, encoding="utf-8")
        if "Tên khách hàng" not in df.columns:
            logger.warning("'Tên khách hàng' column not found in debt file")
            return set()

        # Get unique customer names, remove empty/null values
        customers = set(df["Tên khách hàng"].dropna().unique())
        logger.info(f"Loaded {len(customers)} unique customers from debt file")
        return customers
    except Exception as e:
        logger.error(f"Error reading debt file: {e}")
        return set()


def read_export_data(filepath: Path) -> pd.DataFrame:
    """Read cleaned export receipt data."""
    try:
        df = pd.read_csv(filepath, encoding="utf-8")
        logger.info(f"Loaded data from {filepath.name}: {len(df)} rows")
        return df
    except Exception as e:
        logger.error(f"Error reading {filepath}: {e}")
        return pd.DataFrame()


def aggregate_customer_data(df: pd.DataFrame, debt_customers: set) -> pd.DataFrame:
    """Group by customer and aggregate transaction data.

    Only includes customers present in the debt file.
    """
    if "Tên khách hàng" not in df.columns or "Ngày" not in df.columns:
        logger.error("Required columns 'Tên khách hàng' or 'Ngày' not found")
        return pd.DataFrame()

    # Filter to only debt customers
    before_filter = len(df)
    df = df[df["Tên khách hàng"].isin(debt_customers)].copy()
    after_filter = len(df)
    logger.info(
        f"Filtered transactions: {before_filter} → {after_filter} (only debt customers)"
    )

    if df.empty:
        logger.warning("No transactions found for debt customers")
        return pd.DataFrame()

    # Ensure Ngày is datetime
    df["Ngày"] = pd.to_datetime(df["Ngày"], errors="coerce")

    # Calculate total amount (using Thành tiền if available)
    amount_col = "Thành tiền" if "Thành tiền" in df.columns else "Số lượng"

    customer_summary = (
        df.groupby("Tên khách hàng")
        .agg(
            first_date=("Ngày", "min"),
            last_date=("Ngày", "max"),
            total_amount=(amount_col, "sum"),
            transaction_count=("Ngày", "count"),
        )
        .reset_index()
    )

    logger.info(f"Aggregated {len(customer_summary)} unique customers from debt file")
    return customer_summary


def rank_and_generate_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Rank customers and generate sequential customer IDs."""
    # Sort by first_date (earliest first), then by total_amount (highest first)
    df_sorted = df.sort_values(
        by=["first_date", "total_amount"], ascending=[True, False], na_position="last"
    ).reset_index(drop=True)

    # Generate customer IDs
    df_sorted["Mã khách hàng mới"] = df_sorted.index.map(lambda x: f"KH{x + 1:06d}")

    # Reorder columns
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

    # Rename columns for output
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
    """Save customer data to CSV file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"Saved to {output_path}")


def upload_to_sheets(df: pd.DataFrame, spreadsheet_id: str, sheet_name: str) -> None:
    """Upload data to Google Sheets (handled by Amp MCP tools)."""
    logger.info(
        f"To upload to Google Sheets, use Amp MCP:\n"
        f"  Spreadsheet ID: {spreadsheet_id}\n"
        f"  Sheet name: {sheet_name}\n"
        f"  Rows: {len(df)}"
    )


# ============================================================================
# MAIN EXECUTION
# ============================================================================


def main() -> None:
    """Main processing pipeline."""
    logger.info("Starting customer ID generation")

    # Get customers from debt file
    debt_customers = get_debt_customers()
    if not debt_customers:
        logger.error("No customers found in debt file")
        return

    # Find cleaned export file
    export_file = find_cleaned_export_file()
    if not export_file:
        logger.error(f"No cleaned export file found in {DATA_DIR}")
        return

    # Read export data
    df = read_export_data(export_file)
    if df.empty:
        logger.error("No data loaded")
        return

    # Aggregate customer data (filtered to debt customers only)
    customer_summary = aggregate_customer_data(df, debt_customers)
    if customer_summary.empty:
        logger.error("No customer summary generated")
        return

    # Rank and generate IDs
    result_df = rank_and_generate_ids(customer_summary)

    # Save to CSV
    output_path = OUTPUT_DIR / OUTPUT_FILENAME
    save_to_csv(result_df, output_path)

    # Upload to Google Sheets
    try:
        upload_to_sheets(result_df, SPREADSHEET_ID, SHEET_NAME)
        logger.info("Successfully uploaded to Google Sheets")
    except Exception as e:
        logger.warning(f"Failed to upload to Google Sheets: {e}")

    logger.info("Customer ID generation completed")


if __name__ == "__main__":
    main()
