# -*- coding: utf-8 -*-
"""Reconcile purchase and sale receipts with inventory movement data.

Module: import_export_receipts
Purpose: Verify if all purchases (nhập trong kỳ) and sales (xuất trong kỳ)
         are fully captured in the receipt CSVs by comparing summary aggregates
         with inventory source data.

This script:
1. Loads summary aggregation from purchase receipts (Tổng hợp nhập)
2. Loads summary aggregation from sale receipts (Tổng hợp xuất)
3. Loads inventory movement data (xuat_nhap_ton)
4. Compares purchase/sale quantities and values by product and month/year
5. Generates reconciliation report with discrepancies
6. Exports detailed comparison to CSV for manual review
"""

import logging
from pathlib import Path
from typing import Optional

import pandas as pd

# ============================================================================
# CONFIGURATION
# ============================================================================

DATA_STAGING_DIR = Path.cwd() / "data" / "01-staging" / "import_export"
DATA_VALIDATED_DIR = Path.cwd() / "data" / "02-validated"

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def load_summary_files(staging_dir: Path) -> tuple:
    """Load purchase and sale detail files.

    Returns:
        tuple: (purchase_detail_df, sale_detail_df) or (None, None) if not found
    """
    purchase_files = list(staging_dir.glob("Chi tiết nhập*.csv"))
    sale_files = list(staging_dir.glob("Chi tiết xuất*.csv"))

    purchase_df = None
    sale_df = None

    if purchase_files:
        purchase_df = pd.read_csv(purchase_files[0])
        logger.info(f"Loaded purchase detail: {purchase_files[0].name}")
        logger.info(f"  Rows: {len(purchase_df)}")
    else:
        logger.warning("No purchase detail file found (Chi tiết nhập*.csv)")

    if sale_files:
        sale_df = pd.read_csv(sale_files[0])
        logger.info(f"Loaded sale detail: {sale_files[0].name}")
        logger.info(f"  Rows: {len(sale_df)}")
    else:
        logger.warning("No sale detail file found (Chi tiết xuất*.csv)")

    return purchase_df, sale_df


def load_inventory_data(staging_dir: Path) -> Optional[pd.DataFrame]:
    """Load inventory movement data (xuat_nhap_ton).

    Returns:
        pd.DataFrame with columns including:
        - Mã hàng
        - Số lượng nhập trong kỳ
        - Thành tiền nhập trong kỳ
        - Số lượng xuất trong kỳ
        - Thành tiền xuất trong kỳ
    """
    inventory_files = list(staging_dir.glob("Xuất nhập tồn *.csv"))
    inventory_files = [
        f for f in inventory_files if "adjustments" not in f.name.lower()
    ]

    if not inventory_files:
        logger.warning("No inventory file found (xuat_nhap_ton_*.csv)")
        return None

    df = pd.read_csv(inventory_files[0])
    logger.info(f"Loaded inventory data: {inventory_files[0].name}")
    logger.info(f"  Rows: {len(df)}")

    return df


def normalize_product_code(code: str) -> str:
    """Normalize product code for matching."""
    if pd.isna(code):
        return ""
    return str(code).strip().upper()


def reconcile_purchases(
    purchase_summary: pd.DataFrame, inventory_data: pd.DataFrame
) -> tuple:
    """Compare purchase summary with inventory nhập trong kỳ at multiple levels.

    Returns:
        tuple: (product_level_comparison, month_level_comparison)
    """
    # Product-level reconciliation (sum across all months)
    purchase_agg = purchase_summary.groupby("Mã hàng", as_index=False).agg(
        {"Số lượng": "sum", "Thành tiền": "sum"}
    )
    purchase_agg["Mã hàng"] = purchase_agg["Mã hàng"].apply(normalize_product_code)
    purchase_agg.columns = ["Mã hàng", "Số lượng_receipt", "Thành tiền_receipt"]

    inventory_purchase = inventory_data[
        ["Mã hàng", "Số lượng nhập trong kỳ", "Thành tiền nhập trong kỳ"]
    ].copy()
    inventory_purchase["Mã hàng"] = inventory_purchase["Mã hàng"].apply(
        normalize_product_code
    )
    inventory_purchase = inventory_purchase.groupby("Mã hàng", as_index=False).agg(
        {"Số lượng nhập trong kỳ": "sum", "Thành tiền nhập trong kỳ": "sum"}
    )

    product_comparison = pd.merge(
        purchase_agg,
        inventory_purchase,
        on="Mã hàng",
        how="outer",
    )

    product_comparison["Số lượng_diff"] = product_comparison["Số lượng_receipt"].fillna(
        0
    ) - product_comparison["Số lượng nhập trong kỳ"].fillna(0)
    product_comparison["Thành tiền_diff"] = product_comparison[
        "Thành tiền_receipt"
    ].fillna(0) - product_comparison["Thành tiền nhập trong kỳ"].fillna(0)
    product_comparison["Discrepancy"] = (
        product_comparison["Số lượng_diff"].abs() > 0.01
    ) | (product_comparison["Thành tiền_diff"].abs() > 1.0)
    product_comparison = product_comparison.sort_values("Discrepancy", ascending=False)

    # Month-level reconciliation (by Mã hàng, Tháng, Năm)
    purchase_monthly = purchase_summary.groupby(
        ["Mã hàng", "Tháng", "Năm"], as_index=False
    ).agg({"Số lượng": "sum", "Thành tiền": "sum"})
    purchase_monthly["Mã hàng"] = purchase_monthly["Mã hàng"].apply(
        normalize_product_code
    )
    purchase_monthly.columns = [
        "Mã hàng",
        "Tháng",
        "Năm",
        "Số lượng_receipt",
        "Thành tiền_receipt",
    ]

    # Note: inventory data may not have month/year breakdown, aggregate it by Mã hàng only
    # This is a limitation - we can't truly reconcile by month with the current inventory structure
    # So month-level shows receipt data only
    month_comparison = purchase_monthly.copy()
    month_comparison["Note"] = "Inventory data does not have month-level breakdown"

    return product_comparison, month_comparison


def reconcile_sales(sale_summary: pd.DataFrame, inventory_data: pd.DataFrame) -> tuple:
    """Compare sale summary with inventory xuất trong kỳ at multiple levels.

    Returns:
        tuple: (product_level_comparison, month_level_comparison)
    """
    # Product-level reconciliation (sum across all months)
    sale_agg = sale_summary.groupby("Mã hàng", as_index=False).agg(
        {"Số lượng": "sum", "Thành tiền": "sum"}
    )
    sale_agg["Mã hàng"] = sale_agg["Mã hàng"].apply(normalize_product_code)
    sale_agg.columns = ["Mã hàng", "Số lượng_receipt", "Thành tiền_receipt"]

    inventory_sale = inventory_data[
        ["Mã hàng", "Số lượng xuất trong kỳ", "Thành tiền xuất trong kỳ"]
    ].copy()
    inventory_sale["Mã hàng"] = inventory_sale["Mã hàng"].apply(normalize_product_code)
    inventory_sale = inventory_sale.groupby("Mã hàng", as_index=False).agg(
        {"Số lượng xuất trong kỳ": "sum", "Thành tiền xuất trong kỳ": "sum"}
    )

    product_comparison = pd.merge(
        sale_agg,
        inventory_sale,
        on="Mã hàng",
        how="outer",
    )

    product_comparison["Số lượng_diff"] = product_comparison["Số lượng_receipt"].fillna(
        0
    ) - product_comparison["Số lượng xuất trong kỳ"].fillna(0)
    product_comparison["Thành tiền_diff"] = product_comparison[
        "Thành tiền_receipt"
    ].fillna(0) - product_comparison["Thành tiền xuất trong kỳ"].fillna(0)
    product_comparison["Discrepancy"] = (
        product_comparison["Số lượng_diff"].abs() > 0.01
    ) | (product_comparison["Thành tiền_diff"].abs() > 1.0)
    product_comparison = product_comparison.sort_values("Discrepancy", ascending=False)

    # Month-level reconciliation (by Mã hàng, Tháng, Năm)
    sale_monthly = sale_summary.groupby(
        ["Mã hàng", "Tháng", "Năm"], as_index=False
    ).agg({"Số lượng": "sum", "Thành tiền": "sum"})
    sale_monthly["Mã hàng"] = sale_monthly["Mã hàng"].apply(normalize_product_code)
    sale_monthly.columns = [
        "Mã hàng",
        "Tháng",
        "Năm",
        "Số lượng_receipt",
        "Thành tiền_receipt",
    ]

    # Note: inventory data may not have month/year breakdown
    # So month-level shows receipt data only
    month_comparison = sale_monthly.copy()
    month_comparison["Note"] = "Inventory data does not have month-level breakdown"

    return product_comparison, month_comparison


def print_summary_stats(comparison_df: pd.DataFrame, prefix: str) -> None:
    """Print summary statistics for reconciliation."""
    total_rows = len(comparison_df)
    discrepancy_rows = comparison_df["Discrepancy"].sum()

    logger.info(f"{prefix} - Total products compared: {total_rows}")
    logger.info(f"{prefix} - Products with discrepancies: {discrepancy_rows}")

    if discrepancy_rows > 0:
        logger.warning(f"{prefix} - Review required for {discrepancy_rows} products")


# ============================================================================
# MAIN RECONCILIATION FUNCTION
# ============================================================================


def reconcile_receipts_with_inventory(
    staging_dir: Optional[Path] = None,
) -> tuple:
    """Reconcile purchase and sale receipts with inventory data.

    Args:
        staging_dir: Path to staging directory (default: data/01-staging/import_export/)

    Returns:
        tuple: (purchase_reconciliation_df, sale_reconciliation_df)
    """
    logger.info("=" * 70)
    logger.info("Starting receipt reconciliation with inventory")
    logger.info("=" * 70)

    # Use default if not provided
    if staging_dir is None:
        staging_dir = DATA_STAGING_DIR

    # Load data files
    purchase_summary, sale_summary = load_summary_files(staging_dir)
    inventory_data = load_inventory_data(staging_dir)

    if inventory_data is None:
        logger.error("Cannot proceed without inventory data")
        return None, None

    purchase_reconciliation = None
    sale_reconciliation = None

    # Reconcile purchases
    if purchase_summary is not None:
        logger.info("=" * 70)
        logger.info("Reconciling purchase receipts with inventory nhập trong kỳ")
        logger.info("=" * 70)
        purchase_product, purchase_monthly = reconcile_purchases(
            purchase_summary, inventory_data
        )
        print_summary_stats(purchase_product, "PURCHASE (product-level)")

        # Export product-level reconciliation
        reconciliation_file = staging_dir / "reconciliation_purchase_product.csv"
        purchase_product.to_csv(reconciliation_file, index=False, encoding="utf-8")
        logger.info(f"Saved purchase product-level to: {reconciliation_file}")

        # Export month-level reconciliation
        reconciliation_file = staging_dir / "reconciliation_purchase_monthly.csv"
        purchase_monthly.to_csv(reconciliation_file, index=False, encoding="utf-8")
        logger.info(f"Saved purchase monthly-level to: {reconciliation_file}")

        purchase_reconciliation = purchase_product

    # Reconcile sales
    if sale_summary is not None:
        logger.info("=" * 70)
        logger.info("Reconciling sale receipts with inventory xuất trong kỳ")
        logger.info("=" * 70)
        sale_product, sale_monthly = reconcile_sales(sale_summary, inventory_data)
        print_summary_stats(sale_product, "SALE (product-level)")

        # Export product-level reconciliation
        reconciliation_file = staging_dir / "reconciliation_sale_product.csv"
        sale_product.to_csv(reconciliation_file, index=False, encoding="utf-8")
        logger.info(f"Saved sale product-level to: {reconciliation_file}")

        # Export month-level reconciliation
        reconciliation_file = staging_dir / "reconciliation_sale_monthly.csv"
        sale_monthly.to_csv(reconciliation_file, index=False, encoding="utf-8")
        logger.info(f"Saved sale monthly-level to: {reconciliation_file}")

        sale_reconciliation = sale_product

    logger.info("=" * 70)
    logger.info("Reconciliation complete")
    logger.info("=" * 70)

    return purchase_reconciliation, sale_reconciliation


if __name__ == "__main__":
    reconcile_receipts_with_inventory()
