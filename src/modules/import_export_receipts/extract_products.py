# -*- coding: utf-8 -*-
"""Extract product information from cleaned receipt data.

This module processes cleaned import and export receipt data to generate:
1. Product info: Assigns sequential codes (HH000001, etc.) to products
2. Summary reports: Aggregated quantities and amounts by product
3. Inventory: Calculates stock levels using FIFO costing
4. Prices: First and last buying/selling prices
5. Gross profit: Revenue, COGS, and profit margin by product
6. Enrichment: Fetches Nhóm hàng and Thương hiệu from external Google Sheets

Raw sources: clean_receipts_purchase, clean_receipts_sale
Module: import_export_receipts
Pipeline stage: data/01-staging/ → data/02-validated/
"""

import logging
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

# Add parent directory to path for imports
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG = {
    "staging_dir": Path.cwd() / "data" / "01-staging" / "import_export",
    "validated_dir": Path.cwd() / "data" / "02-validated",
    "report_dir": Path.cwd() / "data" / "reports",
    "nhap_pattern": "Chi tiết nhập*.csv",
    "xuat_pattern": "Chi tiết xuất*.csv",
    "product_file": "product_info.csv",
    "nhap_summary_file": "summary_purchase.csv",
    "xuat_summary_file": "summary_sale.csv",
    "inventory_file": "inventory.csv",
    "nhap_price_file": "price_purchase.csv",
    "xuat_price_file": "price_sale.csv",
    "gross_profit_file": "gross_profit.csv",
    "enrichment_file": "enrichment.csv",
    "revenue_profit_file": "revenue_profit_nhom_thuong_hieu.csv",
    # Google Sheets product lookup (external data source)
    "product_lookup_spreadsheet_id": "16bGN2gjWspCqlFD4xB--7WtkYtTpDaWzRQx9sV97ed8",
    "product_lookup_sheet_name": "Nhóm hàng, thương hiệu",
}

# ============================================================================
# LOGGING SETUP
# ============================================================================

logger = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def find_input_file(directory: Path, pattern: str) -> Path:
    """Find the latest file matching pattern in directory.

    Args:
        directory: Directory to search
        pattern: Glob pattern to match files

    Returns:
        Path to the latest matching file

    Raises:
        FileNotFoundError: If no matching files found
    """
    matching_files = list(directory.glob(pattern))

    if not matching_files:
        raise FileNotFoundError(f"No files matching {pattern} found in {directory}")

    # Sort by modification time, return the latest
    latest_file = sorted(matching_files, key=lambda p: p.stat().st_mtime)[-1]
    logger.info(f"Found input file: {latest_file.name}")
    return latest_file


def load_data(file_path: Path) -> pd.DataFrame:
    """Load CSV file with proper data types.

    Args:
        file_path: Path to CSV file

    Returns:
        Loaded DataFrame

    Raises:
        ValueError: If required columns are missing
    """
    df = pd.read_csv(file_path, dtype_backend="numpy_nullable")

    # Ensure required columns exist
    required_cols = ["Mã hàng", "Tên hàng", "Số lượng", "Thành tiền", "Năm"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Convert Năm to integer for grouping
    df["Năm"] = pd.to_numeric(df["Năm"], errors="coerce").astype("Int64")

    logger.info(f"Loaded data with {len(df)} rows and {len(df.columns)} columns")
    return df


def get_longest_name(group: pd.DataFrame) -> str:
    """Get the longest 'Tên hàng' for a product code.

    Args:
        group: DataFrame group for one product

    Returns:
        Longest product name
    """
    names = group["Tên hàng"].dropna().astype(str).unique()
    if len(names) == 0:
        return ""
    return max(names, key=len)


def standardize_brand_names(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize brand names in 'Tên hàng' column.

    Case-insensitive matching with all-caps replacement:
    - CHENGSIN → CHENGSHIN
    - MICHENLIN → MICHELIN
    - CAOSUMINA → CASUMINA

    Args:
        df: DataFrame with product names

    Returns:
        DataFrame with standardized names
    """
    replacements = {
        "chengsin": "CHENGSHIN",
        "michenlin": "MICHELIN",
        "caosumina": "CASUMINA",
    }

    df = df.copy()

    for old, new in replacements.items():
        # Case-insensitive replacement using regex
        df["Tên hàng"] = df["Tên hàng"].str.replace(old, new, case=False, regex=False)

    return df


def aggregate_by_year(
    group: pd.DataFrame, include_year_breakdown: bool = False
) -> Dict[str, float]:
    """Calculate totals for each year and overall.

    Args:
        group: DataFrame group to aggregate
        include_year_breakdown: If True, include year-specific columns

    Returns:
        Dictionary with overall totals and optionally year-specific breakdown
    """
    result = {}

    # Overall totals
    result["Tổng số lượng"] = group["Số lượng"].sum()
    result["Tổng thành tiền"] = group["Thành tiền"].sum()

    # By year (only if requested)
    if include_year_breakdown:
        for year in sorted(group["Năm"].dropna().unique()):
            year_int = int(year)
            year_group = group[group["Năm"] == year_int]
            result[f"Tổng số lượng - {year_int}"] = year_group["Số lượng"].sum()
            result[f"Tổng thành tiền - {year_int}"] = year_group["Thành tiền"].sum()

    return result


def get_sort_key(group: pd.DataFrame) -> Tuple:
    """Get sort key based on date, doc code, and amount.

    Args:
        group: DataFrame group for one product

    Returns:
        Tuple for sorting
    """
    # Use the first occurrence of the product to rank it
    first_row = group.iloc[0]

    # Convert Ngày to datetime for proper sorting (if it's a string)
    try:
        date_val = pd.to_datetime(first_row.get("Ngày", ""), errors="coerce")
    except Exception:
        date_val = pd.NaT

    ma_chung_tu = str(first_row.get("Mã chứng từ", ""))
    thanh_tien = float(first_row.get("Thành tiền", 0) or 0)

    return (date_val, ma_chung_tu, thanh_tien)


def fetch_product_lookup() -> pd.DataFrame:
    """Fetch product lookup data from Google Sheets.

    Loads Mã hàng, Nhóm hàng(3 Cấp), Thương hiệu, and optionally Tên hàng from external Google Sheets.

    Returns:
        DataFrame with columns: Mã hàng, Nhóm hàng(3 Cấp), Thương hiệu, Tên hàng (if available)
        Returns empty DataFrame if fetch fails
    """
    try:
        from google_api import connect_to_drive, read_sheet_data
    except ImportError:
        logger.error("Cannot import google_api. Skipping product lookup enrichment.")
        return pd.DataFrame()

    try:
        logger.info("Fetching product lookup from Google Sheets...")
        drive_service, sheets_service = connect_to_drive()

        spreadsheet_id = CONFIG["product_lookup_spreadsheet_id"]
        sheet_name = CONFIG["product_lookup_sheet_name"]

        # Read data from sheet
        values = read_sheet_data(sheets_service, spreadsheet_id, sheet_name)

        if not values or len(values) < 2:
            logger.warning("Product lookup sheet is empty or has no data")
            return pd.DataFrame()

        # First row is header
        headers = values[0]
        num_cols = len(headers)

        # Ensure all data rows have same number of columns
        data_rows = []
        for row in values[1:]:
            # Slice to match header length if row is longer
            # Pad with empty strings if row is shorter
            if len(row) > num_cols:
                row = row[:num_cols]
            elif len(row) < num_cols:
                row = row + [""] * (num_cols - len(row))
            data_rows.append(row)

        # Create DataFrame
        lookup_df = pd.DataFrame(data_rows, columns=headers)

        # Ensure required columns exist
        required_cols = ["Mã hàng", "Nhóm hàng(3 Cấp)", "Thương hiệu"]
        missing_cols = [col for col in required_cols if col not in lookup_df.columns]

        if missing_cols:
            logger.error(f"Product lookup missing columns: {missing_cols}")
            return pd.DataFrame()

        # Clean data: strip whitespace, handle empty values
        # Include Tên hàng if present (optional column)
        cols_to_clean = required_cols.copy()
        if "Tên hàng" in lookup_df.columns:
            cols_to_clean.append("Tên hàng")
            logger.info("Found 'Tên hàng' column in Google Sheets lookup")

        for col in cols_to_clean:
            lookup_df[col] = lookup_df[col].astype(str).str.strip()
            # Replace empty strings with NaN for proper fillna behavior
            lookup_df.loc[lookup_df[col].isin(["", "None"]), col] = pd.NA

        # Remove duplicate Mã hàng, keep first occurrence
        lookup_df = lookup_df.drop_duplicates(subset=["Mã hàng"], keep="first")

        logger.info(f"Fetched {len(lookup_df)} products from lookup")
        return lookup_df

    except Exception as e:
        logger.error(f"Failed to fetch product lookup: {e}")
        return pd.DataFrame()


def enrich_product_data(
    product_info_df: pd.DataFrame, lookup_df: pd.DataFrame
) -> pd.DataFrame:
    """Merge product info with lookup data to enrich Nhóm hàng, Thương hiệu, Nhóm cha, Nhóm con, Tên hàng.

    Args:
        product_info_df: DataFrame with Mã hàng mới, Mã hàng (Tên hàng is NOT included)
        lookup_df: DataFrame with Mã hàng, Nhóm hàng(3 Cấp), Thương hiệu, Nhóm cha, Nhóm con, Tên hàng

    Returns:
        DataFrame with enriched columns, or product_info with placeholders if lookup fails
    """
    if lookup_df.empty:
        logger.warning("Product lookup is empty, using placeholder values")
        enrichment_df = product_info_df[["Mã hàng"]].copy()
        enrichment_df["Tên hàng"] = ""
        enrichment_df["Nhóm hàng(3 Cấp)"] = "Chưa phân loại"
        enrichment_df["Thương hiệu"] = "Chưa xác định"
        enrichment_df["Nhóm cha"] = ""
        enrichment_df["Nhóm con"] = ""
        return enrichment_df[
            [
                "Mã hàng",
                "Tên hàng",
                "Nhóm hàng(3 Cấp)",
                "Thương hiệu",
                "Nhóm cha",
                "Nhóm con",
            ]
        ]

    # Prepare lookup columns - include Tên hàng (always if available in lookup)
    lookup_cols = [
        "Mã hàng",
        "Nhóm hàng(3 Cấp)",
        "Thương hiệu",
        "Nhóm cha",
        "Nhóm con",
    ]
    has_lookup_ten_hang = "Tên hàng" in lookup_df.columns
    if has_lookup_ten_hang:
        lookup_cols.append("Tên hàng")
        logger.info(
            "Using Google Sheets 'Tên hàng' column as primary source for products.xlsx"
        )

    # Merge on Mã hàng (product_info only has Mã hàng, no Tên hàng)
    enrichment_df = product_info_df[["Mã hàng"]].copy()
    enrichment_df = enrichment_df.merge(
        lookup_df[lookup_cols],
        on="Mã hàng",
        how="left",
        suffixes=("", "_lookup"),
    )

    # If Google Sheets has Tên hàng, use it (replace any lookup suffix)
    if has_lookup_ten_hang and "Tên hàng_lookup" in enrichment_df.columns:
        # Replace empty strings with NaN
        enrichment_df["Tên hàng"] = enrichment_df["Tên hàng_lookup"].replace("", pd.NA)
        enrichment_df = enrichment_df.drop(columns=["Tên hàng_lookup"])

    # If no Tên hàng in lookup, set to empty
    if "Tên hàng" not in enrichment_df.columns:
        enrichment_df["Tên hàng"] = ""

    # Fill missing values with placeholders
    enrichment_df["Nhóm hàng(3 Cấp)"] = enrichment_df["Nhóm hàng(3 Cấp)"].fillna(
        "Chưa phân loại"
    )
    enrichment_df["Thương hiệu"] = enrichment_df["Thương hiệu"].fillna("Chưa xác định")
    enrichment_df["Nhóm cha"] = enrichment_df["Nhóm cha"].fillna("")
    enrichment_df["Nhóm con"] = enrichment_df["Nhóm con"].fillna("")

    matched = enrichment_df["Nhóm hàng(3 Cấp)"].ne("Chưa phân loại").sum()
    total = len(enrichment_df)
    logger.info(f"Enriched {matched}/{total} products with lookup data")

    return enrichment_df[
        [
            "Mã hàng",
            "Tên hàng",
            "Nhóm hàng(3 Cấp)",
            "Thương hiệu",
            "Nhóm cha",
            "Nhóm con",
        ]
    ]


def process_nhap_data(staging_dir: Path) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process purchase receipt data and return (product_info, summary).

    Args:
        staging_dir: Directory containing staged data

    Returns:
        Tuple of (product_info_df, nhap_summary_df)
    """
    logger.info("=" * 70)
    logger.info("Processing import receipt (nhập) data")
    logger.info("=" * 70)

    input_file = find_input_file(staging_dir, CONFIG["nhap_pattern"])
    df = load_data(input_file)

    # Group by Mã hàng
    grouped = df.groupby("Mã hàng", as_index=False)

    # Build product info
    # Note: Tên hàng is NOT extracted here; it will be sourced from Google Sheets lookup
    products = []
    for ma_hang, group in grouped:
        aggregates = aggregate_by_year(group)
        sort_key = get_sort_key(group)

        product_info = {
            "Mã hàng": ma_hang,
            **aggregates,
            "_sort_key": sort_key,
        }
        products.append(product_info)

    # Create DataFrame and sort
    product_df = pd.DataFrame(products)
    product_df = product_df.sort_values(by="_sort_key", na_position="last")
    product_df = product_df.drop(columns=["_sort_key"])

    # Assign sequential product codes
    product_df["Mã hàng mới"] = [f"HH{i + 1:06d}" for i in range(len(product_df))]

    # Split into two outputs
    # 1. Product info: Mã hàng mới | Mã hàng
    #    Note: Tên hàng is NOT included; it will be sourced from Google Sheets
    product_info_df = product_df[["Mã hàng mới", "Mã hàng"]].copy()

    # 2. Summary: Mã hàng mới | Mã hàng | Tổng số lượng | Tổng thành tiền (overall totals only)
    summary_cols = ["Mã hàng mới", "Mã hàng", "Tổng số lượng", "Tổng thành tiền"]
    summary_df = product_df[summary_cols].copy()

    logger.info(f"Extracted {len(product_info_df)} unique products")
    return product_info_df, summary_df


def process_xuat_data(staging_dir: Path, product_info_df: pd.DataFrame) -> pd.DataFrame:
    """Process sales receipt data for products that exist in nhập data.

    Args:
        staging_dir: Directory containing staged data
        product_info_df: Product info from nhập processing

    Returns:
        Sales summary DataFrame
    """
    logger.info("=" * 70)
    logger.info("Processing export receipt (xuất) data")
    logger.info("=" * 70)

    # Get the set of Mã hàng from product_info
    valid_ma_hang = set(product_info_df["Mã hàng"].unique())
    logger.info(f"Filtering xuất data to {len(valid_ma_hang)} products from nhập")

    input_file = find_input_file(staging_dir, CONFIG["xuat_pattern"])
    df = load_data(input_file)

    # Filter to only products in nhập data
    df = df[df["Mã hàng"].isin(valid_ma_hang)].copy()

    if df.empty:
        logger.warning("No xuất data found for nhập products")
        return pd.DataFrame()

    # Group by Mã hàng (same logic as nhập)
    grouped = df.groupby("Mã hàng", as_index=False)

    # Build summary
    summaries = []
    for ma_hang, group in grouped:
        aggregates = aggregate_by_year(group)

        summary = {
            "Mã hàng": ma_hang,
            **aggregates,
        }
        summaries.append(summary)

    summary_df = pd.DataFrame(summaries)

    # Map Mã hàng to Mã hàng mới from product_info
    ma_hang_mapping = product_info_df.set_index("Mã hàng")["Mã hàng mới"].to_dict()
    summary_df["Mã hàng mới"] = summary_df["Mã hàng"].map(ma_hang_mapping)

    # Reorder columns (overall totals only)
    summary_cols = ["Mã hàng mới", "Mã hàng", "Tổng số lượng", "Tổng thành tiền"]
    summary_df = summary_df[summary_cols].copy()

    logger.info(f"Processed xuất data for {len(summary_df)} products")
    return summary_df


def calculate_fifo_cost(
    nhap_group: pd.DataFrame,
    remaining_qty: float,
) -> Dict[str, float]:
    """Calculate FIFO cost for remaining inventory.

    FIFO assumes oldest items are sold first, so remaining inventory
    is composed of the most recent purchases.

    Args:
        nhap_group: Import transactions for one product (sorted by date)
        remaining_qty: Quantity remaining in inventory

    Returns:
        Dict with unit_cost_fifo and total_cost_remaining
    """
    if remaining_qty <= 0:
        return {
            "unit_cost_fifo": 0,
            "total_cost_remaining": 0,
        }

    # Sort by date (oldest first) to apply FIFO
    nhap_group = nhap_group.sort_values("Ngày").reset_index(drop=True)

    # Calculate unit cost for each purchase
    nhap_group = nhap_group.copy()
    nhap_group["Đơn giá"] = nhap_group["Thành tiền"] / nhap_group["Số lượng"]
    nhap_group["Đơn giá"] = nhap_group["Đơn giá"].replace(
        [float("inf"), float("-inf")], 0
    )

    # FIFO: Track units sold from oldest to newest
    qty_to_allocate = remaining_qty
    remaining_cost = 0

    # Process imports in reverse order (newest first) to build remaining inventory
    for idx in range(len(nhap_group) - 1, -1, -1):
        batch_qty = nhap_group.iloc[idx]["Số lượng"]
        batch_unit_cost = nhap_group.iloc[idx]["Đơn giá"]

        if qty_to_allocate <= 0:
            break

        if batch_qty >= qty_to_allocate:
            # This batch has enough to cover remaining quantity
            remaining_cost += qty_to_allocate * batch_unit_cost
            qty_to_allocate = 0
        else:
            # Use entire batch
            remaining_cost += batch_qty * batch_unit_cost
            qty_to_allocate -= batch_qty

    # Calculate weighted average cost for remaining inventory
    if remaining_qty > 0:
        avg_unit_cost = remaining_cost / remaining_qty
    else:
        avg_unit_cost = 0

    return {
        "unit_cost_fifo": avg_unit_cost,
        "total_cost_remaining": remaining_cost,
    }


def process_inventory_data(
    nhap_df: pd.DataFrame,
    xuat_df: pd.DataFrame,
    product_info_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate inventory (tồn) using strict FIFO method.

    Tổng tồn = Tổng nhập - Tổng xuất (only overall, not by year)
    If Tổng xuất > Tổng nhập: Tổng tồn = 0, Giá vốn = 0
    Giá vốn is calculated using strict FIFO (assuming oldest items are sold first)

    Args:
        nhap_df: Purchase transaction data
        xuat_df: Sales transaction data
        product_info_df: Product info

    Returns:
        Inventory DataFrame
    """
    logger.info("=" * 70)
    logger.info("Calculating inventory (tồn) data using strict FIFO")
    logger.info("=" * 70)

    # Get nhập data by product
    nhap_by_product = {}
    for ma_hang, group in nhap_df.groupby("Mã hàng"):
        qty = group["Số lượng"].sum()
        nhap_by_product[ma_hang] = {
            "tong_so_luong_nhap": qty,
            "nhap_group": group,
        }

    # Get xuất data by product (overall totals only)
    xuat_by_product = {}
    for ma_hang, group in xuat_df.groupby("Mã hàng"):
        qty = group["Số lượng"].sum()
        xuat_by_product[ma_hang] = qty

    # Calculate inventory for each product in product_info
    inventory_records = []

    for _, row in product_info_df.iterrows():
        ma_hang = row["Mã hàng"]
        ma_hang_moi = row["Mã hàng mới"]

        nhap_info = nhap_by_product.get(
            ma_hang,
            {
                "tong_so_luong_nhap": 0,
                "nhap_group": pd.DataFrame(),
            },
        )

        qty_nhap = nhap_info["tong_so_luong_nhap"]
        qty_xuat = xuat_by_product.get(ma_hang, 0)

        # Calculate remaining inventory
        qty_ton = max(0, qty_nhap - qty_xuat)  # Never negative

        # Calculate FIFO cost for remaining inventory
        if qty_ton == 0:
            # No inventory remaining
            thanh_tien_ton = 0
            gia_von = 0
        else:
            # Use strict FIFO to calculate cost of remaining items
            fifo_info = calculate_fifo_cost(nhap_info["nhap_group"], qty_ton)
            gia_von = fifo_info["unit_cost_fifo"]
            thanh_tien_ton = fifo_info["total_cost_remaining"]

        inventory_records.append(
            {
                "Mã hàng mới": ma_hang_moi,
                "Mã hàng": ma_hang,
                "Tổng số lượng": qty_ton,
                "Tổng thành tiền": thanh_tien_ton,
                "Giá vốn": gia_von,
            }
        )

    inventory_df = pd.DataFrame(inventory_records)

    # Convert numeric columns to proper format
    for col in ["Tổng số lượng", "Tổng thành tiền", "Giá vốn"]:
        inventory_df[col] = pd.to_numeric(inventory_df[col], errors="coerce")

    logger.info(f"Calculated inventory for {len(inventory_df)} products")
    return inventory_df


def process_nhap_price_data(
    nhap_df: pd.DataFrame,
    product_info_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate first and last buying prices for each product.

    Args:
        nhap_df: Purchase transaction data
        product_info_df: Product info

    Returns:
        Purchase prices DataFrame
    """
    logger.info("=" * 70)
    logger.info("Calculating import prices (Giá nhập) data")
    logger.info("=" * 70)

    # Create mapping of Mã hàng to Mã hàng mới
    ma_hang_mapping = product_info_df.set_index("Mã hàng")["Mã hàng mới"].to_dict()

    # Calculate unit price for each transaction
    nhap_df = nhap_df.copy()
    nhap_df["Đơn giá"] = nhap_df["Thành tiền"] / nhap_df["Số lượng"]
    nhap_df["Đơn giá"] = nhap_df["Đơn giá"].replace([float("inf"), float("-inf")], 0)
    nhap_df["Ngày"] = pd.to_datetime(nhap_df["Ngày"], errors="coerce")

    price_records = []

    for ma_hang in product_info_df["Mã hàng"].unique():
        product_data = nhap_df[nhap_df["Mã hàng"] == ma_hang].copy()

        if product_data.empty:
            continue

        # Sort by date
        product_data = product_data.sort_values("Ngày")

        # Get first and last prices
        first_price = product_data.iloc[0]["Đơn giá"]
        last_price = product_data.iloc[-1]["Đơn giá"]
        first_date = (
            product_data.iloc[0]["Ngày"].strftime("%Y-%m-%d")
            if pd.notna(product_data.iloc[0]["Ngày"])
            else ""
        )
        last_date = (
            product_data.iloc[-1]["Ngày"].strftime("%Y-%m-%d")
            if pd.notna(product_data.iloc[-1]["Ngày"])
            else ""
        )

        price_records.append(
            {
                "Mã hàng mới": ma_hang_mapping.get(ma_hang, ""),
                "Mã hàng": ma_hang,
                "Giá nhập đầu": first_price,
                "Giá nhập cuối": last_price,
                "Ngày nhập đầu": first_date,
                "Ngày nhập cuối": last_date,
            }
        )

    price_df = pd.DataFrame(price_records)

    # Convert numeric columns
    for col in ["Giá nhập đầu", "Giá nhập cuối"]:
        price_df[col] = pd.to_numeric(price_df[col], errors="coerce")

    logger.info(f"Calculated prices for {len(price_df)} products")
    return price_df


def process_xuat_price_data(
    xuat_df: pd.DataFrame,
    product_info_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate max selling prices for each product (grouped by customer first).

    Groups by {'Tên khách hàng', 'Mã hàng'}, then gets max price by 'Mã hàng'.

    Args:
        xuat_df: Sales transaction data
        product_info_df: Product info

    Returns:
        Sales prices DataFrame
    """
    logger.info("=" * 70)
    logger.info("Calculating export prices (Giá xuất) data")
    logger.info("=" * 70)

    # Create mapping of Mã hàng to Mã hàng mới
    ma_hang_mapping = product_info_df.set_index("Mã hàng")["Mã hàng mới"].to_dict()

    # Calculate unit price for each transaction
    xuat_df = xuat_df.copy()
    xuat_df["Đơn giá"] = xuat_df["Thành tiền"] / xuat_df["Số lượng"]
    xuat_df["Đơn giá"] = xuat_df["Đơn giá"].replace([float("inf"), float("-inf")], 0)
    xuat_df["Ngày"] = pd.to_datetime(xuat_df["Ngày"], errors="coerce")

    # Filter to only products in product_info
    valid_ma_hang = set(product_info_df["Mã hàng"].unique())
    xuat_df = xuat_df[xuat_df["Mã hàng"].isin(valid_ma_hang)].copy()

    if xuat_df.empty:
        logger.warning("No xuất data found for calculating prices")
        return pd.DataFrame()

    price_records = []

    for ma_hang in sorted(valid_ma_hang):
        product_data = xuat_df[xuat_df["Mã hàng"] == ma_hang].copy()

        if product_data.empty:
            continue

        # Sort by date
        product_data = product_data.sort_values("Ngày")

        # Group by customer and find first/last for each customer
        customer_prices = {}
        for khach_hang, group in product_data.groupby("Tên khách hàng"):
            group = group.sort_values("Ngày")
            customer_prices[khach_hang] = {
                "first_price": group.iloc[0]["Đơn giá"],
                "last_price": group.iloc[-1]["Đơn giá"],
                "first_date": group.iloc[0]["Ngày"],
                "last_date": group.iloc[-1]["Ngày"],
            }

        # Get max prices across all customers
        all_prices = [v["last_price"] for v in customer_prices.values()]
        max_price = max(all_prices) if all_prices else 0

        # Find which customer/transaction has the max price
        max_price_entry = None
        for khach_hang, prices in customer_prices.items():
            if prices["last_price"] == max_price:
                max_price_entry = {
                    "khach_hang": khach_hang,
                    "first_price": prices["first_price"],
                    "last_price": prices["last_price"],
                    "first_date": prices["first_date"],
                    "last_date": prices["last_date"],
                }
                break

        if max_price_entry is None:
            max_price_entry = {
                "khach_hang": list(customer_prices.keys())[0],
                "first_price": list(customer_prices.values())[0]["first_price"],
                "last_price": max_price,
                "first_date": product_data.iloc[0]["Ngày"],
                "last_date": product_data.iloc[-1]["Ngày"],
            }

        first_date_str = (
            max_price_entry["first_date"].strftime("%Y-%m-%d")
            if pd.notna(max_price_entry["first_date"])
            else ""
        )
        last_date_str = (
            max_price_entry["last_date"].strftime("%Y-%m-%d")
            if pd.notna(max_price_entry["last_date"])
            else ""
        )

        price_records.append(
            {
                "Mã hàng mới": ma_hang_mapping.get(ma_hang, ""),
                "Mã hàng": ma_hang,
                "Giá xuất đầu": max_price_entry["first_price"],
                "Giá xuất cuối": max_price_entry["last_price"],
                "Ngày xuất đầu": first_date_str,
                "Ngày xuất cuối": last_date_str,
            }
        )

    price_df = pd.DataFrame(price_records)

    # Convert numeric columns
    for col in ["Giá xuất đầu", "Giá xuất cuối"]:
        price_df[col] = pd.to_numeric(price_df[col], errors="coerce")

    logger.info(f"Calculated selling prices for {len(price_df)} products")
    return price_df


def process_gross_profit_data(
    nhap_df: pd.DataFrame,
    xuat_df: pd.DataFrame,
    product_info_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate gross profit (Lãi gộp) and margin using FIFO costing.

    Gross Profit = Total Sales Revenue - FIFO-based Cost of Goods Sold
    Gross Profit Margin = (Gross Profit / Total Sales Revenue) * 100%

    Args:
        nhap_df: Purchase transaction data
        xuat_df: Sales transaction data
        product_info_df: Product info

    Returns:
        Gross profit DataFrame with columns:
        - Mã hàng mới, Mã hàng, Tên hàng
        - Tổng doanh thu (revenue from xuất)
        - Giá vốn FIFO (COGS from nhập using FIFO)
        - Lãi gộp (Gross Profit)
        - Biên lãi gộp (%) (Gross Profit Margin)
    """
    logger.info("=" * 70)
    logger.info("Calculating gross profit (Lãi gộp) data with FIFO costing")
    logger.info("=" * 70)

    # Create mapping of Mã hàng
    ma_hang_mapping = product_info_df.set_index("Mã hàng")["Mã hàng mới"].to_dict()

    # Prepare data
    nhap_df = nhap_df.copy()
    xuat_df = xuat_df.copy()

    # Parse dates
    nhap_df["Ngày"] = pd.to_datetime(nhap_df["Ngày"], errors="coerce")
    xuat_df["Ngày"] = pd.to_datetime(xuat_df["Ngày"], errors="coerce")

    # Convert columns to numeric
    nhap_df["Số lượng"] = pd.to_numeric(nhap_df["Số lượng"], errors="coerce")
    nhap_df["Thành tiền"] = pd.to_numeric(nhap_df["Thành tiền"], errors="coerce")
    xuat_df["Số lượng"] = pd.to_numeric(xuat_df["Số lượng"], errors="coerce")
    xuat_df["Thành tiền"] = pd.to_numeric(xuat_df["Thành tiền"], errors="coerce")

    # Get valid products
    valid_ma_hang = set(product_info_df["Mã hàng"].unique())

    profit_records = []

    for ma_hang in sorted(valid_ma_hang):
        nhap_group = nhap_df[nhap_df["Mã hàng"] == ma_hang].copy()
        xuat_group = xuat_df[xuat_df["Mã hàng"] == ma_hang].copy()

        # Calculate total sales revenue
        total_revenue = xuat_group["Thành tiền"].sum() if not xuat_group.empty else 0

        # Calculate total quantity sold and purchased
        total_qty_sold = xuat_group["Số lượng"].sum() if not xuat_group.empty else 0
        total_qty_nhap = nhap_group["Số lượng"].sum() if not nhap_group.empty else 0
        total_cost_nhap = nhap_group["Thành tiền"].sum() if not nhap_group.empty else 0

        # Calculate FIFO COGS
        # COGS = Total Import Cost - Cost of Remaining Inventory
        # Remaining Inventory = Total Purchased - Total Sold (if positive)
        cogs_fifo = 0
        if total_cost_nhap > 0:
            remaining_qty = max(0, total_qty_nhap - total_qty_sold)

            if remaining_qty > 0:
                # Calculate cost of remaining inventory using FIFO
                fifo_info = calculate_fifo_cost(nhap_group, remaining_qty)
                cost_remaining = fifo_info["total_cost_remaining"]
                # COGS = Total import cost - cost of remaining inventory
                cogs_fifo = total_cost_nhap - cost_remaining
            else:
                # All items were sold, COGS = Total import cost
                cogs_fifo = total_cost_nhap

        # Calculate gross profit and margin
        lai_gop = total_revenue - cogs_fifo
        bien_lai_gop = (lai_gop / total_revenue * 100) if total_revenue > 0 else 0

        profit_records.append(
            {
                "Mã hàng mới": ma_hang_mapping.get(ma_hang, ""),
                "Mã hàng": ma_hang,
                "Tổng doanh thu": total_revenue,
                "Giá vốn FIFO": cogs_fifo,
                "Lãi gộp": lai_gop,
                "Biên lãi gộp (%)": bien_lai_gop,
            }
        )

    profit_df = pd.DataFrame(profit_records)

    # Convert numeric columns
    for col in ["Tổng doanh thu", "Giá vốn FIFO", "Lãi gộp", "Biên lãi gộp (%)"]:
        profit_df[col] = pd.to_numeric(profit_df[col], errors="coerce")

    logger.info(f"Calculated gross profit for {len(profit_df)} products")
    return profit_df


def calculate_cogs_per_transaction(
    nhap_df: pd.DataFrame,
    xuat_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate FIFO COGS for each export transaction.

    Args:
        nhap_df: Purchase transaction data
        xuat_df: Sales transaction data

    Returns:
        Export DataFrame with additional COGS column
    """
    logger.info("=" * 70)
    logger.info("Calculating FIFO COGS per export transaction")
    logger.info("=" * 70)

    nhap_df = nhap_df.copy()
    xuat_df = xuat_df.copy()

    # Parse dates and convert to numeric
    nhap_df["Ngày"] = pd.to_datetime(nhap_df["Ngày"], errors="coerce")
    xuat_df["Ngày"] = pd.to_datetime(xuat_df["Ngày"], errors="coerce")

    for col in ["Số lượng", "Thành tiền"]:
        nhap_df[col] = pd.to_numeric(nhap_df[col], errors="coerce")
        xuat_df[col] = pd.to_numeric(xuat_df[col], errors="coerce")

    # Group exports by product and calculate COGS
    xuat_with_cogs = []

    for ma_hang in xuat_df["Mã hàng"].unique():
        nhap_group = nhap_df[nhap_df["Mã hàng"] == ma_hang].copy()
        xuat_group = xuat_df[xuat_df["Mã hàng"] == ma_hang].copy()

        if nhap_group.empty or xuat_group.empty:
            for _, xuat_row in xuat_group.iterrows():
                xuat_with_cogs.append(
                    {
                        "Mã hàng": ma_hang,
                        "Số lượng": xuat_row["Số lượng"],
                        "Thành tiền": xuat_row["Thành tiền"],
                        "Ngày": xuat_row["Ngày"],
                        "Năm": xuat_row.get("Năm", None),
                        "Tháng": xuat_row.get("Tháng", None),
                        "COGS": 0,
                    }
                )
            continue

        # Sort imports by date (oldest first)
        nhap_group = nhap_group.sort_values("Ngày").reset_index(drop=True)
        nhap_group["Đơn giá"] = nhap_group["Thành tiền"] / nhap_group["Số lượng"]
        nhap_group["Đơn giá"] = nhap_group["Đơn giá"].replace(
            [float("inf"), float("-inf")], 0
        )

        # Create FIFO queue for imports
        import_queue = []
        for _, nhap_row in nhap_group.iterrows():
            import_queue.append(
                {
                    "remaining_qty": nhap_row["Số lượng"],
                    "unit_cost": nhap_row["Đơn giá"],
                }
            )

        # Process exports in date order
        xuat_group = xuat_group.sort_values("Ngày").reset_index(drop=True)

        for _, xuat_row in xuat_group.iterrows():
            qty_to_sell = xuat_row["Số lượng"]
            cogs = 0

            while qty_to_sell > 0 and import_queue:
                batch = import_queue[0]

                if batch["remaining_qty"] >= qty_to_sell:
                    cogs += qty_to_sell * batch["unit_cost"]
                    batch["remaining_qty"] -= qty_to_sell
                    qty_to_sell = 0

                    if batch["remaining_qty"] == 0:
                        import_queue.pop(0)
                else:
                    cogs += batch["remaining_qty"] * batch["unit_cost"]
                    qty_to_sell -= batch["remaining_qty"]
                    import_queue.pop(0)

            xuat_with_cogs.append(
                {
                    "Mã hàng": ma_hang,
                    "Số lượng": xuat_row["Số lượng"],
                    "Thành tiền": xuat_row["Thành tiền"],
                    "Ngày": xuat_row["Ngày"],
                    "Năm": xuat_row.get("Năm", None),
                    "Tháng": xuat_row.get("Tháng", None),
                    "COGS": cogs,
                }
            )

    result_df = pd.DataFrame(xuat_with_cogs)
    logger.info(f"Calculated COGS for {len(result_df)} export transactions")
    return result_df


def process_revenue_profit_by_dimensions(
    xuat_with_cogs_df: pd.DataFrame,
    nhap_df: pd.DataFrame,
    lookup_df: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate revenue and gross profit by Year, Month, Nhóm hàng, Thương hiệu.

    Args:
        xuat_with_cogs_df: Export data with COGS column
        nhap_df: Import transaction data
        lookup_df: Product lookup with Nhóm hàng, Thương hiệu

    Returns:
        Aggregated DataFrame with revenue, COGS, gross profit, and transaction counts by dimensions
    """
    logger.info("=" * 70)
    logger.info("Calculating revenue and gross profit by dimensions")
    logger.info("=" * 70)

    xuat_df = xuat_with_cogs_df.copy()

    # Merge export data with lookup data
    if not lookup_df.empty:
        xuat_df = xuat_df.merge(
            lookup_df[["Mã hàng", "Nhóm hàng(3 Cấp)", "Thương hiệu"]],
            on="Mã hàng",
            how="left",
        )

        xuat_df["Nhóm hàng(3 Cấp)"] = xuat_df["Nhóm hàng(3 Cấp)"].fillna(
            "Chưa phân loại"
        )
        xuat_df["Thương hiệu"] = xuat_df["Thương hiệu"].fillna("Chưa xác định")
    else:
        xuat_df["Nhóm hàng(3 Cấp)"] = "Chưa phân loại"
        xuat_df["Thương hiệu"] = "Chưa xác định"

    # Convert columns to numeric
    for col in ["Năm", "Tháng", "Thành tiền", "COGS"]:
        xuat_df[col] = pd.to_numeric(xuat_df[col], errors="coerce")

    # Calculate gross profit per transaction
    xuat_df["Lãi gộp"] = xuat_df["Thành tiền"] - xuat_df["COGS"]

    # Aggregate export data by dimensions
    xuat_grouped = (
        xuat_df.groupby(["Năm", "Tháng", "Nhóm hàng(3 Cấp)", "Thương hiệu"])
        .agg(
            {
                "Thành tiền": "sum",
                "COGS": "sum",
                "Lãi gộp": "sum",
                "Mã hàng": "count",
            }
        )
        .reset_index()
    )

    # Rename columns
    xuat_grouped.columns = [
        "Năm",
        "Tháng",
        "Nhóm hàng(3 Cấp)",
        "Thương hiệu",
        "Tổng doanh thu",
        "Giá vốn FIFO",
        "Lãi gộp",
        "Số lần xuất",
    ]

    # Prepare import data for counting
    nhap_counts = nhap_df.copy()

    # Merge import data with lookup data
    if not lookup_df.empty:
        nhap_counts = nhap_counts.merge(
            lookup_df[["Mã hàng", "Nhóm hàng(3 Cấp)", "Thương hiệu"]],
            on="Mã hàng",
            how="left",
        )

        nhap_counts["Nhóm hàng(3 Cấp)"] = nhap_counts["Nhóm hàng(3 Cấp)"].fillna(
            "Chưa phân loại"
        )
        nhap_counts["Thương hiệu"] = nhap_counts["Thương hiệu"].fillna("Chưa xác định")
    else:
        nhap_counts["Nhóm hàng(3 Cấp)"] = "Chưa phân loại"
        nhap_counts["Thương hiệu"] = "Chưa xác định"

    # Convert columns to numeric
    for col in ["Năm", "Tháng"]:
        nhap_counts[col] = pd.to_numeric(nhap_counts[col], errors="coerce")

    # Count import transactions by dimensions
    nhap_grouped = (
        nhap_counts.groupby(["Năm", "Tháng", "Nhóm hàng(3 Cấp)", "Thương hiệu"])
        .agg({"Mã hàng": "count"})
        .reset_index()
    )
    nhap_grouped.columns = [
        "Năm",
        "Tháng",
        "Nhóm hàng(3 Cấp)",
        "Thương hiệu",
        "Số lần nhập",
    ]

    # Merge export and import counts
    result = xuat_grouped.merge(
        nhap_grouped,
        on=["Năm", "Tháng", "Nhóm hàng(3 Cấp)", "Thương hiệu"],
        how="left",
    )

    # Fill missing import counts with 0
    result["Số lần nhập"] = result["Số lần nhập"].fillna(0)

    # Calculate margin
    result["Biên lãi gộp (%)"] = (
        (result["Lãi gộp"] / result["Tổng doanh thu"] * 100)
        .where(result["Tổng doanh thu"] > 0, 0)
        .round(2)
    )

    # Convert columns to numeric
    for col in [
        "Tổng doanh thu",
        "Giá vốn FIFO",
        "Lãi gộp",
        "Biên lãi gộp (%)",
        "Số lần xuất",
        "Số lần nhập",
    ]:
        result[col] = pd.to_numeric(result[col], errors="coerce")

    # Reorder columns
    result = result[
        [
            "Năm",
            "Tháng",
            "Nhóm hàng(3 Cấp)",
            "Thương hiệu",
            "Số lần nhập",
            "Số lần xuất",
            "Tổng doanh thu",
            "Giá vốn FIFO",
            "Lãi gộp",
            "Biên lãi gộp (%)",
        ]
    ]

    # Sort by dimensions
    result = result.sort_values(["Năm", "Tháng", "Nhóm hàng(3 Cấp)", "Thương hiệu"])

    logger.info(f"Generated {len(result)} dimension combinations")
    return result


def process(staging_dir: Optional[Path] = None) -> Optional[Path]:
    """Process product extraction pipeline.

    Args:
        staging_dir: Directory with staged receipt data (defaults to config)

    Returns:
        Path to validated output directory or None if failed
    """
    if staging_dir is None:
        staging_dir = CONFIG["staging_dir"]

    if not staging_dir.exists():
        logger.error(f"Staging directory not found: {staging_dir}")
        return None

    logger.info("=" * 70)
    logger.info("STARTING PRODUCT EXTRACTION PIPELINE")
    logger.info("=" * 70)

    try:
        # Step 1: Process nhập data
        product_info_df, nhap_summary_df = process_nhap_data(staging_dir)

        # Step 2: Load raw nhập and xuất data for inventory calculation
        nhap_file = find_input_file(staging_dir, CONFIG["nhap_pattern"])
        nhap_raw_df = pd.read_csv(nhap_file, dtype_backend="numpy_nullable")

        xuat_file = find_input_file(staging_dir, CONFIG["xuat_pattern"])
        xuat_raw_df = pd.read_csv(xuat_file, dtype_backend="numpy_nullable")

        # Step 3: Process xuất data
        xuat_summary_df = process_xuat_data(staging_dir, product_info_df)

        # Step 4: Calculate inventory (tồn) data
        inventory_df = process_inventory_data(nhap_raw_df, xuat_raw_df, product_info_df)

        # Step 5: Calculate buying prices (Giá nhập) data
        nhap_price_df = process_nhap_price_data(nhap_raw_df, product_info_df)

        # Step 6: Calculate selling prices (Giá xuất) data
        xuat_price_df = process_xuat_price_data(xuat_raw_df, product_info_df)

        # Step 7: Calculate gross profit (Lãi gộp) data
        gross_profit_df = process_gross_profit_data(
            nhap_raw_df, xuat_raw_df, product_info_df
        )

        # Step 8: Create output directory
        output_dir = CONFIG["validated_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)

        # Step 9: Standardize brand names and save product info
        product_info_df = standardize_brand_names(product_info_df)
        product_path = output_dir / CONFIG["product_file"]
        product_info_df.sort_values("Mã hàng mới").to_csv(
            product_path, index=False, encoding="utf-8"
        )
        logger.info(f"Product info saved to: {product_path}")
        logger.info(f"  Total products: {len(product_info_df)}")

        # Step 10: Save nhập summary
        nhap_path = output_dir / CONFIG["nhap_summary_file"]
        nhap_summary_df.sort_values("Mã hàng mới").to_csv(
            nhap_path, index=False, encoding="utf-8"
        )
        logger.info(f"Nhập summary saved to: {nhap_path}")
        logger.info(f"  Columns: {', '.join(nhap_summary_df.columns)}")

        # Step 11: Save xuất summary
        if not xuat_summary_df.empty:
            xuat_path = output_dir / CONFIG["xuat_summary_file"]
            xuat_summary_df.sort_values("Mã hàng mới").to_csv(
                xuat_path, index=False, encoding="utf-8"
            )
            logger.info(f"Xuất summary saved to: {xuat_path}")
            logger.info(f"  Products with xuất data: {len(xuat_summary_df)}")
            logger.info(f"  Columns: {', '.join(xuat_summary_df.columns)}")
        else:
            logger.warning("No xuất summary data generated")

        # Step 12: Save inventory summary
        if not inventory_df.empty:
            inventory_path = output_dir / CONFIG["inventory_file"]
            inventory_df.sort_values("Mã hàng mới").to_csv(
                inventory_path, index=False, encoding="utf-8"
            )
            logger.info(f"Inventory summary saved to: {inventory_path}")
            logger.info(
                f"  Products with inventory: {len(inventory_df[inventory_df['Tổng số lượng'] > 0])}"
            )
            logger.info(f"  Columns: {', '.join(inventory_df.columns)}")
        else:
            logger.warning("No inventory data generated")

        # Step 13: Save import prices
        if not nhap_price_df.empty:
            nhap_price_path = output_dir / CONFIG["nhap_price_file"]
            nhap_price_df.sort_values("Mã hàng mới").to_csv(
                nhap_price_path, index=False, encoding="utf-8"
            )
            logger.info(f"Import prices saved to: {nhap_price_path}")
            logger.info(f"  Columns: {', '.join(nhap_price_df.columns)}")
        else:
            logger.warning("No import price data generated")

        # Step 14: Save export prices
        if not xuat_price_df.empty:
            xuat_price_path = output_dir / CONFIG["xuat_price_file"]
            xuat_price_df.sort_values("Mã hàng mới").to_csv(
                xuat_price_path, index=False, encoding="utf-8"
            )
            logger.info(f"Export prices saved to: {xuat_price_path}")
            logger.info(f"  Columns: {', '.join(xuat_price_df.columns)}")
        else:
            logger.warning("No export price data generated")

        # Step 15: Save gross profit
        if not gross_profit_df.empty:
            gross_profit_path = output_dir / CONFIG["gross_profit_file"]
            gross_profit_df.sort_values("Mã hàng mới").to_csv(
                gross_profit_path, index=False, encoding="utf-8"
            )
            logger.info(f"Gross profit data saved to: {gross_profit_path}")
            logger.info(f"  Columns: {', '.join(gross_profit_df.columns)}")
        else:
            logger.warning("No gross profit data generated")

        # Step 16: Calculate revenue and gross profit by dimensions
        logger.info("=" * 70)
        logger.info("Calculating revenue and gross profit by dimensions")
        logger.info("=" * 70)

        xuat_with_cogs_df = calculate_cogs_per_transaction(nhap_raw_df, xuat_raw_df)
        lookup_df = fetch_product_lookup()
        revenue_profit_df = process_revenue_profit_by_dimensions(
            xuat_with_cogs_df, nhap_raw_df, lookup_df
        )

        if not revenue_profit_df.empty:
            revenue_profit_path = output_dir / CONFIG["revenue_profit_file"]
            revenue_profit_df.to_csv(revenue_profit_path, index=False, encoding="utf-8")
            logger.info(f"Revenue/Profit by dimensions saved to: {revenue_profit_path}")
            logger.info(f"  Columns: {', '.join(revenue_profit_df.columns)}")
            logger.info(f"  Total combinations: {len(revenue_profit_df)}")
        else:
            logger.warning("No revenue/profit by dimensions data generated")

        # Step 17: Enrich with product lookup and save enrichment

        # Step 17: Enrich with product lookup and save enrichment
        logger.info("=" * 70)
        logger.info("Creating product enrichment (Nhóm hàng, Thương hiệu)")
        logger.info("=" * 70)

        enrichment_df = enrich_product_data(product_info_df, lookup_df)

        if not enrichment_df.empty:
            enrichment_path = output_dir / CONFIG["enrichment_file"]
            enrichment_df.to_csv(enrichment_path, index=False, encoding="utf-8")
            logger.info(f"Enrichment data saved to: {enrichment_path}")
            logger.info(f"  Columns: {', '.join(enrichment_df.columns)}")
        else:
            logger.warning("No enrichment data generated")

        logger.info("=" * 70)
        logger.info("PRODUCT EXTRACTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)

        return output_dir

    except Exception as e:
        logger.error(f"Product extraction pipeline failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    process()
