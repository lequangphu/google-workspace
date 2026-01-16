"""Reconcile two tabs to understand discrepancy root cause.

This script reads 'Chi tiết xuất' and 'CT.XUAT' tabs from a spreadsheet,
compares them to find differences, and reports the root cause of discrepancies.

Usage:
    uv run scripts/reconcile_export_tabs.py <spreadsheet_id>
"""

import logging
import sys
from typing import Dict

import pandas as pd

from src.modules.google_api import connect_to_drive, read_sheet_data

logger = logging.getLogger(__name__)


def read_tab_to_dataframe(sheets_service, spreadsheet_id: str, tab_name: str) -> pd.DataFrame:
    """Read tab data into pandas DataFrame.

    Handles mismatched column counts by using max columns across all rows.

    Args:
        sheets_service: Google Sheets API service.
        spreadsheet_id: ID of spreadsheet.
        tab_name: Name of tab to read.

    Returns:
        DataFrame with data, or empty DataFrame if no data.
    """
    logger.info(f"Reading tab: {tab_name}")
    values = read_sheet_data(sheets_service, spreadsheet_id, tab_name)

    if not values:
        logger.warning(f"No data in tab '{tab_name}'")
        return pd.DataFrame()

    if len(values) < 2:
        logger.warning(f"No data rows in tab '{tab_name}'")
        return pd.DataFrame()

    headers = values[0]
    data = values[1:]

    # Handle mismatched column counts
    max_cols = max(len(headers), max(len(row) for row in data) if data else 0)
    logger.info(f"Columns: header={len(headers)}, max_data={max_cols}")

    # Pad headers to match max columns
    while len(headers) < max_cols:
        headers.append(f"col_{len(headers)}")

    # Pad data rows to match headers
    padded_data = []
    for row in data:
        padded_row = row.copy()
        while len(padded_row) < max_cols:
            padded_row.append("")
        padded_data.append(padded_row)

    df = pd.DataFrame(padded_data, columns=headers)
    logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

    return df


def standardize_product_name(name: str) -> str:
    """Standardize product name for comparison."""
    if pd.isna(name):
        return ""
    name = str(name).strip()
    # Collapse multiple spaces
    name = " ".join(name.split())
    # Convert to uppercase for comparison
    return name.upper()


def standardize_product_code(code: str) -> str:
    """Standardize product code for comparison."""
    if pd.isna(code):
        return ""
    return str(code).strip().upper()


def compare_dataframes(
    df1: pd.DataFrame, df2: pd.DataFrame, name1: str, name2: str
) -> Dict:
    """Compare two DataFrames and report differences.

    Args:
        df1: First DataFrame.
        df2: Second DataFrame.
        name1: Name of first DataFrame.
        name2: Name of second DataFrame.

    Returns:
        Dict with comparison results.
    """
    results = {
        "total_rows_df1": len(df1),
        "total_rows_df2": len(df2),
        "row_count_diff": len(df1) - len(df2),
        "common_products": [],
        "only_in_df1": [],
        "only_in_df2": [],
        "quantity_diffs": [],
        "total_qty_df1": 0,
        "total_qty_df2": 0,
    }

    # Identify key columns
    code_col_df1 = None
    name_col_df1 = None
    qty_col_df1 = None

    for col in df1.columns:
        col_upper = str(col).upper().strip()
        if "MÃ HÀNG" in col_upper or "CODE" in col_upper:
            code_col_df1 = col
        elif "TÊN HÀNG" in col_upper or "NAME" in col_upper or "TÊN SẢN PHẨM" in col_upper:
            name_col_df1 = col
        elif "SỐ LƯỢNG" in col_upper or "SL" in col_upper and col_upper != "TỔNG SL":
            qty_col_df1 = col

    code_col_df2 = None
    name_col_df2 = None
    qty_col_df2 = None

    for col in df2.columns:
        col_upper = str(col).upper().strip()
        if "MÃ HÀNG" in col_upper or "CODE" in col_upper:
            code_col_df2 = col
        elif "TÊN HÀNG" in col_upper or "NAME" in col_upper or "TÊN SẢN PHẨM" in col_upper:
            name_col_df2 = col
        elif "SỐ LƯỢNG" in col_upper or "SL" in col_upper and col_upper != "TỔNG SL":
            qty_col_df2 = col

    logger.info(f"{name1} key columns: code={code_col_df1}, name={name_col_df1}, qty={qty_col_df1}")
    logger.info(f"{name2} key columns: code={code_col_df2}, name={name_col_df2}, qty={qty_col_df2}")

    # If no product code, use product name as key
    key_col_df1 = code_col_df1 if code_col_df1 else name_col_df1
    key_col_df2 = code_col_df2 if code_col_df2 else name_col_df2

    if not key_col_df1 or not key_col_df2:
        logger.error("Could not identify key columns for comparison")
        logger.info(f"  Available columns in {name1}: {list(df1.columns)[:10]}")
        logger.info(f"  Available columns in {name2}: {list(df2.columns)[:10]}")
        return results

    # Create comparison keys
    df1["comparison_key"] = df1[key_col_df1].apply(
        standardize_product_code if code_col_df1 else standardize_product_name
    )
    df2["comparison_key"] = df2[key_col_df2].apply(
        standardize_product_code if code_col_df2 else standardize_product_name
    )

    # Find products only in df1
    keys_df1 = set(df1["comparison_key"].dropna())
    keys_df2 = set(df2["comparison_key"].dropna())
    results["only_in_df1"] = list(keys_df1 - keys_df2)
    results["only_in_df2"] = list(keys_df2 - keys_df1)
    results["common_products"] = list(keys_df1 & keys_df2)

    # Calculate total quantities
    if qty_col_df1:
        results["total_qty_df1"] = pd.to_numeric(df1[qty_col_df1], errors="coerce").fillna(0).sum()

    if qty_col_df2:
        results["total_qty_df2"] = pd.to_numeric(df2[qty_col_df2], errors="coerce").fillna(0).sum()

    # Compare quantities for common products
    if qty_col_df1 and qty_col_df2 and results["common_products"]:
        df1_qty = df1.set_index("comparison_key")[qty_col_df1]
        df2_qty = df2.set_index("comparison_key")[qty_col_df2]

        common_keys = results["common_products"]
        for key in common_keys:
            try:
                qty1 = pd.to_numeric(df1_qty.get(key, 0), errors="coerce")
                qty2 = pd.to_numeric(df2_qty.get(key, 0), errors="coerce")

                if pd.notna(qty1) and pd.notna(qty2) and qty1 != qty2:
                    results["quantity_diffs"].append(
                        {"key": key, name1: qty1, name2: qty2, "diff": qty1 - qty2}
                    )
            except Exception as e:
                logger.debug(f"Error comparing {key}: {e}")

    return results


def print_comparison_report(results: Dict, name1: str, name2: str) -> None:
    """Print comparison report to console."""
    print("")
    print("=" * 70)
    print(f"RECONCILIATION REPORT: {name1} vs {name2}")
    print("=" * 70)
    print("")
    print(f"Row counts:")
    print(f"  {name1}: {results['total_rows_df1']}")
    print(f"  {name2}: {results['total_rows_df2']}")
    print(f"  Difference: {results['row_count_diff']:+d}")
    print("")
    print(f"Total quantities:")
    print(f"  {name1}: {results['total_qty_df1']:.0f}")
    print(f"  {name2}: {results['total_qty_df2']:.0f}")
    print(f"  Difference: {results['total_qty_df1'] - results['total_qty_df2']:+.0f}")
    print("")
    print(f"Products:")
    print(f"  Common: {len(results['common_products'])}")
    print(f"  Only in {name1}: {len(results['only_in_df1'])}")
    print(f"  Only in {name2}: {len(results['only_in_df2'])}")
    print("")

    if results["only_in_df1"]:
        print(f"Products ONLY in {name1}:")
        for key in results["only_in_df1"][:20]:
            print(f"  - {key}")
        if len(results["only_in_df1"]) > 20:
            print(f"  ... and {len(results['only_in_df1']) - 20} more")
        print("")

    if results["only_in_df2"]:
        print(f"Products ONLY in {name2}:")
        for key in results["only_in_df2"][:20]:
            print(f"  - {key}")
        if len(results["only_in_df2"]) > 20:
            print(f"  ... and {len(results['only_in_df2']) - 20} more")
        print("")

    if results["quantity_diffs"]:
        print(f"Quantity differences:")
        total_diff = sum(abs(d["diff"]) for d in results["quantity_diffs"])
        print(f"  Total discrepancy: {total_diff:+.0f} items")
        print(f"  Count of affected products: {len(results['quantity_diffs'])}")
        print("")
        print(f"  Top differences:")
        sorted_diffs = sorted(results["quantity_diffs"], key=lambda x: abs(x["diff"]), reverse=True)
        for diff in sorted_diffs[:20]:
            print(f"    {diff['key']}: {name1}={diff[name1]:.0f}, {name2}={diff[name2]:.0f}, diff={diff['diff']:+.0f}")
        if len(sorted_diffs) > 20:
            print(f"    ... and {len(sorted_diffs) - 20} more")
    else:
        print("No quantity differences found.")
        print("")

    print("=" * 70)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    if len(sys.argv) != 2:
        logger.error("Usage: uv run scripts/reconcile_export_tabs.py <spreadsheet_id>")
        sys.exit(1)

    spreadsheet_id = sys.argv[1]

    logger.info("=" * 70)
    logger.info("RECONCILING EXPORT TABS")
    logger.info(f"Spreadsheet ID: {spreadsheet_id}")
    logger.info("=" * 70)

    try:
        drive_service, sheets_service = connect_to_drive()
        logger.info("Connected to Google Drive")
    except Exception as e:
        logger.error(f"Failed to connect to Google Drive: {e}")
        sys.exit(1)

    # Read both tabs
    df_chitiet = read_tab_to_dataframe(sheets_service, spreadsheet_id, "Chi tiết xuất")
    df_ctxuat = read_tab_to_dataframe(sheets_service, spreadsheet_id, "CT.XUAT")

    if df_chitiet.empty or df_ctxuat.empty:
        logger.error("Failed to read one or both tabs")
        sys.exit(1)

    # Compare
    results = compare_dataframes(df_chitiet, df_ctxuat, "Chi tiết xuất", "CT.XUAT")

    # Print report
    print_comparison_report(results, "Chi tiết xuất", "CT.XUAT")


if __name__ == "__main__":
    main()
