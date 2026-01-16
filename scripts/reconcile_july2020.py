"""Reconcile July 2020 export tabs to find -16 discrepancy root cause."""

import logging
import sys
from collections import Counter

import pandas as pd

from src.modules.google_api import connect_to_drive, read_sheet_data

logger = logging.getLogger(__name__)


def parse_chitiet_xuat(sheets_service, spreadsheet_id: str) -> pd.DataFrame:
    """Parse Chi tiết xuất tab."""
    logger.info("Reading Chi tiết xuất...")

    values = read_sheet_data(sheets_service, spreadsheet_id, "Chi tiết xuất")
    if not values or len(values) < 2:
        return pd.DataFrame()

    headers = values[0]
    data = values[1:]

    df = pd.DataFrame(data, columns=headers)

    # Standardize product code and name
    df["product_key"] = (
        df["Mã hàng"].astype(str).str.strip().str.upper()
        + "|"
        + df["Tên hàng"].astype(str).str.strip().str.upper()
    )

    # Sum quantities by product
    df_grouped = df.groupby("product_key").agg(
        {"Số lượng": "sum", "Thành tiền": "sum"}
    ).reset_index()

    logger.info(f"  Loaded {len(df)} rows, {len(df_grouped)} unique products")
    logger.info(f"  Total quantity: {df['Số lượng'].sum():.0f}")

    return df_grouped


def parse_ctxuat(sheets_service, spreadsheet_id: str) -> pd.DataFrame:
    """Parse CT.XUAT tab - complex structure."""
    logger.info("Reading CT.XUAT...")

    values = read_sheet_data(sheets_service, spreadsheet_id, "CT.XUAT")
    if not values or len(values) < 5:
        return pd.DataFrame()

    # Skip first 4 header rows
    data = values[4:]

    # Filter data rows (rows with actual content)
    data_rows = []
    for row in data:
        # Must have at least 5 columns
        if len(row) < 5:
            continue
        # First column must be document ID (like PX001, PX002)
        if not row[0] or not isinstance(row[0], str):
            continue
        # Skip header row (PBH = Phiếu bán hàng header)
        if row[0] == "PBH":
            continue
        # Column 5 is MÃ SỐ (product code), Column 6 is Chủng loại (product name)
        if len(row) < 8:
            continue
        if not row[5] and not row[6]:
            continue

        product_code = str(row[5]).strip() if row[5] else ""
        product_name = str(row[6]).strip() if row[6] else ""

        # Column 8 is Số lượng
        quantity = 0
        if len(row) > 8 and row[8]:
            try:
                quantity = float(str(row[8]).replace(",", "").replace(" ", ""))
            except:
                pass

        if product_code or product_name:
            data_rows.append(
                {
                    "product_key": product_code.upper() + "|" + product_name.upper(),
                    "product_code": product_code,
                    "product_name": product_name,
                    "Số lượng": quantity,
                    "document_id": row[0],
                }
            )

    if not data_rows:
        return pd.DataFrame()

    df = pd.DataFrame(data_rows)

    # Sum quantities by product
    df_grouped = df.groupby("product_key").agg(
        {"Số lượng": "sum", "product_code": "first", "product_name": "first"}
    ).reset_index()

    logger.info(f"  Loaded {len(data_rows)} raw rows, {len(df_grouped)} unique products")
    logger.info(f"  Total quantity: {df['Số lượng'].sum():.0f}")

    return df_grouped


def find_discrepancies(df1: pd.DataFrame, df2: pd.DataFrame, name1: str, name2: str):
    """Find quantity discrepancies between two DataFrames."""
    print("")
    print("=" * 70)
    print(f"RECONCILIATION: {name1} vs {name2}")
    print("=" * 70)
    print("")

    # Create dictionaries for easy lookup
    dict1 = {k: v for k, v in zip(df1["product_key"], df1["Số lượng"])}
    dict2 = {k: v for k, v in zip(df2["product_key"], df2["Số lượng"])}

    # All product keys
    all_keys = set(dict1.keys()) | set(dict2.keys())

    discrepancies = []
    for key in all_keys:
        qty1 = dict1.get(key, 0)
        qty2 = dict2.get(key, 0)
        diff = qty1 - qty2

        if diff != 0:
            discrepancies.append({"key": key, name1: qty1, name2: qty2, "diff": diff})

    # Sort by absolute difference
    discrepancies.sort(key=lambda x: abs(x["diff"]), reverse=True)

    # Summary
    total_diff = sum(abs(d["diff"]) for d in discrepancies)
    print(f"Total quantities:")
    print(f"  {name1}: {sum(dict1.values()):.0f}")
    print(f"  {name2}: {sum(dict2.values()):.0f}")
    print(f"  Difference: {sum(dict1.values()) - sum(dict2.values()):+.0f}")
    print("")
    print(f"Discrepancies:")
    print(f"  Affected products: {len(discrepancies)}")
    print(f"  Total discrepancy: {total_diff:+.0f}")
    print("")

    if discrepancies:
        print(f"Top discrepancies (by absolute value):")
        for d in discrepancies[:30]:
            key = d["key"]
            parts = key.split("|")
            code = parts[0] if parts else ""
            name = parts[1] if len(parts) > 1 else ""

            print(f"  {name if name else code}")
            print(f"    {name1}: {d[name1]:.0f}, {name2}: {d[name2]:.0f}, diff: {d['diff']:+.0f}")

        # Check if total matches expected -16
        sum_diffs = sum(d["diff"] for d in discrepancies)
        print("")
        print(f"Sum of all differences: {sum_diffs:+.0f}")
        print(f"Expected discrepancy: -16")
        print(f"Match: {abs(sum_diffs + 16) < 0.1}")

    print("=" * 70)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
    )

    spreadsheet_id = "1QJReHhfPo0EkTEwjHZFH3vEQXJj127nve2hf3Msm_Ko"

    logger.info("=" * 70)
    logger.info("RECONCILING JULY 2020 EXPORT DATA")
    logger.info(f"Spreadsheet ID: {spreadsheet_id}")
    logger.info("=" * 70)

    try:
        drive_service, sheets_service = connect_to_drive()
        logger.info("Connected to Google Drive")
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        sys.exit(1)

    df_chitiet = parse_chitiet_xuat(sheets_service, spreadsheet_id)
    df_ctxuat = parse_ctxuat(sheets_service, spreadsheet_id)

    if df_chitiet.empty or df_ctxuat.empty:
        logger.error("Failed to read data")
        sys.exit(1)

    find_discrepancies(df_chitiet, df_ctxuat, "Chi tiết xuất", "CT.XUAT")


if __name__ == "__main__":
    main()
