# -*- coding: utf-8 -*-
"""Generate synthetic purchase receipt records for opening balances.

This module extracts opening balances from earliest inventory month and generates
synthetic purchase records dated last day of previous month to ensure FIFO
costing works correctly.

Raw source: Xuất nhập tồn CSV (from clean_inventory.py)
Output: Chi tiết nhập CSV with opening balance records prepended
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd
import tomllib


def load_pipeline_config() -> Dict[str, Any]:
    config_path = Path("pipeline.toml")
    if not config_path.exists():
        return {
            "dirs": {
                "raw_data": "data/00-raw",
                "staging": "data/01-staging",
            }
        }

    with open(config_path, "rb") as f:
        return tomllib.load(f)


def load_opening_balance_config() -> Dict[str, Any]:
    config = load_pipeline_config()

    defaults = {
        "receipt_date": "2020-03-31",
        "receipt_code_prefix": "PN-OB",
        "supplier_name": "Kho đầu kỳ",
        "quantity_price_tolerance_pct": 5.0,
    }

    opening_balance_config = config.get("opening_balance", {})
    return {**defaults, **opening_balance_config}


_PIPELINE_CONFIG = load_pipeline_config()
_OB_CONFIG = load_opening_balance_config()
DATA_STAGING_DIR = Path(_PIPELINE_CONFIG["dirs"]["staging"]) / "import_export"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def detect_earliest_month(df: pd.DataFrame) -> Tuple[int, int]:
    if "Năm" not in df.columns or "Tháng" not in df.columns:
        raise ValueError("Inventory data missing Năm or Tháng column")

    df_valid = df.dropna(subset=["Năm", "Tháng"]).copy()
    if df_valid.empty:
        raise ValueError("No valid year/month data in inventory")

    df_valid["_sort_key"] = df_valid["Năm"] * 100 + df_valid["Tháng"]
    earliest_idx = df_valid["_sort_key"].idxmin()

    year = int(df_valid.loc[earliest_idx, "Năm"])
    month = int(df_valid.loc[earliest_idx, "Tháng"])

    logger.info(f"Detected earliest month: {year:04d}-{month:02d}")
    return (year, month)


def extract_opening_balances(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    df_filtered = df[(df["Năm"] == year) & (df["Tháng"] == month)].copy()

    if df_filtered.empty:
        logger.warning(f"No inventory data found for {year:04d}-{month:02d}")
        return pd.DataFrame()

    df_filtered = df_filtered[df_filtered["Số lượng đầu kỳ"] > 0].copy()

    for col in ["Số lượng đầu kỳ", "Đơn giá đầu kỳ", "Thành tiền đầu kỳ"]:
        if col in df_filtered.columns:
            df_filtered[col] = pd.to_numeric(df_filtered[col], errors="coerce")

    required_cols = ["Mã hàng", "Số lượng đầu kỳ", "Đơn giá đầu kỳ"]
    df_filtered = df_filtered.dropna(subset=required_cols)

    logger.info(f"Extracted {len(df_filtered)} opening balance records")
    return df_filtered[
        [
            "Mã hàng",
            "Tên hàng",
            "Số lượng đầu kỳ",
            "Đơn giá đầu kỳ",
            "Thành tiền đầu kỳ",
        ]
    ]


def validate_opening_balances(
    df: pd.DataFrame, tolerance_pct: float = 5.0
) -> pd.DataFrame:
    df_valid = df.copy()

    df_valid["_expected_total"] = (
        df_valid["Số lượng đầu kỳ"] * df_valid["Đơn giá đầu kỳ"]
    )
    df_valid["_actual_total"] = df_valid["Thành tiền đầu kỳ"]

    df_valid["_deviation_pct"] = (
        (df_valid["_actual_total"] - df_valid["_expected_total"]).abs()
        / df_valid["_expected_total"].replace(0, 1)
        * 100
    )

    df_valid = df_valid[df_valid["_deviation_pct"] <= tolerance_pct].copy()

    invalid_count = len(df) - len(df_valid)
    if invalid_count > 0:
        logger.warning(
            f"Rejected {invalid_count} records with > {tolerance_pct}% price/quantity deviation"
        )

    df_valid = df_valid.drop(
        columns=["_expected_total", "_actual_total", "_deviation_pct"]
    )

    logger.info(f"Validated {len(df_valid)}/{len(df)} opening balance records")
    return df_valid


def generate_synthetic_receipts(
    opening_balances: pd.DataFrame,
    receipt_date: str,
    receipt_code_prefix: str,
    supplier_name: str,
) -> pd.DataFrame:
    synthetic_records = []

    for idx, row in opening_balances.iterrows():
        receipt_code = f"{receipt_code_prefix}-{receipt_date}-{idx:04d}"

        synthetic_records.append(
            {
                "Mã hàng": row["Mã hàng"],
                "Tên hàng": row["Tên hàng"] if pd.notna(row["Tên hàng"]) else "",
                "Số lượng": row["Số lượng đầu kỳ"],
                "Đơn giá": row["Đơn giá đầu kỳ"],
                "Thành tiền": row["Thành tiền đầu kỳ"],
                "Ngày": receipt_date,
                "Năm": int(receipt_date[:4]),
                "Tháng": int(receipt_date[5:7]),
                "Mã chứng từ": receipt_code,
                "Tên nhà cung cấp": supplier_name,
            }
        )

    synthetic_df = pd.DataFrame(synthetic_records)

    text_cols = ["Mã hàng", "Tên hàng", "Mã chứng từ", "Tên nhà cung cấp"]
    for col in text_cols:
        if col in synthetic_df.columns:
            synthetic_df[col] = synthetic_df[col].astype(str)
            if col == "Mã hàng":
                synthetic_df[col] = synthetic_df[col].str.upper()

    numeric_cols = ["Số lượng", "Đơn giá", "Thành tiền"]
    for col in numeric_cols:
        if col in synthetic_df.columns:
            synthetic_df[col] = pd.to_numeric(synthetic_df[col], errors="coerce")

    logger.info(f"Generated {len(synthetic_df)} synthetic purchase records")
    return synthetic_df


def detect_existing_synthetic_records(
    df: pd.DataFrame, receipt_code_prefix: str
) -> bool:
    if "Mã chứng từ" not in df.columns:
        return False

    synthetic_mask = df["Mã chứng từ"].str.startswith(receipt_code_prefix, na=False)
    count = synthetic_mask.sum()

    if count > 0:
        logger.info(f"Found {count} existing synthetic records, skipping generation")
        return True

    return False


def append_to_purchases(
    purchases_df: pd.DataFrame, synthetic_df: pd.DataFrame, receipt_date: str
) -> pd.DataFrame:
    combined_df = pd.concat([synthetic_df, purchases_df], ignore_index=True)

    if "Ngày" in combined_df.columns:
        combined_df["_sort_date"] = pd.to_datetime(combined_df["Ngày"], errors="coerce")
        combined_df = combined_df.sort_values(
            by=["_sort_date", "Mã chứng từ"], na_position="last"
        )
        combined_df = combined_df.drop(columns=["_sort_date"])

    logger.info(
        f"Combined: {len(synthetic_df)} synthetic + {len(purchases_df)} existing = {len(combined_df)} total"
    )

    return combined_df


def create_reconciliation_checkpoint(
    inventory_df: pd.DataFrame,
    synthetic_df: pd.DataFrame,
    output_filepath: Path,
    script_name: str = "generate_opening_balance_receipts",
) -> Dict[str, Any]:
    inventory_qty = (
        inventory_df["Số lượng đầu kỳ"].sum() if not inventory_df.empty else 0
    )
    synthetic_qty = synthetic_df["Số lượng"].sum() if not synthetic_df.empty else 0
    inventory_value = (
        inventory_df["Thành tiền đầu kỳ"].sum() if not inventory_df.empty else 0
    )
    synthetic_value = synthetic_df["Thành tiền"].sum() if not synthetic_df.empty else 0

    report = {
        "timestamp": datetime.now().isoformat(),
        "script": script_name,
        "inventory": {
            "records": len(inventory_df),
            "total_quantity": float(inventory_qty),
            "total_value": float(inventory_value),
        },
        "synthetic_records": {
            "records": len(synthetic_df),
            "total_quantity": float(synthetic_qty),
            "total_value": float(synthetic_value),
        },
        "validation": {
            "quantity_match": bool(abs(inventory_qty - synthetic_qty) < 0.01),
            "value_match": bool(abs(inventory_value - synthetic_value) < 1.0),
        },
        "alerts": [],
    }

    if not report["validation"]["quantity_match"]:
        report["alerts"].append(
            f"Quantity mismatch: inventory={inventory_qty:,.0f} vs synthetic={synthetic_qty:,.0f}"
        )
    if not report["validation"]["value_match"]:
        report["alerts"].append(
            f"Value mismatch: inventory={inventory_value:,.0f} vs synthetic={synthetic_value:,.0f}"
        )

    report_filename = f"reconciliation_report_{script_name}.json"
    report_path = output_filepath.parent / report_filename
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    logger.info("=" * 70)
    logger.info(f"RECONCILIATION REPORT ({script_name})")
    logger.info(f"Inventory records: {len(inventory_df)}")
    logger.info(f"Synthetic records: {len(synthetic_df)}")
    logger.info(f"Inventory quantity: {inventory_qty:,.0f}")
    logger.info(f"Synthetic quantity: {synthetic_qty:,.0f}")
    logger.info(f"Report saved to: {report_filename}")
    for alert in report["alerts"]:
        logger.warning(alert)
    logger.info("=" * 70)

    return report


def process(
    inventory_path: Optional[Path] = None,
    purchase_path: Optional[Path] = None,
    output_path: Optional[Path] = None,
) -> Optional[Path]:
    """Generate synthetic purchase records for opening balances.

    Args:
        inventory_path: Path to Xuất nhập tồn CSV (from clean_inventory.py)
        purchase_path: Path to Chi tiết nhập CSV (from clean_receipts_purchase.py)
        output_path: Output path (default: overwrite purchase_path)

    Returns:
        Path to augmented purchase receipts file or None if processing failed
    """
    logger.info("=" * 70)
    logger.info("GENERATING OPENING BALANCE PURCHASE RECORDS")
    logger.info("=" * 70)

    if inventory_path is None:
        inventory_files = list(DATA_STAGING_DIR.glob("Xuất nhập tồn *.csv"))
        inventory_files = [
            f for f in inventory_files if "adjustments" not in f.name.lower()
        ]
        if inventory_files:
            inventory_path = inventory_files[0]
            logger.info(f"Using inventory file: {inventory_path.name}")
        else:
            logger.error("No inventory file found in staging directory")
            return None

    if purchase_path is None:
        purchase_files = list(DATA_STAGING_DIR.glob("Chi tiết nhập *.csv"))
        if purchase_files:
            purchase_path = purchase_files[0]
            logger.info(f"Using purchase file: {purchase_path.name}")
        else:
            logger.error("No purchase receipts file found in staging directory")
            return None

    if output_path is None:
        output_path = purchase_path

    if not inventory_path.exists():
        logger.error(f"Inventory file not found: {inventory_path}")
        return None
    if not purchase_path.exists():
        logger.error(f"Purchase receipts file not found: {purchase_path}")
        return None

    try:
        logger.info(f"Reading inventory data from: {inventory_path}")
        inventory_df = pd.read_csv(inventory_path, encoding="utf-8")

        logger.info(f"Reading purchase receipts from: {purchase_path}")
        purchases_df = pd.read_csv(purchase_path, encoding="utf-8")

        receipt_code_prefix = _OB_CONFIG["receipt_code_prefix"]
        if detect_existing_synthetic_records(purchases_df, receipt_code_prefix):
            logger.info("Opening balance records already exist, skipping generation")
            return purchase_path

        year, month = detect_earliest_month(inventory_df)

        opening_balances = extract_opening_balances(inventory_df, year, month)

        if opening_balances.empty:
            logger.info("No opening balances found, nothing to generate")
            return purchase_path

        tolerance_pct = _OB_CONFIG["quantity_price_tolerance_pct"]
        opening_balances = validate_opening_balances(opening_balances, tolerance_pct)

        if opening_balances.empty:
            logger.warning("No valid opening balances after validation")
            return purchase_path

        receipt_date = _OB_CONFIG["receipt_date"]
        supplier_name = _OB_CONFIG["supplier_name"]
        synthetic_df = generate_synthetic_receipts(
            opening_balances, receipt_date, receipt_code_prefix, supplier_name
        )

        combined_df = append_to_purchases(purchases_df, synthetic_df, receipt_date)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        combined_df.to_csv(output_path, index=False, encoding="utf-8")
        logger.info(f"Saved augmented purchase receipts to: {output_path}")

        create_reconciliation_checkpoint(
            opening_balances,
            synthetic_df,
            output_path,
            "generate_opening_balance_receipts",
        )

        logger.info("=" * 70)
        logger.info("OPENING BALANCE GENERATION COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)

        return output_path

    except Exception as e:
        logger.error(f"Opening balance generation failed: {str(e)}", exc_info=True)
        return None


if __name__ == "__main__":
    process()
