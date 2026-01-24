# -*- coding: utf-8 -*-
"""Generate master product CSV with aggregated metrics.

Creates a standalone master product reference file with:
- Unique Mã hàng from all 3 sources (nhap, xuat, inventory)
- Cleaned Tên hàng
- Doanh thu 2025 (sum of Thành tiền from Chi tiết xuất 2025)
- Giá trị cuối kỳ 2025 (from Xuất nhập tồn Dec 2025)

Used by: orchestrator.py (via -t -m ier)
"""

import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

from src.modules.import_export_receipts.product_cleaning import (
    clean_product_name,
    standardize_product_type,
)
from src.utils.staging_cache import StagingCache

logger = logging.getLogger(__name__)


def find_latest_file(directory: Path, pattern: str) -> Optional[Path]:
    """Find the latest file matching a pattern in a directory."""
    matching_files = list(directory.glob(pattern))
    if not matching_files:
        return None
    matching_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return matching_files[0]


def load_source_files(staging_dir: Path) -> dict:
    """Load Chi tiết nhập, Chi tiết xuất, Xuất nhập tồn."""
    nhap_file = find_latest_file(staging_dir, "*Chi tiết nhập*.csv")
    xuat_file = find_latest_file(staging_dir, "*Chi tiết xuất*.csv")
    xnt_file = find_latest_file(staging_dir, "Xuất nhập tồn*.csv")

    if not nhap_file or not xuat_file or not xnt_file:
        logger.error("Missing source files in staging directory")
        return {}

    files = {}
    logger.info(f"Loading: {nhap_file.name}")
    files["nhap"] = StagingCache.get_dataframe(nhap_file)

    logger.info(f"Loading: {xuat_file.name}")
    files["xuat"] = StagingCache.get_dataframe(xuat_file)

    logger.info(f"Loading: {xnt_file.name}")
    files["xnt"] = StagingCache.get_dataframe(xnt_file)

    return files


def extract_unique_products(files: dict) -> pd.DataFrame:
    """Extract unique (Mã hàng, Tên hàng) from all 3 sources."""
    product_refs = []

    for source_name, df in files.items():
        if "Mã hàng" in df.columns and "Tên hàng" in df.columns:
            temp = df[["Mã hàng", "Tên hàng"]].copy()
            temp["_sources"] = source_name
            product_refs.append(temp.drop_duplicates())

    if not product_refs:
        return pd.DataFrame(columns=["Mã hàng", "Tên hàng", "_sources"])

    master = pd.concat(product_refs, ignore_index=True)

    master["Mã hàng"] = master["Mã hàng"].astype(str).str.strip()
    master["Tên hàng"] = master["Tên hàng"].astype(str).str.strip()

    master = master.drop_duplicates(subset=["Mã hàng"], keep="first")

    return master[["Mã hàng", "Tên hàng", "_sources"]]


def calculate_2025_revenue(xuat_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate Doanh thu 2025 per product."""
    if "Năm" not in xuat_df.columns or "Thành tiền" not in xuat_df.columns:
        return pd.DataFrame(columns=["Mã hàng", "Doanh thu 2025"])

    xuat_2025 = xuat_df[xuat_df["Năm"] == 2025].copy()

    if xuat_2025.empty:
        return pd.DataFrame(columns=["Mã hàng", "Doanh thu 2025"])

    xuat_2025["Thành tiền"] = pd.to_numeric(
        xuat_2025["Thành tiền"], errors="coerce"
    ).fillna(0)

    revenue = (
        xuat_2025.groupby("Mã hàng", dropna=False)["Thành tiền"].sum().reset_index()
    )
    revenue.columns = ["Mã hàng", "Doanh thu 2025"]

    return revenue


def get_december_2025_inventory_value(xnt_df: pd.DataFrame) -> pd.DataFrame:
    """Get Giá trị cuối kỳ 2025 from December row."""
    if "Năm" not in xnt_df.columns or "Tháng" not in xnt_df.columns:
        return pd.DataFrame(columns=["Mã hàng", "Giá trị cuối kỳ 2025"])

    dec_2025 = xnt_df[(xnt_df["Năm"] == 2025) & (xnt_df["Tháng"] == 12)].copy()

    if dec_2025.empty:
        return pd.DataFrame(columns=["Mã hàng", "Giá trị cuối kỳ 2025"])

    if "Giá trị cuối kỳ" in dec_2025.columns:
        dec_2025["Giá trị cuối kỳ"] = pd.to_numeric(
            dec_2025["Giá trị cuối kỳ"], errors="coerce"
        ).fillna(0)
        result = dec_2025[["Mã hàng", "Giá trị cuối kỳ"]].copy()
        result.columns = ["Mã hàng", "Giá trị cuối kỳ 2025"]
        return result

    return pd.DataFrame(columns=["Mã hàng", "Giá trị cuối kỳ 2025"])


def clean_product_name_simple(name: str) -> str:
    """Clean a single product name."""
    if pd.isna(name):
        return ""
    return standardize_product_type(clean_product_name(str(name)))


def process(staging_dir: Optional[Path] = None) -> bool:
    """Generate master product CSV.

    Returns:
        True if successful, False otherwise
    """
    logger.info("=" * 70)
    logger.info("GENERATING MASTER PRODUCT CSV")
    logger.info("=" * 70)

    if staging_dir is None:
        staging_dir = Path("data/01-staging/import_export")

    if not staging_dir.exists():
        logger.error(f"Staging directory not found: {staging_dir}")
        return False

    try:
        files = load_source_files(staging_dir)
        if not files:
            logger.warning("No source files found")
            return True

        logger.info(f"Loaded {len(files)} source files")

        master = extract_unique_products(files)
        logger.info(f"Extracted {len(master)} unique products")

        if master.empty:
            logger.warning("No products found")
            return True

        revenue = calculate_2025_revenue(files["xuat"])
        logger.info(f"Calculated 2025 revenue for {len(revenue)} products")

        inventory_value = get_december_2025_inventory_value(files["xnt"])
        logger.info(f"Got 2025 inventory values for {len(inventory_value)} products")

        master = master.merge(revenue, on="Mã hàng", how="left")
        master = master.merge(inventory_value, on="Mã hàng", how="left")

        master["Doanh thu 2025"] = master["Doanh thu 2025"].fillna(0)
        master["Giá trị cuối kỳ 2025"] = master["Giá trị cuối kỳ 2025"].fillna(0)

        master["Tên hàng"] = master["Tên hàng"].apply(clean_product_name_simple)

        master = master.sort_values(
            by=["Doanh thu 2025", "Giá trị cuối kỳ 2025"], ascending=[False, False]
        )

        output_dir = Path("data/01-staging/master_products")
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / "master_products.csv"
        master.to_csv(output_path, index=False, encoding="utf-8")
        logger.info(f"Saved: {output_path}")

        logger.info("=" * 70)
        logger.info("MASTER PRODUCT CSV GENERATED")
        logger.info(f"  Total products: {len(master)}")
        logger.info(f"  Total 2025 revenue: {master['Doanh thu 2025'].sum():,.0f}")
        logger.info(
            f"  Total inventory value: {master['Giá trị cuối kỳ 2025'].sum():,.0f}"
        )
        logger.info("=" * 70)

        return True

    except Exception as e:
        logger.error(f"Error generating master product CSV: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate master product CSV with aggregated metrics"
    )

    args = parser.parse_args()

    success = process()
    sys.exit(0 if success else 1)
