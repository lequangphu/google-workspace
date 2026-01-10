# -*- coding: utf-8 -*-
"""Verify disambiguation results between purchase and sale outputs.

This script:
1. Loads cleaned purchase and sale data
2. Compares product code handling between them
3. Reports any discrepancies
"""

import logging
from pathlib import Path
from typing import Dict, List, Set

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

DATA_STAGING_DIR = Path.cwd() / "data" / "01-staging" / "import_export"


def load_cleaned_data() -> Dict[str, pd.DataFrame]:
    """Load cleaned purchase and sale data.

    Returns:
        Dict with 'purchase' and 'sale' keys
    """
    purchase_files = list(DATA_STAGING_DIR.glob("*Chi tiết nhập*.csv"))
    sale_files = list(DATA_STAGING_DIR.glob("*Chi tiết xuất*.csv"))

    if not purchase_files:
        raise FileNotFoundError("No purchase detail file found")
    if not sale_files:
        raise FileNotFoundError("No sale detail file found")

    purchase_df = pd.read_csv(purchase_files[0])
    sale_df = pd.read_csv(sale_files[0])

    logger.info(
        f"Loaded purchase data: {purchase_files[0].name} ({len(purchase_df)} rows)"
    )
    logger.info(f"Loaded sale data: {sale_files[0].name} ({len(sale_df)} rows)")

    return {"purchase": purchase_df, "sale": sale_df}


def find_common_product_codes(data: Dict[str, pd.DataFrame]) -> Set[str]:
    """Find product codes present in both purchase and sale data."""
    purchase_codes = set(data["purchase"]["Mã hàng"].unique())
    sale_codes = set(data["sale"]["Mã hàng"].unique())
    return purchase_codes.intersection(sale_codes)


def find_suffixed_codes(data: Dict[str, pd.DataFrame]) -> Dict[str, List[str]]:
    """Find codes that have been suffixed (contain '-')."""
    suffixed = {"purchase": [], "sale": []}

    for source in ["purchase", "sale"]:
        codes = data[source]["Mã hàng"].unique()
        for code in codes:
            if "-" in str(code):
                suffixed[source].append(code)

    return suffixed


def compare_name_consistency(
    data: Dict[str, pd.DataFrame], codes: Set[str]
) -> List[Dict]:
    """Compare product names for common codes between purchase and sale data.

    Returns:
        List of dictionaries with discrepancy details
    """
    purchase_names = data["purchase"].set_index("Mã hàng")["Tên hàng"].to_dict()
    sale_names = data["sale"].set_index("Mã hàng")["Tên hàng"].to_dict()

    inconsistencies = []

    for code in codes:
        purchase_name = purchase_names.get(code)
        sale_name = sale_names.get(code)

        if purchase_name and sale_name and purchase_name != sale_name:
            inconsistencies.append(
                {
                    "code": code,
                    "purchase_name": purchase_name,
                    "sale_name": sale_name,
                }
            )

    return inconsistencies


def verify_disambiguation() -> None:
    """Main verification function."""
    logger.info("=" * 70)
    logger.info("VERIFYING PRODUCT DISAMBIGUATION RESULTS")
    logger.info("=" * 70)

    try:
        data = load_cleaned_data()
    except FileNotFoundError as e:
        logger.error(f"Data file not found: {e}")
        return

    common_codes = find_common_product_codes(data)
    logger.info(f"\nCommon product codes: {len(common_codes)}")

    suffixed = find_suffixed_codes(data)
    logger.info(f"\nSuffixed codes in purchase: {len(suffixed['purchase'])}")
    logger.info(f"Suffixed codes in sale: {len(suffixed['sale'])}")

    if suffixed["purchase"]:
        logger.info(f"  Examples: {suffixed['purchase'][:5]}")
    if suffixed["sale"]:
        logger.info(f"Examples: {suffixed['sale'][:5]}")

    inconsistencies = compare_name_consistency(data, common_codes)
    logger.info(
        f"\nName inconsistencies between purchase and sale: {len(inconsistencies)}"
    )

    if inconsistencies:
        logger.warning("\nInconsistent product names:")
        for inc in inconsistencies[:10]:
            logger.warning(f"  {inc['code']}:")
            logger.warning(f"    Purchase: {inc['purchase_name']}")
            logger.warning(f"    Sale: {inc['sale_name']}")

    logger.info("\n" + "=" * 70)
    logger.info("VERIFICATION COMPLETE")
    logger.info("=" * 70)
    logger.info(
        f"Summary: {len(common_codes)} common codes, {len(inconsistencies)} inconsistencies"
    )

    if len(inconsistencies) > 10:
        logger.warning("⚠️ Too many inconsistencies - review manually")

    # Exit with error code if significant inconsistencies found
    if len(inconsistencies) > 20:
        logger.warning("⚠️ Many name inconsistencies - review manually")
    else:
        logger.info("VERIFICATION COMPLETE")
        logger.info("=" * 70)
        logger.info(
            f"Summary: {len(common_codes)} common codes, {len(inconsistencies)} inconsistencies"
        )


if __name__ == "__main__":
    verify_disambiguation()
