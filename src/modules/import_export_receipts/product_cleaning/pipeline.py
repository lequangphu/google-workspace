# -*- coding: utf-8 -*-
"""Product cleaning pipeline - orchestrates the full cleaning flow.

This module provides the main entry points for product processing:
1. Clean product name (normalize spaces, standardize dimensions)
2. Extract brand from name
3. Classify product (parent>>child)
4. Extract extended attributes
5. Generate Vietnamese description

All actual cleaning logic is in core.py.
"""

import logging
import pandas as pd
from typing import Dict

from .core import (
    clean_product_name,
    classify_product,
    extract_attributes_extended,
    extract_brand_from_name,
    extract_product_type_attributes,
)

logger = logging.getLogger(__name__)


# ============================================================================
# UNIFIED PIPELINE FUNCTIONS
# ============================================================================


def clean_and_extract_complete(name: str) -> Dict[str, any]:
    """Apply full cleaning, classification, and attribute extraction.

    Orchestrates complete pipeline in correct order:
    1. Clean product name (normalize, standardize)
    2. Extract brand (from cleaned name)
    3. Classify product (parent>>child)
    4. Extract attributes (Thuộc tính, Mô tả)

    Args:
        name: Product name string

    Returns:
        Dict with keys:
            - "Tên hàng cleaned": Fully cleaned product name
            - "Thương hiệu": Extracted brand
            - "Nhóm hàng cha": Parent category
            - "Nhóm hàng con": Child category
            - "Nhóm hàng(2 Cấp)": Combined format parent>>child
            - "Vị trí": Position (Vỏ trước, Vỏ sau) or empty
            - "Thuộc tính": Pipe-separated attributes
            - "Mô tả": Human-readable description in Vietnamese
    """
    if not name or not isinstance(name, str):
        return {
            "Tên hàng cleaned": name,
            "Thương hiệu": "",
            "Nhóm hàng cha": "Phụ tùng khác",
            "Nhóm hàng con": "Xe khác",
            "Nhóm hàng(2 Cấp)": "Phụ tùng khác>>Xe khác",
            "Vị trí": "",
            "Thuộc tính": "",
            "Mô tả": "",
        }

    name_clean = clean_product_name(name)
    brand = extract_brand_from_name(name_clean)
    classification = classify_product(name_clean)
    attributes = extract_attributes_extended(name_clean)
    old_attributes = extract_product_type_attributes(name_clean)

    old_attributes_str = "|".join(
        [
            f"{k}:{v}"
            for k, v in old_attributes.items()
            if v is not None and k not in ["tire_type", "has_pattern"]
        ]
    )

    combined_attributes = attributes["Thuộc tính"]
    if old_attributes_str:
        if combined_attributes:
            combined_attributes += "|" + old_attributes_str
        else:
            combined_attributes = old_attributes_str

    return {
        "Tên hàng cleaned": name_clean,
        "Thương hiệu": brand,
        "Nhóm hàng cha": classification["Nhóm hàng cha"],
        "Nhóm hàng con": classification["Nhóm hàng con"],
        "Nhóm hàng(2 Cấp)": classification["Nhóm hàng(2 Cấp)"],
        "Vị trí": classification["Vị trí"],
        "Thuộc tính": combined_attributes,
        "Mô tả": attributes["Mô tả"],
    }


def clean_and_extract_series(series: pd.Series) -> pd.DataFrame:
    """Clean and extract information for a pandas Series of product names.

    Args:
        series: Pandas Series containing product names

    Returns:
        DataFrame with columns:
            - Tên hàng cleaned
            - Thương hiệu
            - Nhóm hàng cha
            - Nhóm hàng con
            - Nhóm hàng(2 Cấp)
            - Vị trí
            - Thuộc tính
            - Mô tả
    """
    if series.empty:
        logger.warning("clean_and_extract_series: empty input series")
        return pd.DataFrame()

    initial_count = len(series)

    results = [clean_and_extract_complete(str(name)) for name in series]

    df = pd.DataFrame(results)
    df.index = series.index

    logger.info(
        f"clean_and_extract_series: processed {initial_count} product names, "
        f"extracted brand for {df['Thương hiệu'].ne('').sum()}, "
        f"classified {df['Nhóm hàng(2 Cấp)'].ne('').sum()} products"
    )

    return df
