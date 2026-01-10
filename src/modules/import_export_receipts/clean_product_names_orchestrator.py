# -*- coding: utf-8 -*-
"""Clean product names and extract complete product information.

This module orchestrates the full pipeline:
1. Clean product names (normalize spaces, standardize dimensions, fix typos)
2. Classify products into hierarchical categories (parent>>child)
3. Extract extended attributes with position detection
4. Generate human-readable descriptions in Vietnamese

Phase 1 Implementation:
- Issue 1: Spaces around special characters (-, /, ., *, etc.)
- Issue 4: Dimension format (W/H/D → W/H-D, drop leading letter)

Phase 2 Implementation:
- Product classification (Nhóm hàng cha, Nhóm hàng con)
- Attribute extraction (Thuộc tính, Mô tả columns)
- Position detection (Vỏ trước, Vỏ sau)

Module: import_export_receipts
"""

import logging

import pandas as pd

from src.modules.import_export_receipts.clean_product_names_core import (
    clean_product_name,
    extract_product_type_attributes,
)
from src.modules.import_export_receipts.classify_products import (
    classify_product,
)
from src.utils.product_attributes import (
    extract_attributes_extended,
)
from src.modules.import_export_receipts.product_disambiguation import (
    extract_brand_from_name,
)

logger = logging.getLogger(__name__)


# ============================================================================
# UNIFIED EXTRACTION FUNCTION
# ============================================================================


def clean_and_extract_complete(name: str) -> dict:
    """Apply full cleaning, classification, and attribute extraction.

    Orchestrates the complete pipeline in correct order:
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
            if v is not None
            and k
            not in [
                "tire_type",
                "has_pattern",
            ]
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


# ============================================================================
# BATCH PROCESSING FUNCTIONS
# ============================================================================


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


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================


def validate_extraction(result: dict, original_name: str) -> dict:
    """Validate extraction quality and compare with original.

    Checks:
    - All required keys present
    - Classification is valid
    - Attributes extracted correctly
    - Description generated for extracted attributes

    Args:
        result: Extraction dict from clean_and_extract_complete()
        original_name: Original product name

    Returns:
        Dict with validation metrics
    """
    metrics = {
        "valid": True,
        "cleaned_different": False,
        "brand_extracted": False,
        "classification_extracted": False,
        "attributes_extracted": False,
        "description_generated": False,
    }

    required_keys = [
        "Tên hàng cleaned",
        "Thương hiệu",
        "Nhóm hàng cha",
        "Nhóm hàng con",
        "Nhóm hàng(2 Cấp)",
        "Vị trí",
        "Thuộc tính",
        "Mô tả",
    ]

    if not all(key in result for key in required_keys):
        metrics["valid"] = False
        return metrics

    if result["Tên hàng cleaned"] != original_name:
        metrics["cleaned_different"] = True

    if result["Thương hiệu"]:
        metrics["brand_extracted"] = True

    if result["Nhóm hàng(2 Cấp)"]:
        metrics["classification_extracted"] = True

    if result["Thuộc tính"]:
        metrics["attributes_extracted"] = True

    if result["Mô tả"]:
        metrics["description_generated"] = True

    return metrics


def validate_extraction_series(
    results_df: pd.DataFrame, original_series: pd.Series
) -> dict:
    """Validate extraction quality for a series of products.

    Args:
        results_df: DataFrame with extraction results
        original_series: Series with original product names

    Returns:
        Dict with validation metrics
    """
    metrics = {
        "total": len(results_df),
        "cleaned_count": 0,
        "brand_extracted_count": 0,
        "classification_extracted_count": 0,
        "attributes_extracted_count": 0,
        "description_generated_count": 0,
        "invalid_count": 0,
    }

    for idx, row in results_df.iterrows():
        original_name = str(original_series.iloc[idx])
        validation = validate_extraction(row.to_dict(), original_name)

        if not validation["valid"]:
            metrics["invalid_count"] += 1
            continue

        if validation["cleaned_different"]:
            metrics["cleaned_count"] += 1
        if validation["brand_extracted"]:
            metrics["brand_extracted_count"] += 1
        if validation["classification_extracted"]:
            metrics["classification_extracted_count"] += 1
        if validation["attributes_extracted"]:
            metrics["attributes_extracted_count"] += 1
        if validation["description_generated"]:
            metrics["description_generated_count"] += 1

    logger.info(
        f"validate_extraction_series: {metrics['total']} products, "
        f"{metrics['brand_extracted_count']} brands, "
        f"{metrics['classification_extracted_count']} classifications, "
        f"{metrics['attributes_extracted_count']} attributes, "
        f"{metrics['description_generated_count']} descriptions, "
        f"{metrics['invalid_count']} invalid"
    )

    return metrics
