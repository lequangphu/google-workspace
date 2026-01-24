# -*- coding: utf-8 -*-
"""Test script to validate product classification and attribute extraction.

Run this script on real product data from staging to verify:
1. Classification accuracy (parent>>child format)
2. Attribute extraction (Thuộc tính, Mô tả)
3. Brand extraction
4. Position detection (Vỏ trước, Vỏ sau)

Module: import_export_receipts
"""

import logging
from pathlib import Path

import pandas as pd

from src.modules.import_export_receipts.clean_product_names_orchestrator import (
    clean_and_extract_complete,
    clean_and_extract_series,
)
from src.modules.import_export_receipts.classify_products import (
    classify_product,
    validate_classification,
)
from src.utils.product_attributes import extract_attributes_extended

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_sample_products(n: int = 20) -> pd.DataFrame:
    """Load sample product names from staging data.

    Args:
        n: Number of sample products to load (default 20)

    Returns:
        DataFrame with product names
    """
    staging_dir = Path.cwd() / "data" / "01-staging" / "import_export"

    nhap_file = staging_dir / "Chi tiết nhập 2020-04_2025-12.csv"

    if not nhap_file.exists():
        logger.error(f"Input file not found: {nhap_file}")
        return pd.DataFrame()

    df = pd.read_csv(nhap_file)

    unique_products = df[["Mã hàng", "Tên hàng"]].drop_duplicates(subset=["Mã hàng"])

    sample_products = unique_products.head(n)

    logger.info(f"Loaded {len(sample_products)} sample products from {nhap_file.name}")

    return sample_products


def test_classification_accuracy(products: pd.DataFrame) -> dict:
    """Test product classification accuracy.

    Args:
        products: DataFrame with product names

    Returns:
        Dict with classification metrics
    """
    logger.info("=" * 70)
    logger.info("TESTING PRODUCT CLASSIFICATION")
    logger.info("=" * 70)

    results = products["Tên hàng"].apply(classify_product)
    results_df = pd.DataFrame(results.tolist())

    valid_count = 0
    invalid_count = 0

    for idx, row in results_df.iterrows():
        if validate_classification(row.to_dict()):
            valid_count += 1
        else:
            invalid_count += 1

    metrics = {
        "total": len(results_df),
        "valid": valid_count,
        "invalid": invalid_count,
        "accuracy": valid_count / len(results_df) if len(results_df) > 0 else 0,
        "parent_distribution": results_df["Nhóm hàng cha"].value_counts().to_dict(),
        "child_distribution": results_df["Nhóm hàng con"].value_counts().to_dict(),
    }

    logger.info(f"Classification accuracy: {metrics['accuracy']:.2%}")
    logger.info(f"Valid classifications: {metrics['valid']}/{metrics['total']}")
    logger.info(f"Invalid classifications: {metrics['invalid']}")
    logger.info(f"Parent distribution: {metrics['parent_distribution']}")
    logger.info(f"Child distribution: {metrics['child_distribution']}")

    return metrics


def test_attribute_extraction(products: pd.DataFrame) -> dict:
    """Test attribute extraction completeness.

    Args:
        products: DataFrame with product names

    Returns:
        Dict with extraction metrics
    """
    logger.info("=" * 70)
    logger.info("TESTING ATTRIBUTE EXTRACTION")
    logger.info("=" * 70)

    results = products["Tên hàng"].apply(extract_attributes_extended)
    results_df = pd.DataFrame(results.tolist())

    thuoc_tinh_count = results_df["Thuộc tính"].ne("").sum()
    mo_ta_count = results_df["Mô tả"].ne("").sum()

    metrics = {
        "total": len(results_df),
        "attributes_extracted": thuoc_tinh_count,
        "descriptions_generated": mo_ta_count,
        "attributes_rate": thuoc_tinh_count / len(results_df)
        if len(results_df) > 0
        else 0,
        "description_rate": mo_ta_count / len(results_df) if len(results_df) > 0 else 0,
    }

    logger.info(
        f"Attributes extracted: {metrics['attributes_extracted']}/{metrics['total']}"
    )
    logger.info(
        f"Descriptions generated: {metrics['descriptions_generated']}/{metrics['total']}"
    )
    logger.info(f"Attribute extraction rate: {metrics['attributes_rate']:.2%}")
    logger.info(f"Description generation rate: {metrics['description_rate']:.2%}")

    return metrics


def test_complete_pipeline(products: pd.DataFrame) -> dict:
    """Test complete pipeline (cleaning + classification + attribute extraction).

    Args:
        products: DataFrame with product names

    Returns:
        Dict with pipeline metrics
    """
    logger.info("=" * 70)
    logger.info("TESTING COMPLETE PIPELINE")
    logger.info("=" * 70)

    extraction_results = clean_and_extract_series(products["Tên hàng"])

    metrics = {
        "total": len(extraction_results),
        "brand_extracted": extraction_results["Thương hiệu"].ne("").sum(),
        "classification_rate": extraction_results["Nhóm hàng(2 Cấp)"].ne("").sum()
        / len(extraction_results)
        if len(extraction_results) > 0
        else 0,
        "attributes_rate": extraction_results["Thuộc tính"].ne("").sum()
        / len(extraction_results)
        if len(extraction_results) > 0
        else 0,
        "description_rate": extraction_results["Mô tả"].ne("").sum()
        / len(extraction_results)
        if len(extraction_results) > 0
        else 0,
        "position_detected": extraction_results["Vị trí"].ne("").sum(),
    }

    logger.info(f"Brand extraction: {metrics['brand_extracted']}/{metrics['total']}")
    logger.info(f"Classification rate: {metrics['classification_rate']:.2%}")
    logger.info(f"Attribute extraction rate: {metrics['attributes_rate']:.2%}")
    logger.info(f"Description generation rate: {metrics['description_rate']:.2%}")
    logger.info(
        f"Position detection: {metrics['position_detected']}/{metrics['total']}"
    )

    return metrics


def display_sample_results(products: pd.DataFrame, n: int = 10) -> None:
    """Display sample results for visual inspection.

    Args:
        products: DataFrame with product names
        n: Number of samples to display (default 10)
    """
    logger.info("=" * 70)
    logger.info(f"DISPLAYING {n} SAMPLE RESULTS")
    logger.info("=" * 70)

    sample_products = products.head(n)

    for idx, row in sample_products.iterrows():
        name = row["Tên hàng"]
        result = clean_and_extract_complete(name)

        logger.info(f"\nOriginal: {name}")
        logger.info(f"Cleaned:  {result['Tên hàng cleaned']}")
        logger.info(f"Brand:     {result['Thương hiệu']}")
        logger.info(f"Category:  {result['Nhóm hàng(2 Cấp)']}")
        logger.info(f"Position:  {result['Vị trí']}")
        logger.info(f"Attributes: {result['Thuộc tính']}")
        logger.info(f"Description: {result['Mô tả']}")


def run_validation_tests() -> dict:
    """Run all validation tests on sample products.

    Returns:
        Dict with all test metrics
    """
    logger.info("STARTING PRODUCT VALIDATION TESTS")
    logger.info("")

    products = load_sample_products(n=30)

    if products.empty:
        logger.error("No products loaded. Exiting.")
        return {}

    classification_metrics = test_classification_accuracy(products)
    attribute_metrics = test_attribute_extraction(products)
    pipeline_metrics = test_complete_pipeline(products)

    display_sample_results(products, n=5)

    all_metrics = {
        "classification": classification_metrics,
        "attributes": attribute_metrics,
        "pipeline": pipeline_metrics,
    }

    logger.info("")
    logger.info("=" * 70)
    logger.info("VALIDATION TEST SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Classification accuracy: {classification_metrics['accuracy']:.2%}")
    logger.info(
        f"Attribute extraction rate: {attribute_metrics['attributes_rate']:.2%}"
    )
    logger.info(
        f"Description generation rate: {attribute_metrics['description_rate']:.2%}"
    )
    logger.info(
        f"Pipeline brand extraction: {pipeline_metrics['brand_extracted']}/{pipeline_metrics['total']}"
    )
    logger.info(
        f"Pipeline classification rate: {pipeline_metrics['classification_rate']:.2%}"
    )
    logger.info("=" * 70)

    return all_metrics


if __name__ == "__main__":
    metrics = run_validation_tests()

    if metrics:
        logger.info("Validation tests completed successfully.")
    else:
        logger.error("Validation tests failed.")
