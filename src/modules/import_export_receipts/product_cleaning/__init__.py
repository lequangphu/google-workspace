# -*- coding: utf-8 -*-
"""Product cleaning and classification module.

This module provides a unified interface for:
- Product name cleaning (normalize spaces, standardize dimensions)
- Brand extraction and unification
- Product classification (Vỏ>>Xe máy, etc.)
- Attribute extraction (size, type, position, descriptions)
- Code disambiguation (Mã hàng)
- Full pipeline orchestration

Public API:
    clean_product_name(name: str) -> str
    clean_product_names_series(series: pd.Series) -> pd.Series
    clean_product_code(code: str) -> str
    extract_brand_from_name(name: str) -> str
    classify_product(name: str) -> dict
    extract_attributes_extended(name: str) -> dict
    disambiguate_product_codes(df: pd.DataFrame, ...) -> tuple
    clean_and_extract_complete(name: str) -> dict
    clean_and_extract_series(series: pd.Series) -> pd.DataFrame

All constants are in constants.py.
All actual cleaning logic is in core.py.
Orchestration is in pipeline.py.
"""

from .constants import (
    # All constants
    BRAND_CATEGORIES,
    BRAND_UNIFICATION,
    DIMENSION_PATTERNS,
    POSITION_KEYWORDS,
    PRODUCT_TYPE_KEYWORDS,
    SIMILARITY_THRESHOLD,
    TYPO_CORRECTIONS,
    VEHICLE_TYPE_KEYWORDS,
)

from .core import (
    # Name cleaning
    clean_product_name,
    clean_product_names_series,
    clean_dimension_format,
    normalize_spaces_around_special_chars,
    standardize_dimension,
    standardize_product_type,
    # Code cleaning
    clean_product_code,
    # Brand extraction
    extract_brand_from_name,
    unify_brand_name,
    # Product classification
    classify_product,
    classify_parent_type,
    classify_child_type,
    detect_position,
    # Attribute extraction
    extract_attributes_extended as extract_attributes,
    extract_product_type_attributes,
    # Similarity and disambiguation
    get_similarity,
    group_similar_names,
    normalize_to_newest_name,
    disambiguate_product_codes,
)

from .pipeline import (
    # Full pipeline
    clean_and_extract_complete,
    clean_and_extract_series,
)

from .constants import (
    BRAND_CATEGORIES,
    BRAND_UNIFICATION,
    DIMENSION_PATTERNS,
    POSITION_KEYWORDS,
    PRODUCT_TYPE_KEYWORDS,
    SIMILARITY_THRESHOLD,
    TYPO_CORRECTIONS,
    VEHICLE_TYPE_KEYWORDS,
)

__all__ = [
    # Name cleaning
    "clean_product_name",
    "clean_product_names_series",
    "clean_product_code",
    "extract_brand_from_name",
    "classify_product",
    "extract_attributes_extended",
    "extract_product_type_attributes",
    "disambiguate_product_codes",
    "clean_and_extract_complete",
    "clean_and_extract_series",
    # Constants
    "BRAND_CATEGORIES",
    "BRAND_UNIFICATION",
    "DIMENSION_PATTERNS",
    "POSITION_KEYWORDS",
    "PRODUCT_TYPE_KEYWORDS",
    "SIMILARITY_THRESHOLD",
    "TYPO_CORRECTIONS",
    "VEHICLE_TYPE_KEYWORDS",
]
