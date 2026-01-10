# -*- coding: utf-8 -*-
"""Clean product names from staging data.

Module: import_export_receipts
This module provides functions to clean product names by:
1. Normalizing spaces around special characters (Issue 1)
2. Standardizing dimension formats (Issue 4)

Phase 1 Implementation:
- Issue 1: Spaces around special characters (-, /, ., *, etc.)
- Issue 4: Dimension format (W/H/D → W/H-D, drop leading letter)

Output: Cleaned product names ready for attribute extraction
"""

import re
from typing import Optional, Tuple
import logging

import pandas as pd

logger = logging.getLogger(__name__)


# ============================================================================
# ISSUE 1: SPACES AROUND SPECIAL CHARACTERS
# ============================================================================

SPECIAL_CHARS_PATTERN = r"[-/.*:;()\[\]{}]"


def normalize_spaces_around_special_chars(name: str) -> str:
    """
    Remove spaces around special characters like '-', '/', '.', '*', etc.

    Rules:
    1. Remove space BEFORE special char if preceded by letter/digit
    2. Remove space AFTER special char if followed by letter/digit
    3. Preserve spaces around operators like '+', 'x' if used
    4. Remove spaces around parentheses

    Examples:
        "CHENGSHIN L. 80/90-17" → "CHENGSHIN L.80/90-17"
        "MiCHENLIN 70/90 - 17 TT" → "MiCHENLIN 70/90-17 TT"
        "KENDA Vỏ 700*23C" → "KENDA Vỏ 700*23C" (no change needed)
        "( N )" → "(N)"

    Args:
        name: Product name string

    Returns:
        Cleaned product name with normalized spaces
    """
    if not name or not isinstance(name, str):
        return name

    # Keep space after "L." to enable matching (removes "L 80" but preserves "L. 80")
    name = re.sub(
        r"([A-Za-zÀ-ỹ])(?!\.)\s*([/\-*])\s*([A-Za-zÀ-ỹ0-9])", r"\1 \2\3", name
    )
    name = re.sub(r"([A-Za-zÀ-ỹ])\.\s*", r"\1.", name)
    name = re.sub(r"(\d+)\s*[*xX]\s*(\d+[A-Z]?)", r"\1*\2", name)
    name = re.sub(r"\s*\)", ")", name)
    name = re.sub(r"\s*,\s*", ",", name)
    name = re.sub(r"\s+", " ", name)

    return name.strip()


# ============================================================================
# ISSUE 4: DIMENSION FORMAT STANDARDIZATION
# ============================================================================

# Dimension format patterns (priority order matters!)
DIMENSION_PATTERNS = {
    # CRITICAL: Pattern overlap exists! Order is essential for correct matching.
    # triple_tire must come BEFORE fractional_tire because "80/90/17"
    # would partially match the fractional pattern (\d+)/(\d+) and fail.
    # Tube range must also come before decimal_tire for similar reasons.
    #
    # Fractional motorcycle tire: W/H-D (e.g., 80/90-17)
    "fractional_tire": r"(\d+)/(\d+)-(\d+)",
    # 3-part tire: W/H/D (e.g., 80/90/17) - needs to convert to W/H-D
    # MUST precede fractional_tire due to pattern overlap
    "triple_tire": r"(\d+)/(\d+)/(\d+)",
    # Decimal motorcycle tire: W.D-D (e.g., 2.50-17)
    "decimal_tire": r"(\d+\.\d+)-(\d+)",
    # Tube range: W1/W2-D (e.g., 2.25/2.50-17)
    # MUST precede decimal_tire due to pattern overlap
    "tube_range": r"(\d+\.\d+)/(\d+\.\d+)-(\d+)",
    # French/bicycle size: DxW (e.g., 27x1.5, 27x1-3/8)
    "french_size": r"(\d+)x([\d\./\-]+)",
}


def standardize_dimension(name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Standardize dimension format in product name.

    Rules:
    - Extract dimension pattern from name
    - Convert W/H/D → W/H-D
    - Drop leading letter (L., L, etc.)
    - Return standardized dimension string

    Examples:
        "L.80/90/17" → "80/90-17"
        "L. 80/90-17" → "80/90-17"
        "2.50-17" → "2.50-17"
        "700*23C" → "700*23C"

    Args:
        name: Product name string

    Returns:
        Tuple of (standardized_dimension, dimension_type)
        - dimension: Standardized dimension string or None
        - type: One of 'motorcycle_tire', 'tube', 'bicycle', 'unknown'
    """
    if not name or not isinstance(name, str):
        return None, "unknown"

    name_clean = re.sub(r"^[A-Za-zÀ-ỹ]\.?\s*", "", name)

    for dim_type, pattern in DIMENSION_PATTERNS.items():
        match = re.search(pattern, name_clean)
        if match:
            groups = match.groups()

            if dim_type == "fractional_tire":
                dim = f"{groups[0]}/{groups[1]}-{groups[2]}"
                return dim, "motorcycle_tire"

            elif dim_type == "triple_tire":
                dim = f"{groups[0]}/{groups[1]}-{groups[2]}"
                return dim, "motorcycle_tire"

            elif dim_type == "decimal_tire":
                dim = f"{groups[0]}-{groups[1]}"
                return dim, "motorcycle_tire"

            elif dim_type == "tube_range":
                dim = f"{groups[0]}/{groups[1]}-{groups[2]}"
                return dim, "tube"

            elif dim_type == "french_size":
                dim = f"{groups[0]}x{groups[1]}"
                return dim, "bicycle"

    return None, "unknown"


def clean_dimension_format(name: str) -> str:
    """
    Clean dimension format inconsistencies in product name.

    Rules:
    1. Drop leading letter: "L.80/90-17" → "80/90-17", "CHENGSHIN L.80/90-17" → "CHENGSHIN 80/90-17"
    2. Convert W/H/D → W/H-D (triple to fractional)
    3. Remove spaces around dimension separators
    4. Ensure consistent format: W/H-D (fractional) or W.D-D (decimal)
    5. Handle bicycle format: 700*23C → 700x23C

    Examples:
        "L.80/90/17" → "80/90-17"
        "L. 80/90-17" → "80/90-17"
        "CHENGSHIN L.80/90-17 RS" → "CHENGSHIN 80/90-17 RS"
        "KENDA 700*23C" → "KENDA 700x23C"

    Args:
        name: Product name string

    Returns:
        Product name with standardized dimension format
    """
    if not name or not isinstance(name, str):
        return name

    # Step 1: Remove "L." or "L " when followed by dimension
    # Handles: "L.80/90-17" (start), "CHENGSHIN L.80/90-17" (after space), "L 80/90-17" (space before)
    # Pattern: Match L or L. followed by optional spaces then digit, or at string end
    name = re.sub(r"(^|\s)L\.?\s*(?=\d|$)", r"\1", name)

    # Step 2: Convert triple to fractional: W/H/D → W/H-D
    name = re.sub(r"(\d+)/(\d+)/(\d+)", r"\1/\2-\3", name)

    # Step 3: Remove spaces in dimension separators
    name = re.sub(r"(\d+)\s*/\s*(\d+)\s*[-]\s*(\d+)", r"\1/\2-\3", name)
    name = re.sub(r"(\d+)/(\d+)\s+(\d+)", r"\1/\2-\3", name)
    name = re.sub(r"(\d+)/(\d+)\s+(\d+)", r"\1/\2-\3", name)
    name = re.sub(r"(\d+)\s*\.\s*(\d+)\s*-\s*(\d+)", r"\1.\2-\3", name)

    # Step 4: Normalize bicycle format
    name = re.sub(r"(\d+)\s*[*xX]\s*(\d+[A-Z]?)", r"\1x\2", name)

    name = re.sub(r"\s+", " ", name)

    return name.strip()


# ============================================================================
# BATCH PROCESSING FUNCTIONS
# ============================================================================


def clean_product_name(name: str) -> str:
    """
    Apply all cleaning rules to product name (Phase 1 + Phase 2).

    Cleaning order:
    1. Normalize spaces around special characters (Issue 1)
    2. Clean dimension format (Issue 4)
    3. Standardize product type format (Issue 5: T/L, TT, PR, region codes)

    Args:
        name: Product name string

    Returns:
        Fully cleaned product name with standardized format
    """
    if not name or not isinstance(name, str):
        return name

    name = normalize_spaces_around_special_chars(name)
    name = clean_dimension_format(name)
    name = standardize_product_type(name)

    return name


def clean_product_names_series(series: pd.Series) -> pd.Series:
    """Clean a pandas Series of product names with data loss tracking.

    Args:
        series: Pandas Series containing product names

    Returns:
        Pandas Series with cleaned product names
    """
    if series.empty:
        logger.warning("clean_product_names_series: empty input series")
        return series

    initial_count = len(series)
    initial_nulls = series.isna().sum()
    initial_empty = (series == "").sum()

    result = series.apply(clean_product_name)

    final_nulls = result.isna().sum()
    final_empty = (result == "").sum()

    if initial_nulls + initial_empty < final_nulls + final_empty:
        lost_count = (final_nulls + final_empty) - (initial_nulls + initial_empty)
        logger.warning(
            f"clean_product_names_series: {lost_count} names failed to clean "
            f"({initial_count} total input, {final_nulls} null, {final_empty} empty)"
        )

    return result


# ============================================================================
# ISSUE 5: PRODUCT TYPE FORMAT STANDARDIZATION
# ============================================================================


def standardize_product_type(name: str) -> str:
    """Standardize product type indicators in product name.

    Rules:
    1. Normalize T/L variations: "T L", "TL" → "T/L"
    2. Normalize TT variations: "TT" → "T/T"
    3. Normalize PR spacing: "6 PR" → "6PR"
    4. Standardize region codes: "(N)" → "-N", "(N, S)" → "-N/S"

    Examples:
        "T/L" → "T/L"
        "TL" → "T/L"
        "T L" → "T/L"
        "TT" → "T/T"
        "6 PR" → "6PR"
        "(N)" → "-N"

    Args:
        name: Product name string

    Returns:
        Product name with standardized product type format
    """
    if not name or not isinstance(name, str):
        return name

    # Normalize T/L variations
    name = re.sub(r"\bT\s+L\b", "T/L", name, flags=re.IGNORECASE)
    name = re.sub(r"\bTL\b", "T/L", name, flags=re.IGNORECASE)

    # Normalize TT to T/T
    name = re.sub(r"\bTT\b(?!\w)", "T/T", name)

    # Normalize PR spacing
    name = re.sub(r"(\d+)\s+PR\b", r"\1PR", name)

    # Standardize region codes: (N) → -N, (N, S) → -N/S
    name = re.sub(
        r"\((\s*[A-ZÀ-Ỹ]+(?:,\s*[A-ZÀ-Ỹ]+)\s*)\)",
        lambda m: f"-{m.group(1).replace(', ', '/').replace(',', '/').replace(' ', '')}",
        name,
    )

    return name


def extract_product_type_attributes(name: str) -> dict:
    """Extract product type attributes as structured data.

    Returns a dict with keys:
    - tire_type: "tubeless", "tube_type", "unknown"
    - ply_rating: int or None
    - load_index: str or None (e.g., "38P")
    - region_code: str or None (e.g., "N")
    - has_pattern: bool (directional pattern indicator)

    Args:
        name: Product name string

    Returns:
        Dict with extracted product type attributes
    """
    if not name or not isinstance(name, str):
        return {}

    result = {
        "tire_type": "unknown",
        "ply_rating": None,
        "load_index": None,
        "region_code": None,
        "has_pattern": False,
    }

    # Extract tire type
    if re.search(r"\bT/L\b", name, re.IGNORECASE):
        result["tire_type"] = "tubeless"
    elif re.search(r"\bT/T\b", name):
        result["tire_type"] = "tube_type"

    # Extract ply rating
    pr_match = re.search(r"(\d+)PR", name, re.IGNORECASE)
    if pr_match:
        result["ply_rating"] = int(pr_match.group(1))

    # Extract load index (but not PR patterns)
    li_match = re.search(r"(\d+)([A-ZÀ-Ỹ])\b", name)
    if li_match and not re.search(r"PR", name):
        result["load_index"] = f"{li_match.group(1)}{li_match.group(2)}"

    # Extract region code (after - sign if present)
    region_match = re.search(r"-([A-ZÀ-Ỹ]+(?:/[A-ZÀ-Ỹ]+)*)\b", name)
    if region_match:
        result["region_code"] = region_match.group(1)

    # Check for directional pattern
    if re.search(r"\b(RS|T/T|R/T|RU)\b", name, re.IGNORECASE):
        result["has_pattern"] = True

    return result


# ============================================================================
# UNIFIED CLEANING INTERFACE
# ============================================================================


def clean_and_extract(name: str) -> dict:
    """Apply full cleaning and extract all product attributes.

    Combines Phase 1+2 cleaning with attribute extraction and brand identification
    for downstream callers that need complete product information in one call.

    Args:
        name: Product name string

    Returns:
        Dict with keys:
            - name_clean: Fully cleaned product name (Phase 1 + Phase 2)
            - brand: Extracted brand (from product_disambiguation module)
            - attributes: Extracted product type attributes
                - tire_type: "tubeless", "tube_type", "unknown"
                - ply_rating: int or None
                - load_index: str or None
                - region_code: str or None
                - has_pattern: bool
    """
    from src.modules.import_export_receipts.product_disambiguation import (
        extract_brand_from_name,
    )

    if not name or not isinstance(name, str):
        return {
            "name_clean": name,
            "brand": "",
            "attributes": {
                "tire_type": "unknown",
                "ply_rating": None,
                "load_index": None,
                "region_code": None,
                "has_pattern": False,
            },
        }

    cleaned_name = clean_product_name(name)
    brand = extract_brand_from_name(cleaned_name)
    attributes = extract_product_type_attributes(cleaned_name)

    return {
        "name_clean": cleaned_name,
        "brand": brand,
        "attributes": attributes,
    }


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================


def check_cleaning_quality(original: str, cleaned: str) -> dict:
    """
    Check quality of product name cleaning.

    Returns a dict with metrics:
    - spaces_removed: bool (if multiple spaces were removed)
    - dimension_cleaned: bool (if dimension was standardized)
    - special_chars_normalized: bool (if spaces around special chars were removed)
    - dimension_extracted: Optional[str] (extracted dimension if found)

    Args:
        original: Original product name
        cleaned: Cleaned product name

    Returns:
        Dict with cleaning quality metrics
    """
    metrics = {
        "spaces_removed": False,
        "dimension_cleaned": False,
        "special_chars_normalized": False,
        "dimension_extracted": None,
    }

    if re.search(r"\s{2,}", original):
        metrics["spaces_removed"] = not re.search(r"\s{2,}", cleaned)

    if re.search(r"[A-Za-zÀ-ỹ0-9]\s*[/\-.*]\s*[A-Za-zÀ-ỹ0-9]", original):
        metrics["special_chars_normalized"] = cleaned != original and not re.search(
            r"[A-Za-zÀ-ỹ0-9]\s*[/\-.*]\s*[A-Za-zÀ-ỹ0-9]", cleaned
        )

    dim_orig, _ = standardize_dimension(original)
    dim_clean, _ = standardize_dimension(cleaned)
    if dim_orig:
        metrics["dimension_cleaned"] = dim_clean != dim_orig or (
            "L." in original or "L " in original
        )
        metrics["dimension_extracted"] = dim_clean

    return metrics
