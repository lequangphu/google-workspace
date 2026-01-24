# -*- coding: utf-8 -*-
"""Core product cleaning, classification, and attribute extraction functions.

This module consolidates functions from:
- clean_product_names_core.py (name cleaning, dimension standardization)
- product_disambiguation.py (brand extraction, similarity, code disambiguation)
- classify_products.py (product classification)
- product_attributes.py (attribute extraction)

All constants are imported from constants.py.
"""

import logging
import re
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

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

logger = logging.getLogger(__name__)

# ============================================================================
# ISSUE 1: SPACES AROUND SPECIAL CHARACTERS
# ============================================================================


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
    name = re.sub(r"(^|\s)L\.?\s*(?=\d|$)", r"\1", name)

    # Step 2: Convert triple to fractional: W/H/D → W/H-D
    name = re.sub(r"(\d+)/(\d+)/(\d+)", r"\1/\2-\3", name)

    # Step 3: Remove spaces in dimension separators
    name = re.sub(r"(\d+)\s*/\s*(\d+)\s*[-]\s*(\d+)", r"\1/\2-\3", name)
    name = re.sub(r"(\d+)/(\d+)\s+(\d+)", r"\1/\2-\3", name)
    name = re.sub(r"(\d+)\s*\.\s*(\d+)\s*-\s*(\d+)", r"\1.\2-\3", name)

    # Step 4: Normalize bicycle format
    name = re.sub(r"(\d+)\s*[*xX]\s*(\d+[A-Z]?)", r"\1x\2", name)

    name = re.sub(r"\s+", " ", name)

    return name.strip()


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


# ============================================================================
# PRODUCT NAME CLEANING
# ============================================================================


def clean_product_name(name: str) -> str:
    """
    Apply all cleaning rules to product name.

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
# PRODUCT CODE CLEANING (Mã hàng)
# ============================================================================


def clean_product_code(code: str) -> str:
    """
    Clean product code (Mã hàng) by removing special characters and fixing typos.

    Rules:
    1. Remove special characters (*, etc.)
    2. Strip whitespace
    3. Convert to uppercase

    Args:
        code: Product code string

    Returns:
        Cleaned product code
    """
    if not code or not isinstance(code, str):
        return code

    # Remove special characters but keep alphanumeric and hyphens
    cleaned = re.sub(r"[^A-Za-z0-9\-]", "", code)
    # Remove leading/trailing whitespace and uppercase
    cleaned = cleaned.strip().upper()

    return cleaned


# ============================================================================
# BRAND EXTRACTION AND UNIFICATION
# ============================================================================


def unify_brand_name(name: str) -> str:
    """Replace brand variations with unified name.

    Args:
        name: Product name to check for brand variations

    Returns:
        Unified brand name if variation found, empty string otherwise
    """
    name_upper = name.upper()
    for unified, variations in BRAND_UNIFICATION.items():
        for variation in variations:
            if variation in name_upper:
                return unified
    return ""


def extract_brand_from_name(name: str) -> str:
    """Extract brand from product name, categorizing non-brand words.

    Returns:
        Extracted brand name
    """
    if not name or not isinstance(name, str):
        return ""

    name_upper = name.upper().strip()
    words = name_upper.split()

    if not words:
        return ""

    first_word = words[0]

    for category, brand_map in BRAND_CATEGORIES.items():
        if category == "PRODUCT_TYPE_KEYWORDS":
            continue

        for canonical_brand, variations in brand_map.items():
            for variation in variations:
                if first_word == variation.upper():
                    return canonical_brand

    # Check if first word is a product type keyword
    for ptype_variations in BRAND_CATEGORIES["PRODUCT_TYPE_KEYWORDS"].values():
        if first_word in [v.upper() for v in ptype_variations]:
            # Product type, not brand - brand is likely second word
            if len(words) > 1:
                second_word = words[1]
                for category, brand_map in BRAND_CATEGORIES.items():
                    if category == "PRODUCT_TYPE_KEYWORDS":
                        continue
                    for canonical_brand, variations in brand_map.items():
                        for variation in variations:
                            if second_word == variation.upper():
                                return canonical_brand
            # If no brand found after product type, return product type as category
            return first_word

    return first_word


# ============================================================================
# PRODUCT CLASSIFICATION
# ============================================================================


def classify_parent_type(name: str) -> str:
    """Classify product into Nhóm hàng cha (parent type).

    Priority order:
    1. Explicit keywords (Vỏ, Ruột, Nhớt, Bình, Phụ tùng khác)
    2. Inferred from dimension patterns
    3. Default to Phụ tùng khác

    Args:
        name: Product name string

    Returns:
        Parent category name (Vỏ, Ruột, Nhớt, Bình, Phụ tùng khác)
    """
    if not name or not isinstance(name, str):
        return "Phụ tùng khác"

    name_upper = name.upper()

    for category, patterns in PRODUCT_TYPE_KEYWORDS.items():
        for pattern in patterns:
            if re.search(pattern, name_upper, re.IGNORECASE):
                return category

    # If no explicit keywords found, check for dimension patterns
    # Tires/tubes have dimension patterns
    if re.search(r"\b\d+[-/.*]\d+\b", name_upper):
        return "Vỏ"

    return "Phụ tùng khác"


def classify_child_type(name: str, parent_type: str) -> str:
    """Classify product into Nhóm hàng con (vehicle type).

    Uses parent type to narrow down classification:
    - Vỏ/Ruột: Only Xe máy, Xe đạp
    - Bình: Only Xe máy (motorcycles use batteries)
    - Nhớt: Only Xe máy (motorcycles use oil)
    - Phụ tùng khác: Default

    Returns:
        Child category name (Xe máy, Xe đạp, Xe khác)
    """
    if not name or not isinstance(name, str):
        return "Xe khác"

    name_upper = name.upper()

    # Classification based on parent type
    if parent_type == "Vỏ" or parent_type == "Ruột":
        return "Xe máy"

    elif parent_type == "Bình":
        # Motorcycle batteries
        if re.search(r"\b(YTZ|WTZ|WP|YB\dL|YB\d)\b", name_upper):
            return "Xe máy"
        # Other batteries (likely bicycle or other vehicles)
        elif re.search(r"\b(6V|12V)\b", name_upper):
            return "Xe khác"

    elif parent_type == "Nhớt":
        # Motorcycle oil brands
        if re.search(
            r"\b(HONDA|YAMAHA|PIAGIO|SUZUKI|DREAM|WAVE|VISION)\s*NHỚT\b",
            name_upper,
        ):
            return "Xe máy"
        # Other oils (likely bicycle or general purpose)
        return "Xe khác"

    else:
        # Phụ tùng khác
        return "Xe khác"


def detect_position(name: str) -> Optional[str]:
    """Detect tire/tube position from product name.

    Keywords: trước/front, sau/rear, single letter codes

    Returns:
        "Vỏ trước" or "Vỏ sau" or None
    """
    if not name or not isinstance(name, str):
        return None

    name_upper = name.upper()

    for position, patterns in POSITION_KEYWORDS.items():
        for pattern in patterns:
            if re.search(pattern, name_upper, re.IGNORECASE):
                return position

    return None


def classify_product(name: str) -> Dict[str, str]:
    """Classify product into hierarchical categories with position detection.

    Args:
        name: Product name string

    Returns:
        Dict with keys:
            - "Nhóm hàng cha": Parent category (Vỏ, Ruột, Nhớt, Bình, Phụ tùng khác)
            - "Nhóm hàng con": Child category (Xe máy, Xe đạp, Xe khác)
            - "Vị trí": Position (Vỏ trước, Vỏ sau) or empty string
            - "Nhóm hàng(2 Cấp)": Combined format "parent>>child"
    """
    parent = classify_parent_type(name)
    child = classify_child_type(name, parent)
    position = detect_position(name) if parent in ["Vỏ", "Ruột"] else ""

    result = {
        "Nhóm hàng cha": parent,
        "Nhóm hàng con": child,
        "Vị trí": position or "",
        "Nhóm hàng(2 Cấp)": f"{parent}>>{child}",
    }

    logger.debug(
        f"Classified '{name[:50]}...' → {parent}>>{child}"
        + (f" ({position})" if position else "")
    )

    return result


# ============================================================================
# ATTRIBUTE EXTRACTION
# ============================================================================


def _extract_dimension(name: str) -> Optional[str]:
    dimension = _extract_tire_fractional(name)
    if dimension:
        return dimension

    dimension = _extract_tire_3part(name)
    if dimension:
        return dimension

    dimension = _extract_tube_range(name)
    if dimension:
        return dimension

    dimension = _extract_tire_decimal(name)
    if dimension:
        return dimension

    dimension = _extract_french_size(name)
    if dimension:
        return dimension

    dimension = _extract_american_size(name)
    if dimension:
        return dimension

    dimension = _extract_bolt_pattern(name)
    if dimension:
        return dimension

    dimension = _extract_tube_simple(name)
    if dimension:
        return dimension

    return None


def _extract_tire_fractional(name: str) -> Optional[str]:
    pattern = r"\b(\d+)/(\d+)-(\d+)\b"
    match = re.search(pattern, name)
    if match:
        return f"{match.group(1)}/{match.group(2)}-{match.group(3)}"
    return None


def _extract_tire_3part(name: str) -> Optional[str]:
    pattern = r"\b(\d+)/(\d+)/(\d+)\b"
    match = re.search(pattern, name)
    if match:
        return f"{match.group(1)}/{match.group(2)}/{match.group(3)}"
    return None


def _extract_tire_decimal(name: str) -> Optional[str]:
    pattern = r"\b(\d+\.\d+)-(\d+)\b"
    match = re.search(pattern, name)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return None


def _extract_tube_range(name: str) -> Optional[str]:
    pattern = r"\b(\d+\.\d+)/(\d+\.\d+)-(\d+)\b"
    match = re.search(pattern, name)
    if match:
        return f"{match.group(1)}/{match.group(2)}-{match.group(3)}"
    return None


def _extract_tube_simple(name: str) -> Optional[str]:
    pattern = r"\b(\d{2,3})-(\d{2})\b"
    match = re.search(pattern, name)
    if match and not _extract_french_size(name):
        return f"{match.group(1)}-{match.group(2)}"
    return None


def _extract_french_size(name: str) -> Optional[str]:
    pattern = r"\b(\d+)x(\d+\.?\d*)\b"
    match = re.search(pattern, name)
    if match:
        return f"{match.group(1)}x{match.group(2)}"
    return None


def _extract_american_size(name: str) -> Optional[str]:
    pattern = r"\b(\d+)-(\d+)-(\d+)\b"
    match = re.search(pattern, name)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return None


def _extract_bolt_pattern(name: str) -> Optional[str]:
    pattern = r"\b(\d+)[xX-](\d+)\b"
    match = re.search(pattern, name)
    if match:
        return f"{match.group(1)}x{match.group(2)}"
    return None


def _extract_position(name: str) -> Optional[str]:
    if not name or not isinstance(name, str):
        return None

    name_upper = name.upper()
    front_patterns = [r"\bTRƯỚC\b", r"\bFRONT\b", r"\bF\b(?![a-z])"]
    rear_patterns = [r"\bSAU\b", r"\bREAR\b", r"\bR\b(?![a-z])"]

    for pattern in front_patterns:
        if re.search(pattern, name_upper, re.IGNORECASE):
            return "Vỏ trước"

    for pattern in rear_patterns:
        if re.search(pattern, name_upper, re.IGNORECASE):
            return "Vỏ sau"

    return None


def _extract_tire_type(name: str) -> Optional[str]:
    tire_keywords = [r"vỏ", r"lốp", r"tyre", r"tire"]
    has_tire_keyword = any(re.search(kw, name, re.IGNORECASE) for kw in tire_keywords)
    has_dimension = _extract_dimension(name) is not None

    if not has_tire_keyword and not has_dimension:
        return None

    if re.search(r"\b(T/?L)\b", name, re.IGNORECASE):
        return "Không ruột"

    if re.search(r"\b(TL)\b(?!\w)", name, re.IGNORECASE):
        return "Không ruột"

    if re.search(r"\b(TT)\b(?!\w)", name):
        return "Có ruột"

    if re.search(r"\b(TR)\b(?!\w)", name, re.IGNORECASE):
        return "Có ruột"

    return None


def _extract_ply_rating(name: str) -> Optional[int]:
    match = re.search(r"(\d+)PR", name, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def _extract_load_index(name: str) -> Optional[str]:
    match = re.search(r"(\d+)([A-ZÀ-Ỹ])\b", name)
    if match and "PR" not in match.group(0):
        return f"{match.group(1)}{match.group(2)}"
    return None


def _extract_region_code(name: str) -> Optional[str]:
    match = re.search(r"-([A-ZÀ-Ỹ]+(?:/[A-ZÀ-Ỹ]+)*)\b", name)
    if match:
        return match.group(1)

    match = re.search(r"\((\s*[A-ZÀ-Ỹ]+(?:,\s*[A-ZÀ-Ỹ]+)\s*)\)", name)
    if match:
        region = match.group(1).replace(", ", "/").replace(",", "/").replace(" ", "")
        return region

    return None


def _extract_battery_code(name: str) -> Optional[str]:
    match = re.search(r"\b(YTZ|WTZ|WP|YB\dL|YB\d)\b", name, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return None


def _extract_battery_voltage(name: str) -> Optional[str]:
    match = re.search(r"(\d+)[vV]", name)
    if match:
        return f"{match.group(1)}V"
    return None


def _extract_oil_volume(name: str) -> Optional[str]:
    match = re.search(r"(\d+\.?\d*)\s*(ml|l)\b", name, re.IGNORECASE)
    if match:
        volume = match.group(1)
        unit = match.group(2).upper()
        return f"{volume}{unit}"
    return None


def _extract_oil_brand(name: str) -> Optional[str]:
    oil_brands = [
        "HONDA",
        "YAMAHA",
        "PIAGIO",
        "SUZUKI",
        "CASTROL",
        "SHELL",
        "MOTUL",
        "TOTAL",
        "THẮNG MEKONG",
        "VISTRA",
        "POWER",
        "TABET",
        "ACTIVE",
    ]

    for brand in oil_brands:
        if brand in name.upper():
            return brand
    return None


def _extract_belt_length(name: str) -> Optional[str]:
    match = re.search(
        r"(?:dây\s*curoa|curoa|dây\s*passer|dây\s*ga).*?(\d{3,4})\b",
        name,
        re.IGNORECASE,
    )
    if match:
        return match.group(1)
    return None


def _explain_dimension(dimension: str) -> str:
    if not dimension:
        return ""

    dimension = dimension.strip()

    match = re.search(r"^(\d+\.?\d*)/(\d+\.?\d*)[-/](\d+)$", dimension)
    if match:
        width = match.group(1)
        height = match.group(2)
        rim = match.group(3)

        width_mm = width.replace(".", ",") if "." in width else width
        height_mm = height.replace(".", ",") if "." in height else height

        return f"Kích thước lốp: rộng {width_mm} milimét, cao {height_mm} milimét, đường kính {rim} insơ"

    match = re.search(r"^(\d+\.\d+)-(\d+)$", dimension)
    if match:
        width = match.group(1)
        rim = match.group(2)

        width_in = width.replace(".", ",")
        return f"Kích thước lốp: rộng {width_in} insơ, đường kính {rim} insơ"

    match = re.search(r"^(\d+)[x*](\d+\.?\d*[A-Z]?)$", dimension)
    if match:
        rim = match.group(1)
        width = match.group(2).rstrip("A-Z")

        width_in = width.replace(".", ",") if "." in width else width
        return f"Kích thước lốp: đường kính {rim} insơ, rộng {width_in} insơ"

    match = re.search(r"^(\d+)-(\d+)-(\d+)$", dimension)
    if match:
        width = match.group(1)
        rim = match.group(2)
        pattern = match.group(3)

        return (
            f"Kích thước lốp: rộng {width} insơ, đường kính {rim} insơ, mẫu {pattern}"
        )

    match = re.search(r"^(\d+)[xX-](\d+)$", dimension)
    if match:
        pattern = match.group(1)
        rim = match.group(2)

        return f"Kích thước lốp: mẫu {pattern}, đường kính {rim} milimét"

    match = re.search(r"^(\d{2,3})-(\d{2})$", dimension)
    if match:
        width = match.group(1)
        rim = match.group(2)

        return f"Kích thước lốp: rộng {width} milimét, đường kính {rim} insơ"

    return f"Kích thước lốp: {dimension}"


def _generate_description(attributes: Dict[str, Any]) -> str:
    description_parts = []

    dimension = attributes.get("Kích thước")
    if dimension:
        description_parts.append(_explain_dimension(dimension))

    tire_type = attributes.get("Loại vỏ")
    if tire_type:
        description_parts.append(f"Loại lốp: {tire_type}")

    position = attributes.get("Vị trí")
    if position:
        description_parts.append(f"Vị trí: {position}")

    ply = attributes.get("Chỉ số PR")
    if ply:
        description_parts.append(f"Chỉ số chịu tải: {ply} lớp")

    load_index = attributes.get("Chỉ số tải")
    if load_index:
        description_parts.append(f"Chỉ số tải: {load_index}")

    region = attributes.get("Khu vực")
    if region:
        region_text = region.replace("/", " và ")
        description_parts.append(f"Khu vực: {region_text}")

    battery_code = attributes.get("Mã bình")
    if battery_code:
        description_parts.append(f"Mã bình: {battery_code}")

    battery_voltage = attributes.get("Điện áp")
    if battery_voltage:
        description_parts.append(f"Điện áp: {battery_voltage}")

    oil_volume = attributes.get("Dung tích nhớt")
    if oil_volume:
        volume = oil_volume.replace("L", " lít").replace("ML", " mililít")
        description_parts.append(f"Dung tích: {volume}")

    oil_brand = attributes.get("Thương hiệu nhớt")
    if oil_brand:
        description_parts.append(f"Thương hiệu: {oil_brand}")

    belt_length = attributes.get("Chiều dây curoa")
    if belt_length:
        description_parts.append(f"Chiều dài dây curoa: {belt_length}")

    return ". ".join(description_parts) if description_parts else ""


def extract_attributes_extended(name: str) -> Dict[str, str]:
    """Extract all product attributes and generate Vietnamese description.

    Args:
        name: Product name string

    Returns:
        Dict with keys:
            - "Thuộc tính": Pipe-separated attributes
            - "Mô tả": Human-readable description in Vietnamese
    """
    if not name or not isinstance(name, str):
        return {"Thuộc tính": "", "Mô tả": ""}

    attributes = {}

    dimension = _extract_dimension(name)
    if dimension:
        attributes["Kích thước"] = dimension

    tire_type = _extract_tire_type(name)
    if tire_type:
        attributes["Loại vỏ"] = tire_type

    position = _extract_position(name)
    if position:
        attributes["Vị trí"] = position

    ply_rating = _extract_ply_rating(name)
    if ply_rating:
        attributes["Chỉ số PR"] = f"{ply_rating}PR"

    load_index = _extract_load_index(name)
    if load_index:
        attributes["Chỉ số tải"] = load_index

    region_code = _extract_region_code(name)
    if region_code:
        attributes["Khu vực"] = region_code

    battery_code = _extract_battery_code(name)
    if battery_code:
        attributes["Mã bình"] = battery_code

    battery_voltage = _extract_battery_voltage(name)
    if battery_voltage:
        attributes["Điện áp"] = battery_voltage

    oil_volume = _extract_oil_volume(name)
    if oil_volume:
        attributes["Dung tích nhớt"] = oil_volume

    oil_brand = _extract_oil_brand(name)
    if oil_brand:
        attributes["Thương hiệu nhớt"] = oil_brand

    belt_length = _extract_belt_length(name)
    if belt_length:
        attributes["Chiều dây curoa"] = belt_length

    attributes_str = "|".join([f"{k}:{v}" for k, v in attributes.items()])
    description = _generate_description(attributes)

    return {
        "Thuộc tính": attributes_str,
        "Mô tả": description,
    }


def extract_product_type_attributes(name: str) -> Dict[str, Any]:
    """Extract product type attributes (legacy function for backward compatibility).

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
# SIMILARITY AND DISAMBIGUATION
# ============================================================================


def get_similarity(name1: str, name2: str) -> float:
    """Calculate similarity ratio between two names using SequenceMatcher.

    Args:
        name1: First product name
        name2: Second product name

    Returns:
        Similarity ratio between 0.0 and 1.0
    """
    return SequenceMatcher(None, name1.upper(), name2.upper()).ratio()


def group_similar_names(
    names: List[str], threshold: float = SIMILARITY_THRESHOLD
) -> List[List[int]]:
    """Group names that are similar to each other using connected components.

    Uses BFS to find connected components in similarity graph where
    edge exists if similarity >= threshold.

    Args:
        names: List of unique product names
        threshold: Similarity threshold (default: 0.8)

    Returns:
        List of groups, each group is list of indices into names list
    """
    n = len(names)
    if n <= 1:
        return [[0]] if n == 1 else []

    # Build adjacency matrix
    similar = [[False] * n for _ in range(n)]
    for i in range(n):
        for j in range(i + 1, n):
            if get_similarity(names[i], names[j]) >= threshold:
                similar[i][j] = similar[j][i] = True
                similar[i][i] = similar[j][j] = True

    # Find connected components (groups of similar names)
    visited = [False] * n
    groups = []

    for i in range(n):
        if not visited[i]:
            group = []
            stack = [i]
            visited[i] = True
            while stack:
                node = stack.pop()
                group.append(node)
                for j in range(n):
                    if similar[node][j] and not visited[j]:
                        visited[j] = True
                        stack.append(j)
            groups.append(group)

    return groups


def normalize_to_newest_name(
    group: pd.DataFrame,
    name_col: str,
    date_col: str,
) -> str:
    """Normalize group to use newest name with typo corrections.

    Args:
        group: DataFrame with records for same product code
        name_col: Name of product name column
        date_col: Name of date column

    Returns:
        Newest name with corrections applied
    """
    name_dates = {}
    for name in group[name_col].unique():
        if date_col in group.columns:
            dates = pd.to_datetime(
                group[group[name_col] == name][date_col], errors="coerce"
            )
            name_dates[name] = dates.max()
        else:
            name_dates[name] = pd.Timestamp.min()

    # Find newest name
    newest_name = max(
        name_dates.keys(), key=lambda n: name_dates[n] or pd.Timestamp.min()
    )

    # Apply typo corrections
    for typo, correction in TYPO_CORRECTIONS.items():
        if typo in newest_name.upper():
            newest_name = newest_name.replace(typo, correction)

    return newest_name


def disambiguate_product_codes(
    df: pd.DataFrame,
    code_col: str = "Mã hàng",
    name_col: str = "Tên hàng",
    date_col: Optional[str] = "Ngày",
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Disambiguate product codes by normalizing or suffixing based on similarity.

    Args:
        df: DataFrame with product codes and names
        code_col: Column name for product code
        name_col: Column name for product name
        date_col: Column name for date (used to determine newest name)

    Returns:
        Tuple of (modified DataFrame, statistics dict)
    """
    if df.empty:
        return df, {"codes_processed": 0, "normalized": 0, "suffixed": 0}

    df = df.copy()
    stats = {
        "codes_processed": 0,
        "normalized": 0,
        "suffixed": 0,
        "groups": [],
    }

    grouped = df.groupby(code_col, group_keys=False)

    for code, group in grouped:
        unique_names = group[name_col].unique().tolist()

        if len(unique_names) <= 1:
            continue

        stats["codes_processed"] += 1

        name_groups = group_similar_names(unique_names, SIMILARITY_THRESHOLD)

        # Check if all names are in one group (all similar)
        if len(name_groups) == 1 and len(name_groups[0]) == len(unique_names):
            all_similar = True
            for i in range(len(unique_names)):
                for j in range(i + 1, len(unique_names)):
                    if (
                        get_similarity(unique_names[i], unique_names[j])
                        < SIMILARITY_THRESHOLD
                    ):
                        all_similar = False
                        break
                if not all_similar:
                    break
        elif len(name_groups) == len(unique_names):
            # Each in own group → suffix all
            all_similar = False
        else:
            # Mixed groups
            all_similar = False

        if all_similar:
            # Normalize all to newest name
            newest_name = normalize_to_newest_name(group, name_col, date_col)

            mask = (df[code_col] == code) & (df[name_col].isin(unique_names))
            df.loc[mask, name_col] = newest_name
            stats["normalized"] += mask.sum()
            stats["groups"].append(
                {
                    "code": code,
                    "action": "normalized",
                    "newest_name": newest_name,
                    "name_count": len(unique_names),
                }
            )
            logger.info(
                f"Normalized '{code}': {len(unique_names)} names → '{newest_name[:80]}'"
            )
            continue

        for group_idx, group_indices in enumerate(name_groups):
            group_names = [unique_names[i] for i in group_indices]

            if len(group_names) == 1:
                # Single name in group
                name = group_names[0]

                if len(name_groups) > 1:
                    # Multiple groups → suffix this group
                    if group_idx > 0:  # First group keeps original code
                        suffix_code = f"{code}-{group_idx:02d}"
                        mask = (df[code_col] == code) & (df[name_col] == name)
                        df.loc[mask, code_col] = suffix_code
                        stats["suffixed"] += mask.sum()
                        stats["groups"].append(
                            {
                                "code": code,
                                "action": "suffixed",
                                "new_code": suffix_code,
                                "name": name,
                            }
                        )
                        logger.info(f"Suffixed '{code}': {suffix_code} ← '{name[:80]}'")
            else:
                # Multiple names in group → normalize to newest name
                newest_name = normalize_to_newest_name(
                    group[group[name_col] == group_names[0]],
                    name_col,
                    date_col,
                )

                mask = (df[code_col] == code) & (df[name_col].isin(group_names))
                df.loc[mask, name_col] = newest_name
                stats["normalized"] += mask.sum()
                stats["groups"].append(
                    {
                        "code": code,
                        "action": "normalized",
                        "newest_name": newest_name,
                        "name_count": len(group_names),
                        "group_idx": group_idx,
                    }
                )
                logger.info(
                    f"Normalized '{code}' (group {group_idx}): {len(group_names)} names → '{newest_name[:80]}'"
                )

    if stats["codes_processed"] > 0:
        logger.info(
            f"Product disambiguation: {stats['codes_processed']} codes processed, "
            f"{stats['normalized']} records normalized, {stats['suffixed']} records suffixed"
        )

    return df, stats
