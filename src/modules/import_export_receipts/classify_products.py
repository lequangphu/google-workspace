# -*- coding: utf-8 -*-
"""
Classify products into hierarchical categories.

Module: import_export_receipts
This module provides functions to classify products into:
- Nhóm hàng cha (parent): Vỏ, Ruột, Nhớt, Bình, Phụ tùng khác
- Nhóm hàng con (child): Xe máy, Xe đạp, Xe khác
- Position: Vỏ trước, Vỏ sau (tires only)

Output format: parent>>child (e.g., 'Vỏ>>Xe máy')
"""

import logging
import re
from typing import Dict, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# PRODUCT TYPE CLASSIFICATION (Nhóm hàng cha)
# ============================================================================

PRODUCT_TYPE_KEYWORDS = {
    "Vỏ": [
        r"\bvỏ\b",
        r"\blốp\b",
        r"\btyre\b",
        r"\btire\b",
        # Check for dimension patterns (indicates tire/tube)
        r"\b\d+[-/.*]\d+\b",  # e.g., 80/90-17, 225-17, 2.50-17
        r"\b\d+\.\d+[-/]\d+\b",  # e.g., 2.50-17, 3.50-10
    ],
    "Ruột": [
        r"\bsăm\b",
        r"\btube\b",
        r"\bruột\b",
    ],
    "Nhớt": [
        r"\bnhớt\b",
        r"\bdầu\b",
        r"\boil\b",
    ],
    "Bình": [
        r"\bbình\b",
        r"\bắc quy\b",
        r"\bbattery\b",
    ],
    "Phụ tùng khác": [
        r"\bdây\b",
        r"\bcuroa\b",
        r"\bpasser\b",
        r"\bphanh\b",
        r"\bbrake\b",
        r"\bkeo\b",
        r"\bmâm\b",
        r"\bnồi\b",
        r"\bxích\b",
        r"\btrục\b",
        r"\bđĩa\b",
        r"\bcốt\b",
        r"\bniền\b",
    ],
}


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


# ============================================================================
# VEHICLE TYPE CLASSIFICATION (Nhóm hàng con)
# ============================================================================

VEHICLE_TYPE_KEYWORDS = {
    "Xe máy": [
        r"\b(VISION|LEAD|ATILA|VARIO|NOUVO|CLICK|AB)\b",
        r"\b\d{2,3}cc\b",
        r"\b110cc\b",
        r"\b125cc\b",
    ],
    "Xe côn tay": [
        r"\b(SH|NOVA|WINNER|PCX|NMAX|MAXSYM|GRANDEX|HONDA)\b",
        r"\b\d{3}\s*(ABS|CBS)\b",
    ],
    "Xe tay ga công nghệ": [
        r"\b(AIR\s+BLADE|SH\s+MODEI\s+VISION\s+125)\b",
        r"\b\d{3}\s*125\b",
    ],
    "Xe phân khối lớn": [
        r"\b(PIAGIO|LIBERTY|SYM|VESPA)\b",
        r"\b(150cc|175cc|250cc)\b",
    ],
    "Xe điện": [
        r"\b(XE\s+ĐIỆN|ELECTRIC|MẸNH\s+ĐIỆN|YADEA|KLARWIN|DAT\s+BIKE|VINFAST|YADEA|KABO|MODAI|LUMIX|ONIO)\b",
        r"\b\d{2,3}[VW]\b",
        r"\b\d{2,3}\s*WATT\b",
    ],
    "Xe đạp": [
        r"\b\d{2}[x*]\d+\b",
        r"\b(700|650|600)\*[A-Z0-9]+\b",
        r"\bxe\s+đạp\b",
    ],
}


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


# ============================================================================
# POSITION DETECTION (Vị trí: Vỏ trước/Vỏ sau)
# ============================================================================

POSITION_KEYWORDS = {
    "Vỏ trước": [
        r"\btrước\b",
        r"\bfront\b",
        r"\bF\b(?![a-z])",
    ],
    "Vỏ sau": [
        r"\bsau\b",
        r"\brear\b",
        r"\bR\b(?![a-z])",
    ],
}


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


# ============================================================================
# UNIFIED CLASSIFICATION FUNCTION
# ============================================================================


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
# VALIDATION FUNCTIONS
# ============================================================================


def validate_classification(result: Dict[str, str]) -> bool:
    """Validate classification result.

    Checks:
    - All required keys present
    - Parent category is valid
    - Child category is valid
    - Combined format matches parent>>child

    Args:
        result: Classification dict from classify_product()

    Returns:
        True if valid, False otherwise
    """
    required_keys = ["Nhóm hàng cha", "Nhóm hàng con", "Vị trí", "Nhóm hàng(2 Cấp)"]
    valid_parents = list(PRODUCT_TYPE_KEYWORDS.keys())
    valid_children = ["Xe máy", "Xe đạp", "Xe khác"]

    if not all(key in result for key in required_keys):
        logger.warning(f"Classification missing required key: {result}")
        return False

    if result["Nhóm hàng cha"] not in valid_parents:
        logger.warning(f"Invalid parent type: {result['Nhóm hàng cha']}")
        return False

    if result["Nhóm hàng con"] not in valid_children:
        logger.warning(f"Invalid child type: {result['Nhóm hàng con']}")
        return False

    expected_combined = f"{result['Nhóm hàng cha']}>>{result['Nhóm hàng con']}"
    if result["Nhóm hàng(2 Cấp)"] != expected_combined:
        logger.warning(
            f"Combined format mismatch: {result['Nhóm hàng(2 Cấp)']} != {expected_combined}"
        )
        return False

    return True
