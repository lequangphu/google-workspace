# -*- coding: utf-8 -*-
"""
Extract product attributes from product names (Tên hàng).

Supports Vietnamese motorcycle/automotive product naming conventions:
- Vỏ/Lốp (tires): CAMEL 110/70-12 CMI 547 T/L
- Săm (tubes): CHENGSHIN SĂM 225-17
- And other product types with embedded attributes

 Output format: tên thuộc tính:giá trị|...

Module: import_export_receipts
"""

import re
from typing import Optional


def extract_attributes(ten_hang: str) -> str:
    """Extract all attributes from product name.

    Args:
        ten_hang: Product name string (e.g., "CAMEL 110/70-12 CMI 547 T/L")

    Returns:
        Pipe-separated attributes string (e.g., "Kích thước:110/70-12|Loại vỏ:Không ruột")
        or empty string if no patterns matched
    """
    if not ten_hang or not isinstance(ten_hang, str):
        return ""

    ten_hang = ten_hang.strip()
    if not ten_hang:
        return ""

    attributes = []

    dimension = _extract_dimension(ten_hang)
    if dimension:
        attributes.append(f"Kích thước:{dimension}")

    tire_type = _extract_tire_type(ten_hang)
    if tire_type:
        attributes.append(f"Loại vỏ:{tire_type}")

    return "|".join(attributes) if attributes else ""


def _extract_dimension(name: str) -> Optional[str]:
    """Extract dimension/size from product name.

    Priority order handles overlapping patterns correctly.
    """
    dimension = _extract_tire_fractional(name)
    if dimension:
        return dimension

    dimension = _extract_tire_3part(name)
    if dimension:
        return dimension

    dimension = _extract_tube_range(name)
    if dimension:
        return dimension

    dimension = _extract_tube_simple(name)
    if dimension:
        return dimension

    dimension = _extract_tube_no_decimal(name)
    if dimension:
        return dimension

    dimension = _extract_tire_decimal(name)
    if dimension:
        return dimension

    dimension = _extract_french_size(name)
    if dimension:
        return dimension

    dimension = _extract_belt_size(name)
    if dimension:
        return dimension

    dimension = _extract_oil_size(name)
    if dimension:
        return dimension

    return None


def _extract_tire_type(name: str) -> Optional[str]:
    """Extract tire type (loại vỏ) from product name.

    Patterns: T/L, TL, TT, TR
    Returns: "Không ruột" (tubeless) or "Có ruột" (tube type)
    Only matches for tire-related products (has dimension + type indicator).
    """
    tire_keywords = [r"vỏ", r"lốp", r"lốp", r"tyre", r"tire"]
    has_tire_keyword = any(re.search(kw, name, re.IGNORECASE) for kw in tire_keywords)

    has_tire_dimension = _extract_dimension(name) is not None

    if not has_tire_keyword and not has_tire_dimension:
        return None

    patterns = [
        (r"\b(T/?L)\b", "Không ruột"),
        (r"\b(TL)\b(?!\w)", "Không ruột"),
        (r"\b(TT)\b(?!\w)", "Có ruột"),
        (r"\b(TR)\b(?!\w)", "Có ruột"),
    ]

    for pattern, value in patterns:
        if re.search(pattern, name, re.IGNORECASE):
            return value

    return None


def _extract_tire_fractional(name: str) -> Optional[str]:
    """Extract fractional tire size: W/H-D (e.g., 110/70-12)."""
    pattern = r"\b(\d+)/(\d+)-(\d+)\b"
    match = re.search(pattern, name)
    if match:
        return f"{match.group(1)}/{match.group(2)}-{match.group(3)}"
    return None


def _extract_tire_3part(name: str) -> Optional[str]:
    """Extract 3-part tire size: W/H/H-D (e.g., 100/70/17)."""
    pattern = r"\b(\d+)/(\d+)/(\d+)\b"
    match = re.search(pattern, name)
    if match:
        return f"{match.group(1)}/{match.group(2)}/{match.group(3)}"
    return None


def _extract_tire_decimal(name: str) -> Optional[str]:
    """Extract decimal tire size: W-D (e.g., 2.50-18)."""
    pattern = r"\b(\d+\.\d+)-(\d+)\b"
    match = re.search(pattern, name)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return None


def _extract_tube_range(name: str) -> Optional[str]:
    """Extract tube size with range: W1/W2-D (e.g., 2.25/2.50-17)."""
    pattern = r"\b(\d+\.\d+)/(\d+\.\d+)-(\d+)\b"
    match = re.search(pattern, name)
    if match:
        return f"{match.group(1)}/{match.group(2)}-{match.group(3)}"
    return None


def _extract_tube_simple(name: str) -> Optional[str]:
    """Extract simple tube size with decimal: W.D-D (e.g., 2.00-17).

    Only matches if not already matched by other patterns.
    Must have tube-related keywords to avoid false positives.
    """
    tube_keywords = [r"săm", r"tube", r"cam", r"điện"]
    has_tube_keyword = any(re.search(kw, name, re.IGNORECASE) for kw in tube_keywords)

    pattern = r"\b(\d+\.\d+)-(\d+)\b"
    match = re.search(pattern, name)
    if match and has_tube_keyword:
        return f"{match.group(1)}-{match.group(2)}"
    return None


def _extract_tube_no_decimal(name: str) -> Optional[str]:
    """Extract tube size without decimal: WW-D (e.g., 225-17).

    Matches 3-digit numbers followed by dash and 2-digit rim size.
    Must have tube-related keywords to avoid false positives.
    """
    tube_keywords = [r"săm", r"tube", r"cam", r"điện"]
    has_tube_keyword = any(re.search(kw, name, re.IGNORECASE) for kw in tube_keywords)

    pattern = r"\b(\d{2,3})-(\d{2})\b"
    match = re.search(pattern, name)
    if match and has_tube_keyword:
        return f"{match.group(1)}-{match.group(2)}"
    return None


def _extract_french_size(name: str) -> Optional[str]:
    """Extract French/bicycle size: DxW or DxW1/W2 (e.g., 27x1.3/8, 20x1.50/1.75)."""
    pattern = r"\b(\d+)x(\d+\.?\d*/?\d*\.?\d*)\b"
    match = re.search(pattern, name)
    if match:
        return f"{match.group(1)}x{match.group(2)}"
    return None


def _extract_belt_size(name: str) -> Optional[str]:
    """Extract belt/curoa size: simple number after product code (e.g., DÂY CUROA XE SH 150)."""
    pattern = r"(?:dây\s*curoa|curoa|dây\s*passer|dây\s*ga).*?(\d{3,4})\b"
    match = re.search(pattern, name, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def _extract_oil_size(name: str) -> Optional[str]:
    """Extract oil/volume size: N.NL or NML (e.g., 0.8L, 1000ML)."""
    pattern = r"(\d+\.?\d*)\s*(ml|l)\b"
    match = re.search(pattern, name, re.IGNORECASE)
    if match:
        volume = match.group(1)
        unit = match.group(2).upper()
        return f"{volume}{unit}"
    return None


def extract_attributes_series(series: list) -> list:
    """Extract attributes for a list of product names.

    Args:
        series: List containing product names

    Returns:
        List with attribute strings
    """
    return [extract_attributes(str(item)) for item in series]
