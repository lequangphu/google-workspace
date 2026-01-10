# -*- coding: utf-8 -*-
"""
Extract extended product attributes from product names (Tên hàng).

Supports Vietnamese motorcycle/automotive product naming conventions:
- Vỏ/Lốp (tires): CAMEL 110/70-12 CMI 547 T/L
- Săm (tubes): CHENGSHIN SĂM 225-17
- Batteries: BÌNH WAVE RS L1 (WTZ5S)
- Oil: NHỚT HONDA SỐ 0.8L
- Belts: DÂY CUROA XE SH 150

Enhanced features:
- Position detection (Vỏ trước, Vỏ sau)
- Layman's term descriptions in Vietnamese
- Pipe-separated attribute format: {name1:value1|name2:value2|...}

Module: import_export_receipts
"""

import logging
import re
from typing import Dict, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# DIMENSION EXTRACTION
# ============================================================================

# NOTE: Vietnamese naming convention uses W/H/D format (Width/Height-Diameter)
# where the slash between W and H represents aspect ratio, not a separator.
# Examples: 90/90-17 = width 90mm, aspect ratio 90%, diameter 17 inches
# This is different from W/H/H-D (triple slash) which is standardized to W/H-D.


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
    """Extract simple tube size without decimal: WW-D (e.g., 225-17)."""
    pattern = r"\b(\d{2,3})-(\d{2})\b"
    match = re.search(pattern, name)

    if match and not _extract_french_size(name):
        return f"{match.group(1)}-{match.group(2)}"

    return None


def _extract_french_size(name: str) -> Optional[str]:
    """Extract French/bicycle size: DxW (e.g., 27x1.5)."""
    pattern = r"\b(\d+)x(\d+\.?\d*)\b"
    match = re.search(pattern, name)
    if match:
        return f"{match.group(1)}x{match.group(2)}"
    return None


def _extract_american_size(name: str) -> Optional[str]:
    """Extract American tire size: W-D-W (e.g., 300-10-3)."""
    pattern = r"\b(\d+)-(\d+)-(\d+)\b"
    match = re.search(pattern, name)

    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

    return None


def _extract_bolt_pattern(name: str) -> Optional[str]:
    """Extract bolt pattern (e.g., 4-110, 4x110)."""
    pattern = r"\b(\d+)[xX-](\d+)\b"
    match = re.search(pattern, name)

    if match:
        return f"{match.group(1)}x{match.group(2)}"

    return None


def _extract_dimension(name: str) -> Optional[str]:
    """Extract dimension/size from product name."""
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


# ============================================================================
# POSITION DETECTION (Vị trí: Vỏ trước/Vỏ sau)
# ============================================================================


def _extract_position(name: str) -> Optional[str]:
    """Detect tire/tube position from product name.

    Keywords: trước/front, sau/rear, single letter codes

    Returns:
        "Vỏ trước" or "Vỏ sau" or None
    """
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


# ============================================================================
# TIRE TYPE EXTRACTION
# ============================================================================


def _extract_tire_type(name: str) -> Optional[str]:
    """Extract tire type (loại vỏ) from product name.

    Patterns: T/L, TL, TT, TR
    Returns: "Không ruột" (tubeless) or "Có ruột" (tube type)
    """
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


# ============================================================================
# PLY RATING EXTRACTION
# ============================================================================


def _extract_ply_rating(name: str) -> Optional[int]:
    """Extract ply rating from product name.

    Pattern: N PR (e.g., 6PR, 4PR, 8PR)
    """
    match = re.search(r"(\d+)PR", name, re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


# ============================================================================
# LOAD INDEX EXTRACTION
# ============================================================================


def _extract_load_index(name: str) -> Optional[str]:
    """Extract load index from product name.

    Pattern: N P (e.g., 38P, 43L, 50J)
    Excludes PR patterns (ply rating)
    """
    match = re.search(r"(\d+)([A-ZÀ-Ỹ])\b", name)
    if match and "PR" not in match.group(0):
        return f"{match.group(1)}{match.group(2)}"
    return None


# ============================================================================
# REGION CODE EXTRACTION
# ============================================================================


def _extract_region_code(name: str) -> Optional[str]:
    """Extract region code from product name.

    Pattern: -N, -S, -N/S, (N), (S), etc.
    """
    match = re.search(r"-([A-ZÀ-Ỹ]+(?:/[A-ZÀ-Ỹ]+)*)\b", name)
    if match:
        return match.group(1)

    match = re.search(r"\((\s*[A-ZÀ-Ỹ]+(?:,\s*[A-ZÀ-Ỹ]+)\s*)\)", name)
    if match:
        region = match.group(1).replace(", ", "/").replace(",", "/").replace(" ", "")
        return region

    return None


# ============================================================================
# BATTERY ATTRIBUTE EXTRACTION
# ============================================================================


def _extract_battery_code(name: str) -> Optional[str]:
    """Extract battery code (e.g., YTZ5S, WTZ5S, YB5L)."""
    match = re.search(r"\b(YTZ|WTZ|WP|YB\dL|YB\d)\b", name, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return None


def _extract_battery_voltage(name: str) -> Optional[str]:
    """Extract battery voltage (e.g., 12V, 6V)."""
    match = re.search(r"(\d+)[vV]", name)
    if match:
        return f"{match.group(1)}V"
    return None


# ============================================================================
# OIL ATTRIBUTE EXTRACTION
# ============================================================================


def _extract_oil_volume(name: str) -> Optional[str]:
    """Extract oil volume (e.g., 0.8L, 1000ML)."""
    match = re.search(r"(\d+\.?\d*)\s*(ml|l)\b", name, re.IGNORECASE)
    if match:
        volume = match.group(1)
        unit = match.group(2).upper()
        return f"{volume}{unit}"
    return None


def _extract_oil_brand(name: str) -> Optional[str]:
    """Extract oil brand from product name."""
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


# ============================================================================
# BELT ATTRIBUTE EXTRACTION
# ============================================================================


def _extract_belt_length(name: str) -> Optional[str]:
    """Extract belt length (e.g., 150, 156)."""
    match = re.search(
        r"(?:dây\s*curoa|curoa|dây\s*passer).*?(\d{3,4})\b", name, re.IGNORECASE
    )
    if match:
        return match.group(1)
    return None


# ============================================================================
# DIMENSION EXPLANATION (Layman's terms in Vietnamese)
# ============================================================================


def _explain_dimension(dimension: str) -> str:
    """Generate human-readable dimension explanation in Vietnamese.

    Formats:
    - W/H-D (e.g., 90/90-14): rộng 90mm, cao 90mm, đường kính 14 inch
    - W.D-D (e.g., 2.50-17): rộng 2.5 inch, đường kính 17 inch
    - DxW (e.g., 27x1.5): đường kính 27 inch, rộng 1.5 inch
    - WxW (e.g., 16*1.75): đường kính 16 inch, rộng 1.75 inch
    - W1/W2-D (e.g., 2.25/2.50-17): rộng 2.25-2.50 inch, đường kính 17 inch
    - W-D-W (e.g., 300-10-3): rộng 300 inch, đường kính 10 inch, mẫu 3
    - WxW (e.g., 4x110): mẫu 4, đường kính 110mm
    - WW-D (e.g., 225-17): rộng 225 inch, đường kính 17 inch
    """
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


# ============================================================================
# ATTRIBUTE DESCRIPTION GENERATION
# ============================================================================


def _generate_description(attributes: Dict[str, any]) -> str:
    """Generate human-readable description in Vietnamese from attributes.

    Converts technical terms to layman's terms:
    - mm → milimét
    - inch → insơ
    - T/L → Không ruột
    - T/T → Có ruột
    """
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


# ============================================================================
# MAIN EXTRACTION FUNCTION
# ============================================================================


def extract_attributes(name: str) -> str:
    """Extract product attributes (legacy function for backward compatibility).

    Returns pipe-separated attributes string.
    """
    if not name or not isinstance(name, str):
        return ""

    attributes = []

    dimension = _extract_dimension(name)
    if dimension:
        attributes.append(f"Kích thước:{dimension}")

    tire_type = _extract_tire_type(name)
    if tire_type:
        attributes.append(f"Loại vỏ:{tire_type}")

    return "|".join(attributes) if attributes else ""


def extract_attributes_extended(name: str) -> Dict[str, str]:
    """Extract all product attributes and generate description.

    Returns pipe-separated attributes and Vietnamese description.

    Args:
        name: Product name string

    Returns:
        Dict with keys:
            - "Thuộc tính": Pipe-separated attributes (e.g., "Kích thước:80/90-14|Loại vỏ:Không ruột")
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
