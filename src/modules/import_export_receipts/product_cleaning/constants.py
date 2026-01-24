# -*- coding: utf-8 -*-
"""Consolidated constants for product cleaning and classification.

This module provides a single source of truth for all constants used across:
- Brand unification (LOBE → GLOBE, INOU → INOUE)
- Product type classification (Vỏ, Ruột, Nhớt, Bình, Phụ tùng khác)
- Dimension pattern matching (W/H/D, W.D-D, DxW, etc.)
- Vehicle type keywords (Xe máy, Xe đạp, Xe điện, etc.)

All other modules should import constants from here.
"""

# ============================================================================
# BRAND UNIFICATION
# ============================================================================

BRAND_UNIFICATION = {
    "GLOBE": ["GLOBE", "LOBE"],
    "INOUE": ["INU", "INOU"],
    "CHENGSHIN": ["CHENGSHIN", "CHENGSIN"],
    "CAMEL": ["CAMEL", "CHEETAH"],
}

TYPO_CORRECTIONS = {
    "BIAGO": "PIAGIO",
    "YOKO": "YOKOHAMA",
    "MICHENLIN": "MICHELIN",
}

BRAND_CATEGORIES = {
    "TIRE_BRANDS": {
        "CHENGSHIN": ["CHENGSHIN", "CHENGSIN"],
        "MAXXIS": ["MAXXIS", "MAXIS"],
        "INU": ["INU", "INOU", "INOUE"],
        "KENDA": ["KENDA"],
        "MICHELIN": ["MICHELIN", "MICHELLIN", "MICHENLIN"],
        "DUNLOP": ["DUNLOP"],
        "YOKOHAMA": ["YOKOHAMA", "YOKO"],
        "CASUMINA": ["CASUMINA"],
        "CONTINENTAL": ["CONTINENTAL"],
        "PIRELLI": ["PIRELLI"],
        "BRIDGESTONE": ["BRIDGESTONE"],
        "GOODYEAR": ["GOODYEAR"],
        "HANKOOK": ["HANKOOK"],
    },
    "BATTERY_BRANDS": {
        "WAVE": ["WAVE", "WAVe"],
        "DREAM": ["DREAM", "DREAm"],
        "GS": ["GS", "Gs"],
        "TITAN": ["TITAN"],
        "VINFAST": ["VINFAST"],
    },
    "OIL_BRANDS": {
        "YAMAHA": ["YAMAHA"],
        "CASTROL": ["CASTROL"],
        "SHELL": ["SHELL"],
        "MOTUL": ["MOTUL"],
        "TOTAL": ["TOTAL"],
    },
    "PRODUCT_TYPE_KEYWORDS": {
        "Vỏ": ["Vỏ", "Lốp", "VO"],
        "LỐP": ["Lốp", "Lốp"],
        "RUỘT": ["RUỘT", "Săm", "Săm"],
        "SĂM": ["SĂM", "Săm"],
        "BÌNH": ["BÌNH", "Bình", "BÌNH "],
        "NHỚT": ["NHỚT", "Nhớt", "Nhớt", "NHỚT "],
    },
}

# ============================================================================
# DIMENSION PATTERNS (consolidated from 3 sources)
# ============================================================================

# IMPORTANT: Pattern overlap exists! Order is essential for correct matching.
# triple_tire must come BEFORE fractional_tire because "80/90/17"
# would partially match the fractional pattern (\d+)/(\d+) and fail.
# Tube range must also come before decimal_tire for similar reasons.
#
# Fractional motorcycle tire: W/H-D (e.g., 80/90-17)
# 3-part tire: W/H/D (e.g., 80/90/17) - needs to convert to W/H-D
# MUST precede fractional_tire due to pattern overlap
# Decimal motorcycle tire: W.D-D (e.g., 2.50-17)
# Tube range: W1/W2-D (e.g., 2.25/2.50-17)
# MUST precede decimal_tire due to pattern overlap
# French/bicycle size: DxW (e.g., 27x1.5, 27x1-3/8)
# American size: W-D-D (e.g., 1.50-2.50-17) - from product_attributes.py
# Bolt pattern: DxW (e.g., 100x120) - from product_attributes.py

DIMENSION_PATTERNS = {
    "triple_tire": r"(\d+)/(\d+)/(\d+)",
    "fractional_tire": r"(\d+)/(\d+)-(\d+)",
    "tube_range": r"(\d+\.\d+)/(\d+\.\d+)-(\d+)",
    "decimal_tire": r"(\d+\.\d+)-(\d+)",
    "french_size": r"(\d+)x([\d\./\-]+)",
    "american_size": r"(\d+)-(\d+)-(\d+)",
    "bolt_pattern": r"(\d+)[xX-](\d+)",
}

# Additional pattern names for backward compatibility
DIMENSION_PATTERNS["fractional_tire"] = DIMENSION_PATTERNS["fractional_tire"]  # W/H-D
DIMENSION_PATTERNS["triple_tire"] = DIMENSION_PATTERNS["triple_tire"]  # W/H/D → W/H-D
DIMENSION_PATTERNS["decimal_tire"] = DIMENSION_PATTERNS["decimal_tire"]  # W.D-D
DIMENSION_PATTERNS["tube_range"] = DIMENSION_PATTERNS["tube_range"]  # W1/W2-D
DIMENSION_PATTERNS["french_size"] = DIMENSION_PATTERNS["french_size"]  # DxW

# ============================================================================
# PRODUCT TYPE CLASSIFICATION
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

# ============================================================================
# SIMILARITY THRESHOLD
# ============================================================================

SIMILARITY_THRESHOLD = 0.8

# ============================================================================
# SPECIAL CHARACTERS FOR NORMALIZATION
# ============================================================================

SPECIAL_CHARS_PATTERN = r"[-/.*:;()\[\]{}]"
