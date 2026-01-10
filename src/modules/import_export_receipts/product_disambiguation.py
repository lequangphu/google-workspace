# -*- coding: utf-8 -*-
"""Product name disambiguation using brand unification and pairwise similarity.

This module provides functions to:
1. Unify brand name variations (LOBE → GLOBE, INOU → INOUE)
2. Calculate similarity between product names
3. Group similar names using connected components algorithm
4. Normalize (same product) or suffix (different products) product codes

Used by: clean_receipts_purchase.py, clean_receipts_sale.py
"""

import logging
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

BRAND_UNIFICATION = {
    "GLOBE": ["GLOBE", "LOBE"],
    "INOUE": ["INU", "INOU"],
    "CHENGSHIN": ["CHENGSHIN", "CHENGSIN"],
    "CAMEL": ["CAMEL", "CHEETAH"],
}

SIMILARITY_THRESHOLD = 0.8

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
        "VỐ": ["VỐ", "Vỏ ", "VO"],
        "LỐP": ["LỐP", "Lốp", "Lốp"],
        "RUỘT": ["RUỘT", "Săm", "Săm"],
        "SĂM": ["SĂM", "Săm"],
        "BÌNH": ["BÌNH", "Bình", "BÌNH "],
        "NHỚT": ["NHỚT", "Nhớt", "Nhớt", "NHỚT "],
    },
}


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
        Tuple of (brand_name, category) where category is one of:
        - TIRE_BRANDS
        - BATTERY_BRANDS
        - OIL_BRANDS
        - PRODUCT_TYPE_KEYWORD (if first word is product type, not brand)
        - UNKNOWN
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


def get_similarity(name1: str, name2: str) -> float:
    """Calculate similarity ratio between two names using SequenceMatcher.

    Args:
        name1: First product name
        name2: Second product name

    Returns:
        Similarity ratio between 0.0 and 1.0
    """
    return SequenceMatcher(None, name1.upper(), name2.upper()).ratio()


def check_pairwise_similarity(names: List[str]) -> Dict[Tuple[int, int], float]:
    """Create similarity matrix for all pairs of names.

    Args:
        names: List of unique product names

    Returns:
        Dict mapping (i, j) tuple to similarity score where i < j
    """
    similarity_matrix = {}
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            similarity_matrix[(i, j)] = get_similarity(names[i], names[j])
    return similarity_matrix


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


def get_brand_from_name(name: str) -> str:
    """Extract brand from product name after unification.

    Args:
        name: Product name to extract brand from

    Returns:
        Unified brand name if found, empty string otherwise
    """
    return unify_brand_name(name)


def log_disambiguation_summary(stats: Dict[str, Any]) -> None:
    """Log summary of disambiguation results.

    Args:
        stats: Statistics dict from disambiguate_product_codes
    """
    logger.info("=" * 70)
    logger.info("PRODUCT DISAMBIGUATION SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Codes processed: {stats['codes_processed']}")

    normalized_groups = [
        g for g in stats.get("groups", []) if g.get("action") == "normalized"
    ]
    suffixed_groups = [
        g for g in stats.get("groups", []) if g.get("action") == "suffixed"
    ]

    if normalized_groups:
        logger.info(f"Normalized codes: {len(normalized_groups)}")
        for g in normalized_groups[:5]:
            logger.info(f"  {g['code']} → {g['newest_name']} ({g['name_count']} names)")
        if len(normalized_groups) > 5:
            logger.info(f"  ... and {len(normalized_groups) - 5} more")

    if suffixed_groups:
        logger.info(f"Suffixed codes: {len(suffixed_groups)}")
        for g in suffixed_groups[:5]:
            logger.info(f"  {g['code']} → {g['new_code']} ({g.get('name', 'N/A')})")
        if len(suffixed_groups) > 5:
            logger.info(f"  ... and {len(suffixed_groups) - 5} more")

    logger.info("=" * 70)
