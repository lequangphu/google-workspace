# -*- coding: utf-8 -*-
"""Refine product master: clean and unify product names across all sources.

This module provides centralized product name cleaning and unification logic:
- Loads all staging files (nhap, xuat, inventory)
- Extracts unique product references from ALL sources
- Applies cleaning/unification once across union of all products
- Updates all files with cleaned/unified values

Used by: orchestrator.py (via -cn and -un flags)
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from src.modules.import_export_receipts.clean_product_names_core import (
    clean_product_name,
    standardize_product_type,
)
from src.modules.import_export_receipts.product_disambiguation import (
    group_similar_names,
    normalize_to_newest_name,
)
from src.utils.staging_cache import StagingCache

logger = logging.getLogger(__name__)


def find_latest_file(directory: Path, pattern: str) -> Optional[Path]:
    """Find the latest file matching a pattern in a directory.

    Args:
        directory: Directory to search
        pattern: Glob pattern (e.g., "*Chi tiết nhập*.csv")

    Returns:
        Path to latest file, or None if no files found
    """
    matching_files = list(directory.glob(pattern))
    if not matching_files:
        return None

    # Sort by modification time, descending
    matching_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return matching_files[0]


def load_staging_files(staging_dir: Path) -> Dict[str, pd.DataFrame]:
    """Load all staging files.

    Returns:
        Dict with keys: 'nhap', 'xuat', 'inventory'
        - 'nhap': DataFrame for Chi tiết nhập
        - 'xuat': DataFrame for Chi tiết xuất
        - 'inventory': List of DataFrames for Xuất nhập tồn files
    """
    # Find latest files matching patterns
    nhap_file = find_latest_file(staging_dir, "*Chi tiết nhập*.csv")
    xuat_file = find_latest_file(staging_dir, "*Chi tiết xuất*.csv")
    inventory_files = list(staging_dir.glob("Xuất nhập tồn *.csv"))

    result = {}

    # Load nhap file
    if nhap_file and nhap_file.exists():
        logger.info(f"Loading: {nhap_file.name}")
        result["nhap"] = StagingCache.get_dataframe(nhap_file)

    # Load xuat file
    if xuat_file and xuat_file.exists():
        logger.info(f"Loading: {xuat_file.name}")
        result["xuat"] = StagingCache.get_dataframe(xuat_file)

    # Load inventory files
    inventory_dfs = []
    for inv_file in inventory_files:
        logger.info(f"Loading: {inv_file.name}")
        try:
            inventory_dfs.append(StagingCache.get_dataframe(inv_file))
        except Exception as e:
            logger.warning(f"Failed to load {inv_file.name}: {e}")

    if inventory_dfs:
        result["inventory"] = inventory_dfs

    return result


def extract_product_references(files: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Extract unique (Mã hàng, Tên hàng) pairs from all files.

    Returns:
        DataFrame with columns: Mã hàng, Tên hàng, _source_file
    """
    product_references = []

    # From nhap
    if "nhap" in files and "Mã hàng" in files["nhap"].columns:
        df = files["nhap"][["Mã hàng", "Tên hàng"]].copy()
        df["_source_file"] = "nhap"
        product_references.append(df.drop_duplicates())

    # From xuat
    if "xuat" in files and "Mã hàng" in files["xuat"].columns:
        df = files["xuat"][["Mã hàng", "Tên hàng"]].copy()
        df["_source_file"] = "xuat"
        product_references.append(df.drop_duplicates())

    # From inventory
    if "inventory" in files:
        for idx, df in enumerate(files["inventory"]):
            if "Mã hàng" in df.columns:
                temp_df = df[["Mã hàng", "Tên hàng"]].copy()
                temp_df["_source_file"] = f"inventory_{idx}"
                product_references.append(temp_df.drop_duplicates())

    if not product_references:
        logger.warning("No product references found in any files")
        return pd.DataFrame(
            columns=["Mã hàng", "Tên hàng", "_source_file", "_original_key"]
        )

    # Combine and deduplicate
    master = pd.concat(product_references, ignore_index=True)
    master["_original_key"] = (
        master["Mã hàng"].astype(str) + "|" + master["Tên hàng"].astype(str)
    )

    return master.drop_duplicates(subset=["Mã hàng", "Tên hàng"])


def clean_product_names_in_master(
    master_df: pd.DataFrame,
) -> pd.DataFrame:
    """Apply name cleaning to master product reference.

    Cleaning order:
    1. Normalize spaces around special characters
    2. Clean dimension format (W/H/D → W/H-D)
    3. Standardize product type (T/L, T/T, PR, region codes)

    Returns:
        DataFrame with cleaned Tên hàng column
    """
    master_df = master_df.copy()

    if "Tên hàng" not in master_df.columns:
        logger.warning("No Tên hàng column in master")
        return master_df

    # Store original for comparison
    if "_original_name" not in master_df.columns:
        master_df["_original_name"] = master_df["Tên hàng"].copy()

    # Apply cleaning
    master_df["Tên hàng"] = master_df["Tên hàng"].apply(
        lambda n: standardize_product_type(clean_product_name(n))
    )

    # Log statistics
    cleaned_count = (master_df["Tên hàng"] != master_df["_original_name"]).sum()

    logger.info(f"Cleaned {cleaned_count} product names")

    return master_df


def unify_product_names_in_master(
    master_df: pd.DataFrame,
    date_col: Optional[str] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Unify names for same product code.

    For each product code:
    1. Get all unique names associated with code
    2. Group similar names (threshold >= 0.8)
    3. If ALL names similar → unify to newest name
    4. If multiple groups → suffix codes (code-01, code-02, etc.)

    Returns:
        (modified DataFrame, statistics dict)
    """
    stats = {
        "codes_processed": 0,
        "unified": 0,
        "suffixed": 0,
        "examples": [],
    }

    if "Mã hàng" not in master_df.columns or "Tên hàng" not in master_df.columns:
        return master_df, stats

    master_df = master_df.copy()

    # For each unique code, check if it has multiple names
    grouped = master_df.groupby("Mã hàng", group_keys=False)

    for code, group in grouped:
        # Filter out NaN values from product names
        unique_names = group["Tên hàng"].dropna().unique().tolist()

        if len(unique_names) <= 1:
            continue

        stats["codes_processed"] += 1

        # Group similar names
        name_groups = group_similar_names(unique_names, threshold=0.8)

        # Check if all names are similar (single group)
        if len(name_groups) == 1 and len(name_groups[0]) == len(unique_names):
            # All similar → unify to newest name
            newest_name = normalize_to_newest_name(
                group, "Tên hàng", date_col or "_source_file"
            )

            # Update all names
            master_df.loc[master_df["Mã hàng"] == code, "Tên hàng"] = newest_name
            stats["unified"] += 1

            stats["examples"].append(
                {
                    "code": code,
                    "original_names": unique_names,
                    "unified_name": newest_name,
                    "action": "unified",
                }
            )

            logger.info(
                f"Unified '{code}': {len(unique_names)} names → '{newest_name[:80]}'"
            )
        else:
            # Multiple groups → suffix codes
            for group_idx, group_indices in enumerate(name_groups):
                if group_idx > 0:  # First group keeps original code
                    suffix_code = f"{code}-{group_idx:02d}"
                    group_names = [unique_names[i] for i in group_indices]

                    # Update code for this group
                    for name in group_names:
                        mask = (master_df["Mã hàng"] == code) & (
                            master_df["Tên hàng"] == name
                        )
                        master_df.loc[mask, "Mã hàng"] = suffix_code

                    stats["suffixed"] += 1
                    stats["examples"].append(
                        {
                            "original_code": code,
                            "new_code": suffix_code,
                            "names": group_names,
                            "action": "suffixed",
                        }
                    )

            logger.info(
                f"Suffixed '{code}': {len(name_groups)} groups → {len(name_groups)} codes"
            )

    return master_df, stats


def apply_mapping_to_df(
    df: pd.DataFrame,
    mapping: Dict[str, Tuple[str, str]],
) -> pd.DataFrame:
    """Apply mapping to a single dataframe.

    Args:
        df: DataFrame to apply mapping to
        mapping: Dict mapping original_key to (final_code, final_name)

    Returns:
        DataFrame with mapped codes and names
    """
    if "Mã hàng" not in df.columns or "Tên hàng" not in df.columns:
        return df

    df = df.copy()

    # Create key
    df["_key"] = df["Mã hàng"].astype(str) + "|" + df["Tên hàng"].astype(str)

    # Map to final code and name
    df["Mã hàng"] = df["_key"].map(lambda k: mapping.get(k, (None, None))[0])
    df["Tên hàng"] = df["_key"].map(lambda k: mapping.get(k, (None, None))[1])

    # Remove temp key
    df.drop(columns=["_key"], inplace=True)

    return df


def apply_mapping_to_files(
    master_df: pd.DataFrame,
    original_keys: pd.DataFrame,
    files: Dict[str, pd.DataFrame],
) -> Dict[str, pd.DataFrame]:
    """Create mapping and apply to all files.

    Args:
        master_df: Processed master with final code/name
        original_keys: Original (code, name) keys with _original_key
        files: Dict of staging dataframes

    Returns:
        Dict of updated dataframes
    """
    # Create mapping from original key to final (code, name)
    mapping = {}
    for idx, row in master_df.iterrows():
        orig_key = row["_original_key"]
        final_code = row["Mã hàng"]
        final_name = row["Tên hàng"]
        mapping[orig_key] = (final_code, final_name)

    # Update each file
    updated_files = {}

    for file_key, df in files.items():
        if file_key == "inventory":
            # List of dataframes
            updated_dfs = []
            for inv_df in df:
                updated_df = apply_mapping_to_df(inv_df, mapping)
                updated_dfs.append(updated_df)
            updated_files[file_key] = updated_dfs
        else:
            # Single dataframe
            updated_df = apply_mapping_to_df(df, mapping)
            updated_files[file_key] = updated_df

    return updated_files


def generate_report(
    files: Dict[str, pd.DataFrame],
    master_before: pd.DataFrame,
    master_after: pd.DataFrame,
    clean_stats: Dict[str, Any],
    unify_stats: Dict[str, Any],
) -> Dict[str, Any]:
    """Generate reconciliation report.

    Args:
        files: Updated staging files
        master_before: Master product reference before processing
        master_after: Master product reference after processing
        clean_stats: Statistics from name cleaning
        unify_stats: Statistics from name unification

    Returns:
        Report dictionary
    """
    report = {
        "staging_files": [],
        "unique_products_before": len(master_before),
        "unique_products_after": len(master_after),
        "names_cleaned": clean_stats,
        "names_unified": unify_stats,
    }

    # Add file info
    if "nhap" in files:
        report["staging_files"].append("chi_tiet_nhap_cleaned.csv")
    if "xuat" in files:
        report["staging_files"].append("chi_tiet_xuat_cleaned.csv")
    if "inventory" in files:
        report["staging_files"].extend(
            [
                f"Xuất nhập tồn refined_{idx}.csv"
                for idx in range(len(files["inventory"]))
            ]
        )

    # Save report
    report_path = (
        Path.cwd()
        / "data"
        / "01-staging"
        / "import_export"
        / "refine_product_master_report.json"
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    logger.info(f"Report saved to: {report_path}")

    return report


def process(
    staging_dir: Optional[Path] = None,
    clean_names: bool = False,
    unify_names: bool = False,
) -> bool:
    """Refine product master across all staging files.

    1. Load all staging files (nhap, xuat, inventory)
    2. Extract unique (Mã hàng, Tên hàng) pairs
    3. Apply name cleaning (if clean_names)
    4. Apply name unification (if unify_names)
    5. Create mapping: (orig_code, orig_name) → (final_code, final_name)
    6. Update all files with cleaned/unified values
    7. Save files back to staging

    Returns:
        True if successful, False otherwise
    """
    logger.info("=" * 70)
    logger.info("REFINING PRODUCT MASTER")
    logger.info("=" * 70)

    # Set defaults
    if staging_dir is None:
        staging_dir = Path.cwd() / "data" / "01-staging" / "import_export"

    if not staging_dir.exists():
        logger.error(f"Staging directory not found: {staging_dir}")
        return False

    # Log what's enabled
    logger.info(f"Product name cleaning: {'ENABLED' if clean_names else 'DISABLED'}")
    logger.info(f"Name unification: {'ENABLED' if unify_names else 'DISABLED'}")

    try:
        # Step 1: Load files
        files = load_staging_files(staging_dir)
        logger.info(f"Loaded {len(files)} staging file groups")

        if not files:
            logger.warning("No staging files found to process")
            return True

        # Step 2: Extract master
        master = extract_product_references(files)
        logger.info(f"Extracted {len(master)} unique product references")

        if master.empty:
            logger.warning("No product references found in staging files")
            return True

        # Store original for comparison
        master_original = master.copy()

        # Step 3: Clean names
        clean_stats = {"enabled": False}
        if clean_names:
            master = clean_product_names_in_master(master)
            clean_stats = {
                "enabled": True,
                "count": len(master),
                "examples": [
                    {"original": row["_original_key"], "cleaned": row["Tên hàng"]}
                    for _, row in master.head(3).iterrows()
                ],
            }

        # Step 4: Unify names
        unify_stats = {"enabled": False}
        if unify_names:
            master, unify_stats = unify_product_names_in_master(master)

        # Step 5: Apply mapping
        updated_files = apply_mapping_to_files(master, master_original, files)

        # Step 6: Save files
        staging_dir.mkdir(parents=True, exist_ok=True)

        # Save nhap
        if "nhap" in updated_files:
            nhap_file = staging_dir / "chi_tiet_nhap_cleaned.csv"
            updated_files["nhap"].to_csv(nhap_file, index=False, encoding="utf-8")
            logger.info(f"Saved: {nhap_file}")

        # Save xuat
        if "xuat" in updated_files:
            xuat_file = staging_dir / "chi_tiet_xuat_cleaned.csv"
            updated_files["xuat"].to_csv(xuat_file, index=False, encoding="utf-8")
            logger.info(f"Saved: {xuat_file}")

        # Save inventory files
        if "inventory" in updated_files:
            for idx, df in enumerate(updated_files["inventory"]):
                inv_file = staging_dir / f"Xuất nhập tồn refined_{idx}.csv"
                df.to_csv(inv_file, index=False, encoding="utf-8")
                logger.info(f"Saved: {inv_file}")

        # Step 7: Generate report
        generate_report(
            updated_files,
            master_original,
            master,
            clean_stats,
            unify_stats,
        )

        logger.info("=" * 70)
        logger.info("PRODUCT MASTER REFINEMENT COMPLETED")
        logger.info("=" * 70)

        return True

    except Exception as e:
        logger.error(f"Error refining product master: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Refine product master: clean names and unify across sources"
    )
    parser.add_argument(
        "--clean-names",
        "-cn",
        action="store_true",
        default=False,
        help="Clean product names (normalize spaces, fix dimensions, standardize format)",
    )
    parser.add_argument(
        "--unify-names",
        "-un",
        action="store_true",
        default=False,
        help="Unify names for same product code (one code = one name)",
    )

    args = parser.parse_args()

    success = process(
        clean_names=args.clean_names,
        unify_names=args.unify_names,
    )

    sys.exit(0 if success else 1)
