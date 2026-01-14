# -*- coding: utf-8 -*-
"""Clean inventory data (Xuất Nhập Tồn) from raw CSV files.

This module:
1. Groups files by header structure
2. Extracts and combines multi-level headers handling Vietnamese characters
3. Loads data with year/month metadata from filename
4. Drops rows with empty product codes or no inventory quantities
5. Removes columns with <90% non-null coverage
6. Converts numeric columns (handles comma-separated values)
7. Calculates profit margin (Biên lãi gộp) from revenue and gross profit
8. Standardizes columns and exports cleaned data

Raw source: Inventory files (Xuất Nhập Tồn) from KiotViet
"""

import csv
import json
import logging
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from tqdm import tqdm

# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG = {
    "file_pattern": r"(\d{4})_(\d{1,2})_XNT\.csv",
    "file_suffix": "XNT.csv",
    "min_non_null_percentage": 90,
    "skiprows": 5,
    "date_format": "%d-%m-%Y",
    "columns_to_convert": [
        "TỒN_ĐẦU_KỲ_Đ_GIÁ",
        "TỒN_CUỐI_KỲ_THÀNH_TIỀN",
        "TỒN_CUỐI_KỲ_DOANH_THU",
        "TỒN_CUỐI_KỲ_LÃI_GỘP",
        "CHI_PHÍ_TIỀN",
    ],
    "column_rename_map": {
        "Mã_SP": "Mã hàng",
        "TÊN_HÀNG": "Tên hàng",
        "TỒN_ĐẦU_KỲ_S_LƯỢNG": "Số lượng đầu kỳ",
        "TỒN_ĐẦU_KỲ_Đ_GIÁ": "Đơn giá đầu kỳ",
        "TỒN_ĐẦU_KỲ_THÀNH_TIỀN": "Thành tiền đầu kỳ",
        "NHẬP_TRONG_KỲ_S_LƯỢNG": "Số lượng nhập trong kỳ",
        "NHẬP_TRONG_KỲ_Đ_GIÁ": "Đơn giá nhập trong kỳ",
        "NHẬP_TRONG_KỲ_THÀNH_TIỀN": "Thành tiền nhập trong kỳ",
        "XUẤT_TRONG_KỲ_SỐ_LƯỢNG": "Số lượng xuất trong kỳ",
        "XUẤT_TRONG_KỲ": "Xuất trong kỳ",
        "XUẤT_TRONG_KỲ_Đ_GIÁ": "Đơn giá xuất trong kỳ",
        "XUẤT_TRONG_KỲ_THÀNH_TIỀN": "Thành tiền xuất trong kỳ",
        "TỒN_CUỐI_KỲ_S_LƯỢNG": "Số lượng cuối kỳ",
        "TỒN_CUỐI_KỲ_Đ_GIÁ": "Đơn giá cuối kỳ",
        "TỒN_CUỐI_KỲ_THÀNH_TIỀN": "Thành tiền cuối kỳ",
        "TỒN_CUỐI_KỲ_DOANH_THU": "Doanh thu cuối kỳ",
        "TỒN_CUỐI_KỲ_LÃI_GỘP": "Lãi gộp cuối kỳ",
        "CHI_PHÍ_DIỄN_GIẢI": "Tên chi phí",
        "CHI_PHÍ_TIỀN": "Thành tiền chi phí",
        "NGÀY": "Ngày",
    },
    "columns_to_drop_from_output": [
        "Đơn giá đầu kỳ",
        "Đơn giá nhập trong kỳ",
        "Đơn giá xuất trong kỳ",
        "Đơn giá cuối kỳ",
        "Biên lãi gộp",
    ],
}
    "columns_to_convert": [
        "TỒN_ĐẦU_KỲ_Đ_GIÁ",
        "TỒN_CUỐI_KỲ_THÀNH_TIỀN",
        "TỒN_CUỐI_KỲ_DOANH_THU",
        "TỒN_CUỐI_KỲ_LÃI_GỘP",
        "CHI_PHÍ_TIỀN",
    ],
    "column_rename_map": {
        "Mã_SP": "Mã hàng",
        "TÊN_HÀNG": "Tên hàng",
        "TỒN_ĐẦU_KỲ_S_LƯỢNG": "Số lượng đầu kỳ",
        "TỒN_ĐẦU_KỲ_Đ_GIÁ": "Đơn giá đầu kỳ",
        "TỒN_ĐẦU_KỲ_THÀNH_TIỀN": "Thành tiền đầu kỳ",
        "NHẬP_TRONG_KỲ_S_LƯỢNG": "Số lượng nhập trong kỳ",
        "NHẬP_TRONG_KỲ_Đ_GIÁ": "Đơn giá nhập trong kỳ",
        "NHẬP_TRONG_KỲ_THÀNH_TIỀN": "Thành tiền nhập trong kỳ",
        "XUẤT_TRONG_KỲ_SỐ_LƯỢNG": "Số lượng xuất trong kỳ",
        "XUẤT_TRONG_KỲ": "Xuất trong kỳ",
        "XUẤT_TRONG_KỲ_Đ_GIÁ": "Đơn giá xuất trong kỳ",
        "XUẤT_TRONG_KỲ_THÀNH_TIỀN": "Thành tiền xuất trong kỳ",
        "TỒN_CUỐI_KỲ_S_LƯỢNG": "Số lượng cuối kỳ",
        "TỒN_CUỐI_KỲ_Đ_GIÁ": "Đơn giá cuối kỳ",
        "TỒN_CUỐI_KỲ_THÀNH_TIỀN": "Thành tiền cuối kỳ",
        "TỒN_CUỐI_KỲ_DOANH_THU": "Doanh thu cuối kỳ",
        "TỒN_CUỐI_KỲ_LÃI_GỘP": "Lãi gộp cuối kỳ",
        "CHI_PHÍ_DIỄN_GIẢI": "Tên chi phí",
        "CHI_PHÍ_TIỀN": "Thành tiền chi phí",
        "NGÀY": "Ngày",
    },
    "numeric_cols": [
        "Tồn đầu kỳ",
        "Giá trị đầu kỳ",
        "Số lượng nhập",
        "Giá trị nhập",
        "Số lượng xuất",
        "Giá trị xuất",
        "Tồn cuối kỳ",
        "Giá trị cuối kỳ",
        "Doanh thu cuối kỳ",
        "Thành tiền",
    ],
    "columns_to_drop_from_output": [
        "Đơn giá đầu kỳ",
        "Đơn giá nhập trong kỳ",
        "Đơn giá xuất trong kỳ",
        "Đơn giá cuối kỳ",
        "Biên lãi gộp",
    ],
    "column_rename_final": {
        "Số lượng đầu kỳ": "Tồn đầu kỳ",
        "Số lượng cuối kỳ": "Tồn cuối kỳ",
        "Thành tiền đầu kỳ": "Giá trị đầu kỳ",
        "Thành tiền cuối kỳ": "Giá trị cuối kỳ",
        "Số lượng nhập trong kỳ": "Số lượng nhập",
        "Thành tiền nhập trong kỳ": "Giá trị nhập",
        "Số lượng xuất trong kỳ": "Số lượng xuất",
        "Thành tiền xuất trong kỳ": "Giá trị xuất",
        "Thành tiền chi phí": "Thành tiền"
    },
    "columns_to_drop_from_output": [
        "Đơn giá đầu kỳ",
        "Đơn giá nhập trong kỳ",
        "Đơn giá xuất trong kỳ",
        "Đơn giá cuối kỳ",
        "Biên lãi gộp",
    ],
}

# ============================================================================
# LOGGING SETUP
# ============================================================================

logger = logging.getLogger(__name__)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def combine_headers(header_row_1: List[str], header_row_2: List[str]) -> List[str]:
    """Combine two header rows into a single list of clean column names.

    Handles parent-child header relationships, empty cells, and duplicates.
    Preserves Vietnamese characters.

    Args:
        header_row_1: First header row (parent level)
        header_row_2: Second header row (child level)

    Returns:
        List of combined, cleaned column names
    """
    combined_names = []
    spanning_parent_header = ""
    name_counts = {}

    max_len = max(len(header_row_1), len(header_row_2))
    h1_padded = list(header_row_1) + [""] * (max_len - len(header_row_1))
    h2_padded = list(header_row_2) + [""] * (max_len - len(header_row_2))

    for i, (h1_val, h2_val) in enumerate(zip(h1_padded, h2_padded)):
        current_col_name = ""

        if h1_val and h1_val.strip() != "":
            spanning_parent_header = h1_val.strip()

        if h2_val and h2_val.strip() != "":
            if spanning_parent_header:
                current_col_name = f"{spanning_parent_header}_{h2_val.strip()}"
            else:
                current_col_name = h2_val.strip()
        elif spanning_parent_header:
            current_col_name = spanning_parent_header
        else:
            current_col_name = f"Unnamed_{i}"

        current_col_name = current_col_name.strip()
        current_col_name = current_col_name.replace(" ", "_")

        # Regex to keep alphanumeric, underscore, and Vietnamese characters
        current_col_name = re.sub(
            r"[^\wÁÀẢẠÃĂẰẮẲẶẶẬẤẦẨẪẬẬÉÈẺẼẸÊẾỀỂỄỆÍÌỈĨỊÓÒỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÚÙỦŨỤƯỨỪỬỮỰÝỲỶỸỴĐđ]",
            "_",
            current_col_name,
            flags=re.UNICODE,
        )
        current_col_name = re.sub(r"_{2,}", "_", current_col_name)
        current_col_name = current_col_name.strip("_")
        if not current_col_name:
            current_col_name = f"Unnamed_{i}"

        # Handle duplicates
        original_name = current_col_name
        count = name_counts.get(current_col_name, 0)
        temp_col_name = current_col_name
        while temp_col_name in combined_names:
            count += 1
            temp_col_name = f"{original_name}_{count}"
        name_counts[original_name] = count
        combined_names.append(temp_col_name)

    return combined_names


def extract_date_from_filename(filename: str) -> Optional[Tuple[str, str, str]]:
    """Extract year, month, and formatted date from filename.

    Args:
        filename: Filename matching pattern YYYY_M_XNT.csv

    Returns:
        Tuple of (year, month_padded, date_string) or None if pattern not matched
    """
    match = re.match(CONFIG["file_pattern"], filename)
    if match:
        year = match.group(1)
        month = match.group(2).zfill(2)
        date_string = f"01-{month}-{year}"
        return year, month, date_string
    return None


# ============================================================================
# MAIN PROCESSING FUNCTIONS
# ============================================================================


def find_input_files(input_dir: Path) -> List[Path]:
    """Find all XNT.csv files in the input directory.

    Args:
        input_dir: Directory to search for files

    Returns:
        List of file paths

    Raises:
        FileNotFoundError: If no XNT.csv files are found
    """
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    xnt_files = sorted(input_dir.glob(f"*{CONFIG['file_suffix']}"))

    if not xnt_files:
        raise FileNotFoundError(
            f"No files ending with '{CONFIG['file_suffix']}' found in '{input_dir}'"
        )

    logger.info(f"Found {len(xnt_files)} input files")
    return xnt_files


def group_files_by_headers(file_paths: List[Path]) -> Dict[Tuple, List[Path]]:
    """Group files by their header structure.

    Args:
        file_paths: List of file paths to process

    Returns:
        Dict mapping header tuples to lists of file paths
    """
    header_groups = {}
    errors = []

    for file_path in tqdm(file_paths, desc="Reading headers"):
        try:
            with open(file_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                rows = []
                for i, row in enumerate(reader):
                    rows.append(row)
                    if i == 3:
                        break

                if len(rows) > 3:
                    header_row_3 = list(rows[2])
                    header_row_4 = list(rows[3])
                    combined_column_names = combine_headers(header_row_3, header_row_4)
                    combined_header_key = tuple(combined_column_names)

                    if combined_header_key not in header_groups:
                        header_groups[combined_header_key] = []
                    header_groups[combined_header_key].append(file_path)
                else:
                    errors.append((file_path.name, "Insufficient header rows"))
        except Exception as e:
            errors.append((file_path.name, str(e)))

    if errors:
        logger.warning(f"Failed to read headers from {len(errors)} file(s)")
        for filename, error in errors:
            logger.debug(f"  {filename}: {error}")

    logger.info(f"Grouped files into {len(header_groups)} header groups")
    return header_groups


def load_and_process_group(
    combined_header_key: Tuple,
    file_paths: List[Path],
) -> Optional[pd.DataFrame]:
    """Load and consolidate dataframes for a group of files with matching headers.

    Args:
        combined_header_key: The header structure key for this group
        file_paths: List of file paths in this group

    Returns:
        Consolidated DataFrame or None if no valid data
    """
    group_dataframes = []
    errors = []
    output_row_idx = 0

    for file_path in file_paths:
        try:
            filename = file_path.name
            date_info = extract_date_from_filename(filename)

            if not date_info:
                errors.append((filename, "Could not extract date from filename"))
                continue

            year, month, ngay_value = date_info

            df = pd.read_csv(
                file_path,
                skiprows=CONFIG["skiprows"],
                header=None,
                encoding="utf-8",
                engine="python",
                on_bad_lines="skip",
            )

            # Align columns with header
            if df.shape[1] > len(combined_header_key):
                df = df.iloc[:, : len(combined_header_key)]
            elif df.shape[1] < len(combined_header_key):
                for col_idx in range(df.shape[1], len(combined_header_key)):
                    df[f"Unnamed_missing_{col_idx}"] = pd.NA

            df.columns = combined_header_key
            df["Năm"] = year
            df["Tháng"] = month
            df["NGÀY"] = ngay_value
            df["NGÀY"] = pd.to_datetime(
                df["NGÀY"], errors="coerce", format=CONFIG["date_format"]
            )
            output_row_idx += len(df)
            group_dataframes.append(df)

        except Exception as e:
            errors.append((file_path.name, str(e)))

    if errors:
        logger.warning(f"Failed to load {len(errors)} file(s) from group")
        for filename, error in errors:
            logger.debug(f"  {filename}: {error}")

    if group_dataframes:
        return pd.concat(group_dataframes, ignore_index=True)
    return None


def consolidate_files(
    header_groups: Dict[Tuple, List[Path]],
) -> Dict[str, pd.DataFrame]:
    """Consolidate all file groups into dataframes.

    Args:
        header_groups: Dict mapping headers to file paths

    Returns:
        Dict mapping group names to consolidated DataFrames
    """
    consolidated_dataframes = {}
    group_idx = 0

    for combined_header_key, file_paths in tqdm(
        header_groups.items(), desc="Processing groups"
    ):
        df = load_and_process_group(combined_header_key, file_paths)
        if df is not None:
            consolidated_dataframes[f"Group_{group_idx}"] = df
        group_idx += 1

    logger.info(f"Created {len(consolidated_dataframes)} consolidated group(s)")
    return consolidated_dataframes


def clean_data(
    consolidated_dataframes: Dict[str, pd.DataFrame],
) -> Dict[str, pd.DataFrame]:
    """Apply cleaning steps to each consolidated dataframe.

    Args:
        consolidated_dataframes: Dict of group name to DataFrame

    Returns:
        Dict of cleaned DataFrames with statistics
    """
    stats = {
        "rows_dropped_empty_code": 0,
        "columns_dropped": {},
        "rows_dropped_empty_qty": 0,
    }

    for group_name, df in list(consolidated_dataframes.items()):
        # Drop rows with empty product codes
        if "Mã_SP" in df.columns:
            df["Mã_SP"] = df["Mã_SP"].astype(str)
            rows_before = len(df)
            df_cleaned = df[df["Mã_SP"].str.strip() != ""]
            df_cleaned = df_cleaned[df_cleaned["Mã_SP"] != "nan"]
            rows_dropped = rows_before - len(df_cleaned)
            stats["rows_dropped_empty_code"] += rows_dropped
            consolidated_dataframes[group_name] = df_cleaned

        df = consolidated_dataframes[group_name]

        # Drop columns with low non-null coverage
        non_null_percentage = df.notna().sum() / len(df) * 100
        columns_to_drop = non_null_percentage[
            non_null_percentage < CONFIG["min_non_null_percentage"]
        ].index.tolist()

        # Preserve critical columns
        if "NGÀY" in columns_to_drop:
            columns_to_drop.remove("NGÀY")

        if columns_to_drop:
            stats["columns_dropped"][group_name] = columns_to_drop
            consolidated_dataframes[group_name] = df.drop(columns=columns_to_drop)

    logger.info(
        f"Dropped {stats['rows_dropped_empty_code']} rows with empty product codes"
    )
    logger.info(f"Dropped columns from {len(stats['columns_dropped'])} group(s)")

    return consolidated_dataframes


def extract_cost_data(consolidated_dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Extract cost rows (CHI PHÍ) into a separate DataFrame.

    Extracts rows where 'CHI_PHÍ_DIỄN_GIẢI' has a value before they're dropped
    due to empty product codes.

    Args:
        consolidated_dataframes: Dict of group name to DataFrame

    Returns:
        DataFrame with cost data: Năm, Tháng, Tên chi phí, Thành tiền chi phí
    """
    cost_rows = []

    for group_name, df in consolidated_dataframes.items():
        if "CHI_PHÍ_DIỄN_GIẢI" not in df.columns:
            continue

        # Filter rows where Tên chi phí has a value
        df_costs = df[
            df["CHI_PHÍ_DIỄN_GIẢI"].notna()
            & (df["CHI_PHÍ_DIỄN_GIẢI"].astype(str).str.strip() != "")
        ].copy()

        if df_costs.empty:
            continue

        # Select and rename columns
        df_costs = df_costs.rename(
            columns={
                "CHI_PHÍ_DIỄN_GIẢI": "Tên chi phí",
                "CHI_PHÍ_TIỀN": "Thành tiền chi phí",
            }
        )

        # Keep only required columns
        available_cols = ["Năm", "Tháng", "Tên chi phí", "Thành tiền chi phí"]
        available_cols = [col for col in available_cols if col in df_costs.columns]
        df_costs = df_costs[available_cols].copy()

        # Ensure numeric type for Thành tiền chi phí
        if "Thành tiền chi phí" in df_costs.columns:
            df_costs["Thành tiền chi phí"] = pd.to_numeric(
                df_costs["Thành tiền chi phí"]
                .astype(str)
                .str.replace(",", "", regex=False),
                errors="coerce",
            ).fillna(0)

        cost_rows.append(df_costs)

    if not cost_rows:
        logger.info("No cost data found in input files")
        return pd.DataFrame()

    cost_df = pd.concat(cost_rows, ignore_index=True)
    logger.info(f"Extracted {len(cost_df)} cost rows from input files")

    # Sum duplicates if they exist
    if cost_df.duplicated(subset=["Tên chi phí"], keep=False).any():
        cost_df = cost_df.groupby(["Năm", "Tháng", "Tên chi phí"], as_index=False)[
            "Thành tiền chi phí"
        ].sum()
        logger.info(f"Aggregated to {len(cost_df)} unique cost entries")

    return cost_df


def merge_and_refine(consolidated_dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Merge all consolidated dataframes and apply final refinements.

    Args:
        consolidated_dataframes: Dict of DataFrames

    Returns:
        Final refined DataFrame
    """
    all_dfs = list(consolidated_dataframes.values())
    if not all_dfs:
        logger.warning("No data to merge")
        return pd.DataFrame()

    final_df = pd.concat(all_dfs, ignore_index=True)
    logger.info(f"Merged into final DataFrame with {len(final_df)} rows")

    # Convert numeric columns
    for col in CONFIG["columns_to_convert"]:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(
                final_df[col].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            )

    # Rename columns
    final_df = final_df.rename(columns=CONFIG["column_rename_map"])

    # Drop redundant column
    if "Xuất trong kỳ" in final_df.columns:
        final_df = final_df.drop(columns=["Xuất trong kỳ"])
        logger.info("Dropped redundant 'Xuất trong kỳ' column")

    # Drop rows with no inventory
    so_luong_cols = [
        col
        for col in final_df.columns
        if col.startswith("Số lượng") or col.startswith("Tồn")
    ]
    if so_luong_cols:
        for col in so_luong_cols:
            final_df[col] = pd.to_numeric(final_df[col], errors="coerce")

        mask_empty = final_df[so_luong_cols].isna().all(axis=1) | (
            final_df[so_luong_cols] == 0
        ).all(axis=1)
        rows_dropped = mask_empty.sum()
        final_df = final_df[~mask_empty]
        if rows_dropped > 0:
            logger.info(f"Dropped {rows_dropped} rows with no inventory")

    columns_to_drop = CONFIG.get("columns_to_drop_from_output", [])
    columns_to_drop_existing = [
        col for col in columns_to_drop if col in final_df.columns
    ]
    if columns_to_drop_existing:
        final_df = final_df.drop(columns=columns_to_drop_existing)
        logger.info(
            f"Dropped {len(columns_to_drop_existing)} columns from output: {columns_to_drop_existing}"
        )

    column_rename_map = CONFIG.get("column_rename_final", {})
    if column_rename_map:
        final_df = final_df.rename(columns=column_rename_map)
        logger.info(f"Renamed columns: {column_rename_map}")

    return final_df


def format_columns(final_df: pd.DataFrame) -> pd.DataFrame:
    """Format and convert column data types.

    Args:
        final_df: DataFrame to format

    Returns:
        Formatted DataFrame
    """
    # Text columns
    if "Mã hàng" in final_df.columns:
        final_df["Mã hàng"] = final_df["Mã hàng"].astype(str).str.upper()
    if "Tên hàng" in final_df.columns:
        # Clean: strip leading/trailing spaces, collapse multiple spaces to single space
        final_df["Tên hàng"] = (
            final_df["Tên hàng"]
            .astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
        )

    # Numeric columns
    for col in CONFIG["numeric_cols"]:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors="coerce")

    # Integer columns
    for col in ["Tháng", "Năm"]:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors="coerce").astype(
                "Int64"
            )

    # Sort
    if "Ngày" in final_df.columns and "Mã hàng" in final_df.columns:
        final_df["Ngày"] = pd.to_datetime(final_df["Ngày"], errors="coerce")
        final_df = final_df.sort_values(by=["Ngày", "Mã hàng"], na_position="last")

    return final_df


def create_reconciliation_checkpoint(
    input_dir: Path,
    output_filepath: Path,
    script_name: str = "clean_inventory",
) -> Dict[str, Any]:
    """Reconcile input vs output quantities with detailed breakdown.

    Args:
        input_dir: Path to raw import_export directory
        output_filepath: Path to output CSV file
        script_name: Name of script for report identification

    Returns:
        dict: Reconciliation report with input/output/dropout by file
    """
    # Step 1: Calculate INPUT totals from raw files
    input_row_counts = defaultdict(int)

    for csv_file in input_dir.glob("*XNT.csv"):
        try:
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                rows = list(reader)

            # Skip header rows (first 5 rows)
            for row_idx, row in enumerate(rows[5:], start=1):
                input_row_counts[csv_file.name] += 1
                # Count rows
        except Exception as e:
            logger.warning(f"Error reading {csv_file.name} for reconciliation: {e}")

    # Step 2: Calculate OUTPUT totals from staging file
    output_df = pd.read_csv(output_filepath)
    output_row_count = len(output_df)

    # Step 3: Build reconciliation report
    total_input_rows = sum(input_row_counts.values())

    report = {
        "timestamp": datetime.now().isoformat(),
        "script": script_name,
        "input": {
            "total_rows": int(total_input_rows),
        },
        "output": {
            "total_rows": int(output_row_count),
        },
        "reconciliation": {
            "row_dropout_pct": (
                (total_input_rows - output_row_count) / total_input_rows * 100
                if total_input_rows > 0
                else 0
            ),
        },
        "alerts": [],
    }

    # Step 5: Flag issues

    # Step 6: Save reconciliation report
    report_filename = f"reconciliation_report_{script_name}.json"
    report_path = output_filepath.parent / report_filename
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # Step 7: Log report
    logger.info("=" * 70)
    logger.info(f"RECONCILIATION REPORT ({script_name})")
    logger.info(f"Input:  {total_input_rows:,} rows")
    logger.info(f"Output: {output_row_count:,} rows")
    logger.info(f"Dropout: {report['reconciliation']['row_dropout_pct']:.1f}%")
    logger.info(f"Report saved to: {report_filename}")
    for alert in report["alerts"]:
        logger.warning(alert)
    logger.info("=" * 70)

    return report


def process(
    input_dir: Path,
    staging_dir: Path,
) -> Optional[Path]:
    """Process inventory files from input_dir and save to staging_dir.

    Args:
        input_dir: Directory containing raw XNT CSV files
        staging_dir: Directory to save staged data

    Returns:
        Path to output file or None if processing failed
    """
    staging_dir.mkdir(parents=True, exist_ok=True)

    logger.info("=" * 70)
    logger.info("STARTING INVENTORY (XUẤT NHẬP TỒN) PROCESSING")
    logger.info("=" * 70)

    try:
        # Load input files
        file_paths = find_input_files(input_dir)

        # Group by headers
        header_groups = group_files_by_headers(file_paths)

        # Consolidate groups
        consolidated_dataframes = consolidate_files(header_groups)

        if not consolidated_dataframes:
            logger.error("No data could be processed")
            return None

        # Extract cost data before cleaning drops rows with empty product codes
        cost_df = extract_cost_data(consolidated_dataframes)

        # Clean data
        consolidated_dataframes = clean_data(consolidated_dataframes)

        # Merge and refine
        final_df = merge_and_refine(consolidated_dataframes)

        if final_df.empty:
            logger.error("Final DataFrame is empty after processing")
            return None

        # Format columns
        final_df = format_columns(final_df)

        # Determine output filename from data
        if "Ngày" not in final_df.columns:
            logger.warning("'Ngày' column missing")
            output_filename = "Xuất nhập tồn.csv"
        else:
            final_df_valid = final_df.dropna(subset=["Ngày"])
            if final_df_valid.empty:
                output_filename = "Xuất nhập tồn.csv"
            else:
                min_date = final_df_valid["Ngày"].min()
                max_date = final_df_valid["Ngày"].max()

                first_year = min_date.year
                first_month = str(min_date.month).zfill(2)
                last_year = max_date.year
                last_month = str(max_date.month).zfill(2)

                output_filename = f"Xuất nhập tồn {first_year}_{first_month}_{last_year}_{last_month}.csv"

        output_filepath = staging_dir / output_filename
        final_df.to_csv(output_filepath, index=False, encoding="utf-8")
        logger.info(f"Saved output to: {output_filepath}")

        # Print quality report
        logger.info("=" * 70)
        logger.info("DATA QUALITY REPORT")
        logger.info("=" * 70)
        logger.info(f"Total rows: {len(final_df)}")
        logger.info(f"Total columns: {len(final_df.columns)}")

        # Null value summary
        null_summary = final_df.isnull().sum()
        null_summary = null_summary[null_summary > 0]
        if not null_summary.empty:
            logger.info("Columns with null values:")
            for col, count in null_summary.items():
                pct = (count / len(final_df)) * 100
                logger.info(f"  {col}: {count} ({pct:.1f}%)")

        logger.info("=" * 70)

        create_reconciliation_checkpoint(input_dir, output_filepath, "clean_inventory")

        financial_path = create_financial_report(cost_df, staging_dir)
        if financial_path:
            logger.info(f"Financial report ready for upload: {financial_path}")

        logger.info("=" * 70)
        logger.info("INVENTORY PROCESSING COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)

        return output_filepath

    except Exception as e:
        logger.error(f"Inventory processing pipeline failed: {str(e)}", exc_info=True)
        raise


def create_financial_report(
    cost_df: pd.DataFrame,
    output_dir: Path,
) -> Optional[Path]:
    """Create 'Chi tiết chi phí' (Cost Detail) report from extracted cost data.

    Args:
        cost_df: DataFrame from extract_cost_data() with columns:
            Năm, Tháng, Tên chi phí, Thành tiền chi phí
        output_dir: Directory to save report

    Returns:
        Path to report CSV or None if creation failed
    """
    logger.info("=" * 70)
    logger.info("CREATING COST DETAIL REPORT (CHI TIẾT CHI PHÍ)")
    logger.info("=" * 70)

    if cost_df.empty:
        logger.warning("Cost data is empty, skipping report")
        return None

    # Generate filename with date range
    cost_df_valid = cost_df.dropna(subset=["Năm", "Tháng"]).copy()
    if not cost_df_valid.empty:
        # Create date column to find actual min/max (year, month) pairs
        cost_df_valid["sort_key"] = cost_df_valid["Năm"] * 100 + cost_df_valid["Tháng"]
        min_idx = cost_df_valid["sort_key"].idxmin()
        max_idx = cost_df_valid["sort_key"].idxmax()

        first_year = int(cost_df_valid.loc[min_idx, "Năm"])
        first_month = int(cost_df_valid.loc[min_idx, "Tháng"])
        last_year = int(cost_df_valid.loc[max_idx, "Năm"])
        last_month = int(cost_df_valid.loc[max_idx, "Tháng"])

        month_names = [
            "",
            "01",
            "02",
            "03",
            "04",
            "05",
            "06",
            "07",
            "08",
            "09",
            "10",
            "11",
            "12",
        ]
        last_month_name = month_names[last_month]
        filename = (
            f"Chi tiết chi phí "
            f"{first_year}_{first_month:02d}_{last_year}_{last_month_name}.csv"
        )
    else:
        filename = "Chi tiết chi phí.csv"

    filepath = output_dir / filename
    cost_df.to_csv(filepath, index=False, encoding="utf-8")

    logger.info(f"✅ Created cost report: {filepath}")
    logger.info(f"   Rows: {len(cost_df)}")

    return filepath


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    input_dir = Path.cwd() / "data" / "00-raw" / "import_export"
    staging_dir = Path.cwd() / "data" / "01-staging" / "import_export"
    process(input_dir, staging_dir)
