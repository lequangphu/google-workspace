# -*- coding: utf-8 -*-
"""Clean purchase receipt data (Chứng từ nhập) from CSV files.

Module: import_export_receipts
Raw source: XUẤT NHẬP TỒN TỔNG T* (CT.NHAP sheet from Google Drive)
Pipeline stage: data/00-raw/ → data/01-staging/
Output: Cleaned purchase receipt details for Products/PriceBook extraction

This script:
1. Loads CSV files with multi-level headers from data/00-raw/
2. Extracts and combines headers
3. Parses dates robustly (handles Excel dates, multiple formats, ambiguities)
4. Validates dates against source year/month
5. Standardizes columns and exports cleaned data to data/01-staging/
"""

import csv
import logging
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ============================================================================
# CONFIGURATION
# ============================================================================

# Data folder paths (from data/README.md: ingest → staging)
DATA_RAW_DIR = Path.cwd() / "data" / "00-raw" / "import_export"
DATA_STAGING_DIR = Path.cwd() / "data" / "01-staging" / "import_export"

# Column mappings: specific indices for NHAP structure
HEADER_COLUMN_MAP = [
    ((0, 0, 1, 2), "Chứng từ nhập"),
    ((0, 1, 1, 2), "Chứng từ nhập"),
    ((0, 2, 1, 2), "Chứng từ nhập"),
    (3, "Nhà CC"),
    (4, "Người mua"),
    (5, "Mã HH"),
    (6, "Chủng loại"),
    (7, "ĐVT"),
    ((8, 8, 9, 10, 14, 15), "Số lượng"),
    (22, "Ghi chú"),
    (23, "Đơn giá nhập"),
    (24, "Thành tiền"),
    (25, "GHI CHÚ"),
    ((26, 26, 27, 28), "Thời hạn bảo hành"),
]

# Columns to drop per group
COLUMNS_TO_DROP = {
    "common": [
        "Chứng từ nhập_PXH",
        "Người mua",
        "ĐVT",
        "Số lượng_Kho 2",
        "Số lượng_Kho 3",
        "Số lượng_Asc",
        "Số lượng_Đào Khánh",
        "Ghi chú",
        "Thời hạn bảo hành_Thời gian",
        "Thời hạn bảo hành_Hết hạn",
        "Thời hạn bảo hành_Gia hạn",
    ],
    "group_2_specific": ["Nhà SX", "GHI CHÚ"],
}

# Column rename mapping (normalize to KiotViet column names)
RENAME_MAPPING = {
    "Chứng từ nhập_PNK": "Mã chứng từ",
    "Chứng từ nhập_Ngày": "Ngày",
    "Nhà CC": "Tên nhà cung cấp",
    "Mã HH": "Mã hàng",
    "Chủng loại": "Tên hàng",
    "Số lượng_Kho 1": "Số lượng",
    "Đơn giá nhập": "Đơn giá",
    "Thành tiền": "Thành tiền",
}

# Final column order (for output)
COLUMN_ORDER = [
    "Mã hàng",
    "Tên hàng",
    "Số lượng",
    "Đơn giá",
    "Thành tiền",
    "Ngày",
    "Tháng",
    "Năm",
    "Mã chứng từ",
    "Tên nhà cung cấp",
]

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def combine_headers(
    header_row1: List[str], header_row2: List[str]
) -> Tuple[List[str], List[int]]:
    """Combine two header rows into a single standardized header list.

    Args:
        header_row1: Primary header row
        header_row2: Secondary header row

    Returns:
        tuple: (combined_headers, original_indices)
    """

    def normalize_header(h: str) -> str:
        """Strip and replace internal whitespace."""
        return re.sub(r"\s+", " ", h.strip())

    final_combined_headers = [
        f"{normalize_header(header_row1[0])}_{normalize_header(header_row2[0])}",
        f"{normalize_header(header_row1[0])}_{normalize_header(header_row2[1])}",
        f"{normalize_header(header_row1[0])}_{normalize_header(header_row2[2])}",
        normalize_header(header_row2[3]),
        normalize_header(header_row1[4]),
        normalize_header(header_row1[5]),
        normalize_header(header_row1[6]),
        normalize_header(header_row1[7]),
        f"{normalize_header(header_row1[8])}_{normalize_header(header_row2[8])}",
        f"{normalize_header(header_row1[8])}_{normalize_header(header_row2[9])}",
        f"{normalize_header(header_row1[8])}_{normalize_header(header_row2[10])}",
        f"{normalize_header(header_row1[8])}_{normalize_header(header_row2[14])}",
        f"{normalize_header(header_row1[8])}_{normalize_header(header_row2[15])}",
        normalize_header(header_row1[22]),
        normalize_header(header_row1[23]),
        normalize_header(header_row1[24]),
        normalize_header(header_row1[25]),
        f"{normalize_header(header_row1[26])}_{normalize_header(header_row2[26])}",
        f"{normalize_header(header_row1[26])}_{normalize_header(header_row2[27])}",
        f"{normalize_header(header_row1[26])}_{normalize_header(header_row2[28])}",
    ]

    original_indices = [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        14,
        15,
        22,
        23,
        24,
        25,
        26,
        27,
        28,
    ]

    return final_combined_headers, original_indices


def is_float_check(value) -> bool:
    """Check if value can be converted to float."""
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def try_parse_date(date_str: str, fmt: str) -> pd.Timestamp:
    """Try to parse date with a single format."""
    try:
        return pd.to_datetime(date_str, format=fmt, errors="raise")
    except ValueError:
        return pd.NaT


def parse_date_robustly(row) -> pd.Timestamp:
    """Parse date string robustly with multiple format attempts.

    Strategy:
    1. Try unambiguous 4-digit year formats
    2. Try ambiguous formats (DD/MM/YYYY vs MM/DD/YYYY), guided by source month
    3. Try 2-digit year formats
    4. Try dash formats
    5. Fallback to general parsing
    6. Use source year/month if all else fails
    """
    date_str = row["Ngày"]
    source_month = row["_source_file_month"]
    source_year = row["_source_file_year"]

    if pd.isna(date_str) or not isinstance(date_str, str):
        return pd.NaT

    date_str = str(date_str).strip()

    # Stage 1: Unambiguous 4-digit year formats
    for fmt in ["%Y/%m/%d", "%Y-%m-%d"]:
        result = try_parse_date(date_str, fmt)
        if pd.notna(result):
            return result

    # Stage 2: Ambiguous 4-digit year formats (guided by source month)
    parsed_dmy = try_parse_date(date_str, "%d/%m/%Y")
    parsed_mdy = try_parse_date(date_str, "%m/%d/%Y")

    if pd.notna(parsed_dmy) and pd.notna(parsed_mdy):
        if parsed_mdy.month == source_month:
            return parsed_mdy
        elif parsed_dmy.month == source_month:
            return parsed_dmy
        return parsed_dmy
    elif pd.notna(parsed_dmy):
        return parsed_dmy
    elif pd.notna(parsed_mdy):
        return parsed_mdy

    # Stage 3: 2-digit year formats
    dmy_parsed = try_parse_date(date_str, "%d/%m/%y")
    mdy_parsed = try_parse_date(date_str, "%m/%d/%y")
    ymd_parsed = try_parse_date(date_str, "%y/%m/%d")

    potential_dates = [d for d in [dmy_parsed, mdy_parsed, ymd_parsed] if pd.notna(d)]

    if len(potential_dates) == 1:
        return potential_dates[0]
    elif len(potential_dates) > 1:
        # Prefer match with source month
        for parsed in [dmy_parsed, mdy_parsed, ymd_parsed]:
            if pd.notna(parsed) and parsed.month == source_month:
                return parsed
        return dmy_parsed if pd.notna(dmy_parsed) else mdy_parsed

    # Stage 4: Dash formats
    for fmt in ["%d-%m-%Y", "%m-%d-%Y", "%d-%m-%y", "%m-%d-%y"]:
        result = try_parse_date(date_str, fmt)
        if pd.notna(result):
            return result

    # Stage 5: Fallback to general parsing
    try:
        return pd.to_datetime(date_str, dayfirst=True, errors="raise")
    except ValueError:
        pass

    # Stage 6: Use source year/month as last resort
    if pd.notna(source_month) and pd.notna(source_year) and 1 <= source_month <= 12:
        try:
            return pd.to_datetime(f"{int(source_year)}-{int(source_month)}-01")
        except Exception:
            pass

    return pd.NaT


def load_and_extract_headers(
    matching_files: List[Path],
) -> Dict[Path, Tuple[List[str], List[int]]]:
    """Load CSV files and extract headers.

    Returns:
        dict: Mapping of file_path to (combined_headers, original_indices)
    """
    file_headers_map = {}
    for file_path in matching_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                rows = list(reader)

            header_row_main = 3  # 0-indexed
            header_row_sub = 4

            if len(rows) > header_row_sub:
                combined_header, original_indices = combine_headers(
                    rows[header_row_main], rows[header_row_sub]
                )
                file_headers_map[file_path] = (combined_header, original_indices)
            else:
                logger.warning(f"{file_path.name} has <5 rows, skipping.")
        except Exception as e:
            logger.error(f"Error processing {file_path.name} for headers: {e}")

    return file_headers_map


def process_group_data(
    files_and_indices_list: List[Tuple[Path, List[int]]], common_headers: List[str]
) -> pd.DataFrame:
    """Process data for a single group of files with same headers.

    Args:
        files_and_indices_list: List of (file_path, original_indices) tuples
        common_headers: List of column names

    Returns:
        pd.DataFrame: Merged DataFrame for the group
    """
    group_dfs = []
    data_start_row = 5

    for file_path, original_indices in files_and_indices_list:
        try:
            # Extract year/month from filename (format: YYYY_MM_...)
            filename = file_path.name
            parts = filename.split("_")
            source_year = int(parts[0])
            source_month = int(parts[1])

            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                all_rows = list(reader)

            if len(all_rows) < data_start_row:
                logger.warning(f"{file_path.name} has <5 rows, skipping data.")
                continue

            data_rows = all_rows[data_start_row:]
            processed_data = []

            for row in data_rows:
                if len(row) > max(original_indices) if original_indices else 0:
                    processed_data.append([row[idx] for idx in original_indices])
                else:
                    # Fill missing columns with pd.NA
                    new_row = [
                        row[idx] if idx < len(row) else pd.NA
                        for idx in original_indices
                    ]
                    processed_data.append(new_row)

            df = pd.DataFrame(processed_data, columns=common_headers)
            df = df.replace("", pd.NA)
            df["_source_file_month"] = source_month
            df["_source_file_year"] = source_year
            group_dfs.append(df)

        except Exception as e:
            logger.error(f"Error loading {file_path.name}: {e}")

    return pd.concat(group_dfs, ignore_index=True) if group_dfs else pd.DataFrame()


def clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and parse dates in the 'Ngày' column.

    Handles:
    - Excel serial dates (floats)
    - Multiple date formats (string)
    - Year/month mismatches vs source file
    """
    # Separate float and string dates
    float_mask = df["Ngày"].apply(is_float_check)
    float_dates_df = df[float_mask].copy()
    string_dates_df = df[~float_mask].copy()

    # Parse string dates
    if not string_dates_df.empty:
        string_dates_df["Parsed Ngày"] = string_dates_df.apply(
            parse_date_robustly, axis=1
        )

    # Convert Excel serial dates
    if not float_dates_df.empty:
        float_dates_df["Ngày"] = pd.to_numeric(float_dates_df["Ngày"], errors="coerce")
        float_dates_df["Parsed Ngày"] = pd.to_datetime(
            float_dates_df["Ngày"], unit="D", origin="1899-12-30", errors="coerce"
        )

    # Merge parsed dates back to main dataframe
    processed_dates = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    if not string_dates_df.empty:
        processed_dates.update(string_dates_df["Parsed Ngày"])
    if not float_dates_df.empty:
        processed_dates.update(float_dates_df["Parsed Ngày"])

    df["Ngày"] = processed_dates
    df["Ngày"] = pd.to_datetime(df["Ngày"], errors="coerce")

    # Handle date mismatches with backward fill
    verify_dates = pd.to_datetime(df["Ngày"], errors="coerce")
    mismatch_mask = verify_dates.notna() & (
        (verify_dates.dt.year != df["_source_file_year"])
        | (verify_dates.dt.month != df["_source_file_month"])
    )

    if mismatch_mask.any():
        df.loc[mismatch_mask, "Ngày"] = pd.NaT
        df["Ngày"] = df["Ngày"].bfill()
        logger.info(
            f"Applied backward fill to {mismatch_mask.sum()} conflicting dates."
        )

    # Enforce source year/month for remaining mismatches
    verify_dates = pd.to_datetime(df["Ngày"], errors="coerce")
    final_mismatch = verify_dates.notna() & (
        (verify_dates.dt.year != df["_source_file_year"])
        | (verify_dates.dt.month != df["_source_file_month"])
    )

    if final_mismatch.any():
        for idx in df[final_mismatch].index:
            try:
                current_date = pd.to_datetime(df.loc[idx, "Ngày"], errors="coerce")
                day = current_date.day if pd.notna(current_date) else 1
                year = int(df.loc[idx, "_source_file_year"])
                month = int(df.loc[idx, "_source_file_month"])

                max_days = pd.Timestamp(year, month, 1).days_in_month
                day = min(day, max_days)

                df.loc[idx, "Ngày"] = datetime(year, month, day)
            except (ValueError, TypeError):
                df.loc[idx, "Ngày"] = datetime(
                    int(df.loc[idx, "_source_file_year"]),
                    int(df.loc[idx, "_source_file_month"]),
                    1,
                )

        logger.info(f"Resolved {final_mismatch.sum()} year/month mismatches.")

    # Format as ISO string
    df["Ngày"] = df["Ngày"].dt.strftime("%Y-%m-%d").fillna("")
    return df


def clean_text_column(series: pd.Series) -> pd.Series:
    """Clean text: strip whitespace and normalize internal spaces."""
    return series.astype(str).str.strip().str.replace(r"\s+", " ", regex=True)


def standardize_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """Apply consistent data types to all columns."""
    # Text columns
    text_cols = ["Mã hàng", "Mã chứng từ", "Tên nhà cung cấp", "Tên hàng"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)
            if col == "Mã hàng":
                df[col] = df[col].str.upper()

    # Clean specific text columns
    for col in ["Tên hàng", "Tên nhà cung cấp"]:
        if col in df.columns:
            df[col] = clean_text_column(df[col])

    # Numeric columns
    for col in ["Số lượng", "Đơn giá", "Thành tiền"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Integer columns
    for col in ["Tháng", "Năm"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


def reorder_and_sort(df: pd.DataFrame) -> pd.DataFrame:
    """Reorder columns and sort by date and receipt ID."""
    # Reorder columns
    available_cols = [col for col in COLUMN_ORDER if col in df.columns]
    df = df[available_cols].copy()

    # Sort by date and receipt ID
    if "Ngày" in df.columns:
        df["_sort_date"] = pd.to_datetime(df["Ngày"], errors="coerce")
        df = df.sort_values(by=["_sort_date", "Mã chứng từ"], na_position="last")
        df = df.drop(columns=["_sort_date"])

    return df


def generate_output_filename(df: pd.DataFrame) -> str:
    """Generate filename from date range in data."""
    if "Năm" in df.columns and "Tháng" in df.columns:
        df["_source_date"] = pd.to_datetime(
            df["Năm"].astype(str) + "-" + df["Tháng"].astype(str) + "-01",
            errors="coerce",
        )
        min_date = df["_source_date"].min()
        max_date = df["_source_date"].max()

        min_year, min_month = min_date.year, min_date.month
        max_year, max_month = max_date.year, max_date.month

        filename = f"Chi tiết nhập {min_year:04d}-{min_month:02d}_{max_year:04d}-{max_month:02d}.csv"
        df.drop(columns=["_source_date"], inplace=True)
    else:
        filename = "Chi tiết nhập.csv"

    return filename


# ============================================================================
# MAIN PIPELINE FUNCTION
# ============================================================================


def transform_purchase_receipts(
    input_dir: Optional[Path] = None, output_dir: Optional[Path] = None
) -> Path:
    """Transform purchase receipt data from raw to staging.

    Args:
        input_dir: Path to raw directory (default: data/00-raw/import_export/)
        output_dir: Path to staging directory (default: data/01-staging/import_export/)

    Returns:
        Path: Output file path
    """
    logger.info("=" * 70)
    logger.info("Starting purchase receipt (CT.NHAP) transformation")
    logger.info("=" * 70)

    # Use defaults if not provided
    if input_dir is None:
        input_dir = DATA_RAW_DIR
    if output_dir is None:
        output_dir = DATA_STAGING_DIR

    # Find files matching pattern
    file_pattern = "*CT.NHAP.csv"
    matching_files = list(input_dir.glob(file_pattern))

    if not matching_files:
        logger.warning(f"No CSV files found matching {file_pattern} in {input_dir}")
        return None

    logger.info(f"Processing {len(matching_files)} file(s)")

    # Step 1: Load and extract headers
    file_headers_map = load_and_extract_headers(matching_files)

    # Step 2: Group files by header signature
    grouped_files = defaultdict(list)
    for file_path, (headers, indices) in file_headers_map.items():
        grouped_files[tuple(headers)].append((file_path, indices))

    # Step 3: Process each group
    merged_dataframes = {}
    for i, (headers_tuple, files_and_indices) in enumerate(grouped_files.items()):
        common_headers = list(headers_tuple)
        merged_df = process_group_data(files_and_indices, common_headers)

        if not merged_df.empty:
            merged_dataframes[f"Group_{i + 1}"] = merged_df

    if not merged_dataframes:
        logger.warning("No data to process.")
        return None

    # Step 4: Drop and rename columns first (before combining)
    processed_groups = {}
    for group_key, df in merged_dataframes.items():
        cols_to_drop = COLUMNS_TO_DROP["common"].copy()
        if "Group_2" in group_key:
            cols_to_drop.extend(COLUMNS_TO_DROP["group_2_specific"])

        df = df.drop(columns=cols_to_drop, errors="ignore")
        df = df.rename(columns=RENAME_MAPPING)
        if "Số lượng" in df.columns:
            df = df.dropna(subset=["Số lượng"])
        processed_groups[group_key] = df

    # Step 5: Combine all groups
    final_combined_df = pd.concat(
        [df for df in processed_groups.values()], ignore_index=True
    )

    # Step 5.5: Fill null Đơn giá and Thành tiền with 0 for rows with non-null Số lượng
    for col in ["Đơn giá", "Thành tiền"]:
        final_combined_df.loc[final_combined_df["Số lượng"].notna(), col] = (
            final_combined_df.loc[final_combined_df["Số lượng"].notna(), col].fillna(0)
        )

    # Step 6: Clean dates
    final_combined_df = clean_dates(final_combined_df)

    # Step 7: Rename source metadata columns
    final_combined_df = final_combined_df.rename(
        columns={"_source_file_month": "Tháng", "_source_file_year": "Năm"}
    )

    # Step 8: Standardize column types
    final_combined_df = standardize_column_types(final_combined_df)

    # Step 9: Reorder and sort
    final_combined_df = reorder_and_sort(final_combined_df)

    # Step 10: Save to validated directory
    output_dir.mkdir(parents=True, exist_ok=True)
    output_filename = generate_output_filename(final_combined_df)
    output_filepath = output_dir / output_filename

    final_combined_df.to_csv(output_filepath, index=False, encoding="utf-8")
    logger.info(f"Saved to: {output_filepath}")
    logger.info(
        f"Rows: {len(final_combined_df)}, Columns: {len(final_combined_df.columns)}"
    )

    return output_filepath


if __name__ == "__main__":
    transform_purchase_receipts()
