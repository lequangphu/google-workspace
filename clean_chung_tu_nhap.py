# -*- coding: utf-8 -*-
"""Clean import receipt data (Chứng từ nhập) from CSV files.

This script:
1. Loads CSV files with multi-level headers
2. Extracts and combines headers
3. Parses dates robustly (handles Excel dates, multiple formats, ambiguities)
4. Validates dates against source year/month
5. Standardizes columns and exports cleaned data
"""

import csv
import glob
import os
from collections import defaultdict
from datetime import datetime

import pandas as pd

# === CONFIGURATION ===
CONFIG = {
    "data_dir": os.path.join(os.getcwd(), "data", "raw"),
    "file_pattern": "*CT.NHAP.csv",
    "output_dir": os.path.join(os.getcwd(), "data", "final"),
    "header_row_main": 3,  # 0-indexed
    "header_row_sub": 4,
    "data_start_row": 5,
    "date_formats_strict": [
        "%Y/%m/%d",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%m-%d-%Y",
        "%d-%m-%y",
        "%m-%d-%y",
        "%d/%m/%y",
        "%m/%d/%y",
        "%y/%m/%d",
    ],
}

# Column mappings: (row1_idx, row2_idx, new_name) or row1_idx for single row
HEADER_COLUMN_MAP = [
    ((0, 0, 1, 2), "Chứng từ nhập"),  # Combine rows 1 and 2
    ((0, 1, 1, 2), "Chứng từ nhập"),
    ((0, 2, 1, 2), "Chứng từ nhập"),
    (3, "Nhà CC"),  # Single row
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

# Column rename mapping
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

# Final column order
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


# === HELPER FUNCTIONS ===
def combine_headers(header_row1, header_row2):
    """Combine two header rows into a single standardized header list.
    
    Args:
        header_row1: Primary header row
        header_row2: Secondary header row
        
    Returns:
        tuple: (combined_headers, original_indices)
    """
    final_combined_headers = [
        f"{header_row1[0].strip()}_{header_row2[0].strip()}",
        f"{header_row1[0].strip()}_{header_row2[1].strip()}",
        f"{header_row1[0].strip()}_{header_row2[2].strip()}",
        header_row2[3].strip(),
        header_row1[4].strip(),
        header_row1[5].strip(),
        header_row1[6].strip(),
        header_row1[7].strip(),
        f"{header_row1[8].strip()}_{header_row2[8].strip()}",
        f"{header_row1[8].strip()}_{header_row2[9].strip()}",
        f"{header_row1[8].strip()}_{header_row2[10].strip()}",
        f"{header_row1[8].strip()}_{header_row2[14].strip()}",
        f"{header_row1[8].strip()}_{header_row2[15].strip()}",
        header_row1[22].strip(),
        header_row1[23].strip().replace("\n", " "),
        header_row1[24].strip(),
        header_row1[25].strip(),
        f"{header_row1[26].strip()}_{header_row2[26].strip()}",
        f"{header_row1[26].strip()}_{header_row2[27].strip()}",
        f"{header_row1[26].strip()}_{header_row2[28].strip()}",
    ]
    
    original_indices = [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 14, 15, 22, 23, 24, 25, 26, 27, 28
    ]
    
    return final_combined_headers, original_indices


def is_float_check(value):
    """Check if value can be converted to float."""
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def try_parse_date(date_str, fmt):
    """Try to parse date with a single format."""
    try:
        return pd.to_datetime(date_str, format=fmt, errors="raise")
    except ValueError:
        return pd.NaT


def parse_date_robustly(row):
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


def load_and_extract_headers(matching_files):
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

            if len(rows) > CONFIG["header_row_sub"]:
                header_row_main = rows[CONFIG["header_row_main"]]
                header_row_sub = rows[CONFIG["header_row_sub"]]
                combined_header, original_indices = combine_headers(
                    header_row_main, header_row_sub
                )
                file_headers_map[file_path] = (combined_header, original_indices)
            else:
                print(f"Warning: {file_path} has <5 rows, skipping.")
        except Exception as e:
            print(f"Error processing {file_path} for headers: {e}")

    return file_headers_map


def process_group_data(files_and_indices_list, common_headers):
    """Process data for a single group of files with same headers.
    
    Args:
        files_and_indices_list: List of (file_path, original_indices) tuples
        common_headers: List of column names
        
    Returns:
        pd.DataFrame: Merged DataFrame for the group
    """
    group_dfs = []
    
    for file_path, original_indices in files_and_indices_list:
        try:
            # Extract year/month from filename (format: YYYY_MM_...)
            filename = os.path.basename(file_path)
            parts = filename.split("_")
            source_year = int(parts[0])
            source_month = int(parts[1])

            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                all_rows = list(reader)

            if len(all_rows) < CONFIG["data_start_row"]:
                print(f"Warning: {file_path} has <5 rows, skipping data.")
                continue

            data_rows = all_rows[CONFIG["data_start_row"] :]
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
            print(f"Error loading {file_path}: {e}")

    return pd.concat(group_dfs, ignore_index=True) if group_dfs else pd.DataFrame()


def clean_dates(df):
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
        string_dates_df["Parsed Ngày"] = string_dates_df.apply(parse_date_robustly, axis=1)

    # Convert Excel serial dates
    if not float_dates_df.empty:
        float_dates_df["Ngày"] = pd.to_numeric(float_dates_df["Ngày"], errors="coerce")
        float_dates_df["Parsed Ngày"] = pd.to_datetime(
            float_dates_df["Ngày"], unit="D", origin="1899-12-30", errors="coerce"
        )

    # Merge parsed dates back to main dataframe
    processed_dates = pd.Series(
        pd.NaT, index=df.index, dtype="datetime64[ns]"
    )
    if not string_dates_df.empty:
        processed_dates.update(string_dates_df["Parsed Ngày"])
    if not float_dates_df.empty:
        processed_dates.update(float_dates_df["Parsed Ngày"])

    df["Ngày"] = processed_dates
    df["Ngày"] = pd.to_datetime(df["Ngày"], errors="coerce")

    # Handle date mismatches with backward fill
    verify_dates = pd.to_datetime(df["Ngày"], errors="coerce")
    mismatch_mask = verify_dates.notna() & (
        (verify_dates.dt.year != df["_source_file_year"]) |
        (verify_dates.dt.month != df["_source_file_month"])
    )
    
    if mismatch_mask.any():
        df.loc[mismatch_mask, "Ngày"] = pd.NaT
        df["Ngày"] = df["Ngày"].bfill()
        print(f"Applied backward fill to {mismatch_mask.sum()} conflicting dates.")

    # Enforce source year/month for remaining mismatches
    verify_dates = pd.to_datetime(df["Ngày"], errors="coerce")
    final_mismatch = verify_dates.notna() & (
        (verify_dates.dt.year != df["_source_file_year"]) |
        (verify_dates.dt.month != df["_source_file_month"])
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
        
        print(f"Resolved {final_mismatch.sum()} year/month mismatches.")

    # Format as ISO string
    df["Ngày"] = df["Ngày"].dt.strftime("%Y-%m-%d").fillna("")
    return df


def format_output(df):
    """Format and standardize output columns.
    
    Args:
        df: DataFrame to format
        
    Returns:
        pd.DataFrame: Formatted DataFrame
    """
    # Ensure text columns are strings
    text_cols = ["Mã hàng", "Mã chứng từ", "Tên nhà cung cấp", "Tên hàng"]
    for col in text_cols:
        if col in df.columns:
            if col == "Mã hàng":
                df[col] = df[col].astype(str).str.upper()
            else:
                df[col] = df[col].astype(str)

    # Numeric columns
    for col in ["Số lượng", "Đơn giá", "Thành tiền"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Integer columns
    for col in ["Tháng", "Năm"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Reorder columns
    available_cols = [col for col in COLUMN_ORDER if col in df.columns]
    df = df[available_cols].copy()

    # Sort by date and receipt ID
    if "Ngày" in df.columns:
        df.loc[:, "_sort_date"] = pd.to_datetime(df["Ngày"], errors="coerce")
        df = df.sort_values(by=["_sort_date", "Mã chứng từ"], na_position="last")
        df = df.drop(columns=["_sort_date"])

    return df


# === MAIN EXECUTION ===
def main():
    """Main processing pipeline."""
    directory_path = CONFIG["data_dir"]
    file_pattern = CONFIG["file_pattern"]
    full_pattern = os.path.join(directory_path, file_pattern)
    matching_files = glob.glob(full_pattern)

    if not matching_files:
        print("No CSV files found for processing.")
        return

    print(f"Processing {len(matching_files)} file(s)...")

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
        print("No data to process.")
        return

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

    # Step 6: Clean dates (now that columns are renamed)
    final_combined_df = clean_dates(final_combined_df)

    # Step 7: Rename source metadata columns
    final_combined_df = final_combined_df.rename(
        columns={"_source_file_month": "Tháng", "_source_file_year": "Năm"}
    )

    # Step 8: Format output
    final_combined_df = format_output(final_combined_df)

    # Step 9: Save to CSV
    output_dir = CONFIG["output_dir"]
    os.makedirs(output_dir, exist_ok=True)

    # Generate filename from date range
    if "Năm" in final_combined_df.columns and "Tháng" in final_combined_df.columns:
        final_combined_df["_source_date"] = pd.to_datetime(
            final_combined_df["Năm"].astype(str) + "-" +
            final_combined_df["Tháng"].astype(str) + "-01",
            errors="coerce",
        )
        min_date = final_combined_df["_source_date"].min()
        max_date = final_combined_df["_source_date"].max()
        
        min_year, min_month = min_date.year, min_date.month
        max_year, max_month = max_date.year, max_date.month
        
        output_filename = (
            f"{min_year:04d}_{min_month:02d}_{max_year:04d}_{max_month:02d}_"
            f"CT.NHAP_processed.csv"
        )
        final_combined_df = final_combined_df.drop(columns=["_source_date"])
    else:
        output_filename = "CT.NHAP_processed.csv"

    output_filepath = os.path.join(output_dir, output_filename)
    final_combined_df.to_csv(output_filepath, index=False, encoding="utf-8")
    print(f"\nFinal DataFrame saved to: {output_filepath}")
    print(f"Rows: {len(final_combined_df)}, Columns: {len(final_combined_df.columns)}")


if __name__ == "__main__":
    main()
