# -*- coding: utf-8 -*-
"""Clean sale receipt data (Chứng từ xuất) from CSV files.

Module: import_export_receipts
Raw source: XUẤT NHẬP TỒN TỔNG T* (CT.XUAT sheet from Google Drive)
Pipeline stage: data/00-raw/ → data/01-staging/
Output: Cleaned sale receipt details for Products/PriceBook extraction

This script:
1. Loads CSV files with multi-level headers from data/00-raw/
2. Groups files by header structure
3. Extracts and combines headers with uniqueness handling
4. Parses dates robustly (handles multiple formats, ambiguities)
5. Calculates total quantity (bán lẻ + bán sì)
6. Drops rows with zero/missing quantities
7. Standardizes columns and exports cleaned data to data/01-staging/
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

# ============================================================================
# CONFIGURATION
# ============================================================================

# Data folder paths (from data/README.md: ingest → staging)
DATA_RAW_DIR = Path.cwd() / "data" / "00-raw" / "import_export"
DATA_STAGING_DIR = Path.cwd() / "data" / "01-staging" / "import_export"

FILE_PATTERN = "*CT.XUAT.csv"

AUXILIARY_COLUMNS_TO_DROP = [
    "Số lượng_Bán lẻ",
    "PBH",
    "Tháng",
    "Unnamed_Column",
    "ĐVT",
    "Bán sì",
    "Unnamed_Column_1",
    "Unnamed_Column_2",
    "Unnamed_Column_3",
    "Unnamed_Column_4",
    "Unnamed_Column_5",
    "Unnamed_Column_6",
    "Unnamed_Column_7",
    "Unnamed_Column_8",
    "Ghi Chú",
    "Unnamed_Column_9",
    "Unnamed_Column_10",
    "164",
    "Tỉnh/TP",
    "Unnamed_Column_11",
    "Ngày_Year",
    "Ngày_Month",
]

RENAME_MAPPING = {
    "Chứng từ": "Mã chứng từ",
    "Ngày": "Ngày",
    "Khách hàng": "Tên khách hàng",
    "Mã Số": "Mã hàng",
    "Tên": "Tên hàng",
    "Giá bán": "Đơn giá",
    "Thành tiền": "Thành tiền",
    "Số lượng": "Số lượng",
    "year_from_filename": "Năm",
    "month_from_filename": "Tháng",
}

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
    "Tên khách hàng",
]

TEXT_COLUMNS = ["Mã hàng", "Mã chứng từ", "Tên khách hàng", "Tên hàng"]
NUMERIC_COLUMNS = ["Số lượng", "Đơn giá", "Thành tiền"]
INTEGER_COLUMNS = ["Tháng", "Năm"]

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


def extract_year_month_from_filename(
    filepath: Path,
) -> Tuple[Optional[int], Optional[int]]:
    """Extract year and month from filename in format 'YYYY_MM_CT.XUAT.csv'."""
    match = re.search(r"(\d{4})_(\d{1,2})_CT\.XUAT\.csv", filepath.name)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def read_header_lines(
    filepath: Path, num_lines: int = 5, encoding: str = "utf-8"
) -> Optional[List[str]]:
    """Read first N lines from file with encoding fallback."""
    try:
        with open(filepath, "r", encoding=encoding) as f:
            return [f.readline() for _ in range(num_lines)]
    except UnicodeDecodeError:
        if encoding == "utf-8":
            return read_header_lines(filepath, num_lines, encoding="latin1")
        logger.error(
            f"Failed to read {filepath.name} with both UTF-8 and latin1 encodings"
        )
        return None


def combine_headers(header_row_4: pd.Series, header_row_5: pd.Series) -> List[str]:
    """Combine two header rows into single header list with uniqueness handling."""
    combined_headers = []
    for h4, h5 in zip(header_row_4, header_row_5):
        h4 = str(h4).strip() if pd.notna(h4) else ""
        h5 = str(h5).strip() if pd.notna(h5) else ""

        if h4 and h5 and h4 != h5:
            combined_headers.append(f"{h4}_{h5}")
        elif h4:
            combined_headers.append(h4)
        elif h5:
            combined_headers.append(h5)
        else:
            combined_headers.append("Unnamed_Column")

    # Ensure header uniqueness
    seen = {}
    unique_headers = []
    for header_name in combined_headers:
        original_name = header_name
        count = seen.get(original_name, 0)
        if count > 0:
            header_name = f"{original_name}_{count}"
        while header_name in unique_headers:
            count += 1
            header_name = f"{original_name}_{count}"
        unique_headers.append(header_name)
        seen[original_name] = count + 1

    return unique_headers


def extract_and_combine_headers(filepath: Path) -> List[str]:
    """Extract and combine headers from rows 4 and 5 of CSV file."""
    lines = read_header_lines(filepath)
    if lines is None:
        return [f"Encoding_Error_for_{filepath.name}"]

    try:
        header_data = []
        for i in [3, 4]:  # Rows 4 and 5 are 0-indexed as 3 and 4
            if i < len(lines):
                header_data.append([item.strip() for item in lines[i].split(",")])
            else:
                header_data.append([])

        # Pad rows to same length
        max_cols = max(len(row) for row in header_data) if header_data else 0
        for row in header_data:
            while len(row) < max_cols:
                row.append("")

        df_preview = pd.DataFrame(header_data)
        header_row_4 = df_preview.iloc[0].fillna("")
        header_row_5 = (
            df_preview.iloc[1].fillna("") if len(df_preview) > 1 else pd.Series([])
        )

        return combine_headers(header_row_4, header_row_5)

    except Exception as e:
        logger.error(f"Error processing headers for {filepath.name}: {e}")
        return [f"Error_Processing_Headers_for_{filepath.name}"]


def process_dates(
    df: pd.DataFrame, year: Optional[int], month: Optional[int]
) -> pd.DataFrame:
    """Parse and standardize the 'Ngày' date column.

    Original XUAT logic: Try standard format first, fallback to day-only interpretation.
    This preserves original behavior while still fixing encoding issues.
    """
    if "Ngày" not in df.columns:
        df["Ngày_Year"] = pd.NA
        df["Ngày_Month"] = pd.NA
        return df

    # Attempt to parse date directly with standard format
    df["Ngày_parsed"] = pd.to_datetime(df["Ngày"], format="%d/%m/%Y", errors="coerce")

    if year is not None and month is not None:
        # Extract day-only values
        day_only = pd.to_numeric(df["Ngày"], errors="coerce")
        valid_day_mask = day_only.notna() & (day_only >= 1) & (day_only <= 31)

        # Fill NaT where day was valid but full date parsing failed
        needs_date_completion = valid_day_mask & df["Ngày_parsed"].isna()
        if needs_date_completion.any():
            date_strings = (
                day_only[needs_date_completion].astype(int).astype(str).str.zfill(2)
                + f"/{month:02d}/{year}"
            )
            df.loc[needs_date_completion, "Ngày_parsed"] = pd.to_datetime(
                date_strings, format="%d/%m/%Y", errors="coerce"
            )

        # Default unparsed dates to day 1 of month/year from filename
        unparsed_mask = df["Ngày_parsed"].isna()
        if unparsed_mask.any():
            default_date = f"01/{month:02d}/{year}"
            df.loc[unparsed_mask, "Ngày_parsed"] = pd.to_datetime(
                default_date, errors="coerce"
            )

    df["Ngày_Year"] = df["Ngày_parsed"].dt.year
    df["Ngày_Month"] = df["Ngày_parsed"].dt.month
    df["Ngày"] = df["Ngày_parsed"].dt.strftime("%Y-%m-%d").fillna("")
    df.drop(columns=["Ngày_parsed"], inplace=True)

    return df


def read_csv_file(filepath: Path, header_tuple: Tuple) -> Optional[pd.DataFrame]:
    """Read CSV file with encoding fallback and robust date processing."""
    year, month = extract_year_month_from_filename(filepath)

    encodings = ["utf-8", "latin1"]
    for encoding in encodings:
        try:
            df = pd.read_csv(
                filepath,
                skiprows=5,
                header=None,
                names=list(header_tuple),
                encoding=encoding,
                sep=",",
                engine="python",
            )
            df["year_from_filename"] = year
            df["month_from_filename"] = month
            df = process_dates(df, year, month)
            return df
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.warning(f"Error reading {filepath.name} with {encoding}: {e}")
            continue

    logger.error(f"Failed to read {filepath.name} with all encodings")
    return None


def validate_date_consistency(df: pd.DataFrame) -> None:
    """Check for mismatches between parsed dates and filename metadata."""
    df["Ngày_Year"] = pd.to_numeric(df["Ngày_Year"], errors="coerce")
    df["Ngày_Month"] = pd.to_numeric(df["Ngày_Month"], errors="coerce")

    year_mismatch = df[
        (df["Ngày_Year"] != df["year_from_filename"])
        & df["Ngày_Year"].notna()
        & df["year_from_filename"].notna()
    ]

    month_mismatch = df[
        (df["Ngày_Month"] != df["month_from_filename"])
        & df["Ngày_Month"].notna()
        & df["month_from_filename"].notna()
    ]

    if not year_mismatch.empty:
        logger.warning(f"{len(year_mismatch)} rows with year mismatches")
    if not month_mismatch.empty:
        logger.warning(f"{len(month_mismatch)} rows with month mismatches")
    if year_mismatch.empty and month_mismatch.empty:
        logger.info("All dates consistently match filename metadata")


def validate_data_completeness(df: pd.DataFrame) -> None:
    """Check for sparse columns with less than 90% non-null values."""
    non_null_percentage = (df.count() / len(df)) * 100
    sparse_cols = non_null_percentage[non_null_percentage < 90].index.tolist()
    if sparse_cols:
        logger.warning(
            f"{len(sparse_cols)} columns have less than 90% non-null values: {sparse_cols}"
        )


def clean_text_column(series: pd.Series) -> pd.Series:
    """Clean text: strip whitespace and normalize internal spaces."""
    return series.astype(str).str.strip().str.replace(r"\s+", " ", regex=True)


def standardize_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """Apply consistent data types to all columns."""
    # Text columns
    for col in TEXT_COLUMNS:
        if col in df.columns:
            df[col] = df[col].astype(str)
            if col == "Mã hàng":
                df[col] = df[col].str.upper()

    # Clean specific text columns (whitespace normalization)
    for col in ["Tên khách hàng", "Tên hàng"]:
        if col in df.columns:
            df[col] = clean_text_column(df[col])

    # Numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Integer columns
    for col in INTEGER_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


def process_groups(
    grouped_files: Dict,
) -> pd.DataFrame:
    """Process each file group and combine into single DataFrame.

    Args:
        grouped_files: Dict mapping header tuples to file paths
    """
    combined_dfs = {}

    for header_tuple, filepaths in grouped_files.items():
        dfs_for_group = []
        row_counter = 0

        for filepath in filepaths:
            df = read_csv_file(filepath, header_tuple)
            if df is not None:
                row_counter += len(df)
                dfs_for_group.append(df)

        if not dfs_for_group:
            continue

        combined_df = pd.concat(dfs_for_group, ignore_index=True)

        # Conditional column renaming (header-pattern specific)
        if (
            len(header_tuple) > 5
            and header_tuple[5] == "Unnamed_Column"
            and "Unnamed_Column" in combined_df.columns
        ):
            combined_df.rename(columns={"Unnamed_Column": "Mã Số"}, inplace=True)

        if "Mã số_MÃ SỐ" in combined_df.columns:
            combined_df.rename(columns={"Mã số_MÃ SỐ": "Mã Số"}, inplace=True)

        if "Chủng loại" in combined_df.columns:
            combined_df.rename(columns={"Chủng loại": "Tên"}, inplace=True)

        if "Chứng từ xuất_PXK" in combined_df.columns:
            combined_df.rename(columns={"Chứng từ xuất_PXK": "Chứng từ"}, inplace=True)

        # Calculate total quantity
        so_luong_ban_le = pd.to_numeric(
            combined_df.get("Số lượng_Bán lẻ", pd.Series(index=combined_df.index)),
            errors="coerce",
        ).fillna(0)
        ban_si = pd.to_numeric(
            combined_df.get("Bán sì", pd.Series(index=combined_df.index)),
            errors="coerce",
        ).fillna(0)

        # Handle alternative pattern where quantity is combined (Số lượng_Bán sì)
        if (
            "Số lượng_Bán sì" in combined_df.columns
            and so_luong_ban_le.sum() == 0
            and ban_si.sum() == 0
        ):
            so_luong_ban_si = pd.to_numeric(
                combined_df.get("Số lượng_Bán sì", pd.Series(index=combined_df.index)),
                errors="coerce",
            ).fillna(0)
            combined_df["Số lượng"] = so_luong_ban_si
        else:
            combined_df["Số lượng"] = so_luong_ban_le + ban_si

        # Drop zero-quantity rows
        combined_df = combined_df[
            combined_df["Số lượng"].notna() & (combined_df["Số lượng"] != 0)
        ]

        # Convert numeric columns
        if "Giá bán" in combined_df.columns:
            combined_df["Giá bán"] = pd.to_numeric(
                combined_df["Giá bán"], errors="coerce"
            )
        if "Thành tiền" in combined_df.columns:
            combined_df["Thành tiền"] = pd.to_numeric(
                combined_df["Thành tiền"], errors="coerce"
            )

        combined_dfs[header_tuple] = combined_df

    return (
        pd.concat(combined_dfs.values(), ignore_index=True)
        if combined_dfs
        else pd.DataFrame()
    )


def fill_and_adjust_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Fill nulls and adjust quantities based on data patterns.

    On rows with non-null 'Số lượng':
    - Fill null 'Đơn giá' and 'Thành tiền' with 0
    - Fill null 'Tên khách hàng' with 'KHÁCH LẺ'

    On rows with positive 'Số lượng' and negative 'Đơn giá':
    - Make 'Số lượng' negative and 'Đơn giá' positive
    """
    # Ensure numeric columns are properly typed
    if "Số lượng" in df.columns:
        df["Số lượng"] = pd.to_numeric(df["Số lượng"], errors="coerce")
    if "Đơn giá" in df.columns:
        df["Đơn giá"] = pd.to_numeric(df["Đơn giá"], errors="coerce")
    if "Thành tiền" in df.columns:
        df["Thành tiền"] = pd.to_numeric(df["Thành tiền"], errors="coerce")

    # Rows with non-null Số lượng
    non_null_qty = df["Số lượng"].notna()

    # Fill null Đơn giá and Thành tiền with 0
    if "Đơn giá" in df.columns:
        df.loc[non_null_qty & df["Đơn giá"].isna(), "Đơn giá"] = 0
    if "Thành tiền" in df.columns:
        df.loc[non_null_qty & df["Thành tiền"].isna(), "Thành tiền"] = 0

    # Fill null Tên khách hàng with 'KHÁCH LẺ'
    if "Tên khách hàng" in df.columns:
        df.loc[non_null_qty & df["Tên khách hàng"].isna(), "Tên khách hàng"] = (
            "KHÁCH LẺ"
        )

    # For rows with positive Số lượng and negative Đơn giá
    if "Số lượng" in df.columns and "Đơn giá" in df.columns:
        pos_qty_neg_price = (df["Số lượng"] > 0) & (df["Đơn giá"] < 0)
        df.loc[pos_qty_neg_price, "Số lượng"] = -df.loc[pos_qty_neg_price, "Số lượng"]
        df.loc[pos_qty_neg_price, "Đơn giá"] = -df.loc[pos_qty_neg_price, "Đơn giá"]

    return df


def generate_output_filename(df: pd.DataFrame) -> str:
    """Generate filename from date range in data."""
    df["year_month_dt"] = pd.to_datetime(
        df["Năm"].astype(str) + "-" + df["Tháng"].astype(str).str.zfill(2) + "-01",
        errors="coerce",
    )

    valid_dates_df = df.dropna(subset=["year_month_dt"])
    if not valid_dates_df.empty:
        min_date = valid_dates_df["year_month_dt"].min()
        max_date = valid_dates_df["year_month_dt"].max()
        first_year, first_month = min_date.year, min_date.month
        last_year, last_month = max_date.year, max_date.month
    else:
        first_year = first_month = last_year = last_month = 0
        logger.warning("Could not determine year/month range from data")

    filename = f"Chi tiết xuất {first_year:04d}-{first_month:02d}_{last_year:04d}-{last_month:02d}.csv"
    df.drop(columns=["year_month_dt"], inplace=True, errors="ignore")

    return filename


def create_reconciliation_checkpoint(
    input_dir: Path,
    output_filepath: Path,
    script_name: str = "clean_receipts_sale",
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
    input_totals = defaultdict(float)
    input_row_counts = defaultdict(int)

    for csv_file in input_dir.glob("*CT.XUAT.csv"):
        try:
            with open(csv_file, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                rows = list(reader)

            # Skip header rows (first 5 rows)
            for row_idx, row in enumerate(rows[5:], start=1):
                # Count rows and sum quantities (approximate scan)
                input_row_counts[csv_file.name] += 1
                if len(row) > 8:  # Quantity typically in column 8+
                    try:
                        qty = float(row[8]) if row[8] else 0
                        input_totals[csv_file.name] += qty
                    except ValueError:
                        pass
        except Exception as e:
            logger.warning(f"Error reading {csv_file.name} for reconciliation: {e}")

    # Step 2: Calculate OUTPUT totals from staging file
    output_df = pd.read_csv(output_filepath)
    output_total_qty = (
        output_df["Số lượng"].sum() if "Số lượng" in output_df.columns else 0
    )
    output_row_count = len(output_df)

    # Step 3: Build reconciliation report
    total_input_qty = sum(input_totals.values())
    total_input_rows = sum(input_row_counts.values())

    report = {
        "timestamp": datetime.now().isoformat(),
        "script": script_name,
        "input": {
            "total_quantity": float(total_input_qty),
            "total_rows": int(total_input_rows),
        },
        "output": {
            "total_quantity": float(output_total_qty),
            "total_rows": int(output_row_count),
        },
        "reconciliation": {
            "quantity_dropout_pct": (
                (total_input_qty - output_total_qty) / total_input_qty * 100
                if total_input_qty > 0
                else 0
            ),
            "row_dropout_pct": (
                (total_input_rows - output_row_count) / total_input_rows * 100
                if total_input_rows > 0
                else 0
            ),
        },
        "alerts": [],
    }

    # Step 5: Flag issues
    if report["reconciliation"]["quantity_dropout_pct"] > 5:
        report["alerts"].append(
            f"⚠️ WARNING: {report['reconciliation']['quantity_dropout_pct']:.1f}% "
            f"quantity dropped ({total_input_qty:,.0f} → {output_total_qty:,.0f})"
        )

    # Step 6: Save reconciliation report
    report_filename = f"reconciliation_report_{script_name}.json"
    report_path = output_filepath.parent / report_filename
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    # Step 7: Log report
    logger.info("=" * 70)
    logger.info(f"RECONCILIATION REPORT ({script_name})")
    logger.info(f"Input:  {total_input_qty:,.0f} qty across {total_input_rows:,} rows")
    logger.info(f"Output: {output_total_qty:,.0f} qty across {output_row_count:,} rows")
    logger.info(f"Dropout: {report['reconciliation']['quantity_dropout_pct']:.1f}%")
    logger.info(f"Report saved to: {report_filename}")
    for alert in report["alerts"]:
        logger.warning(alert)
    logger.info("=" * 70)

    return report


# ============================================================================
# MAIN PIPELINE FUNCTION
# ============================================================================


def transform_sale_receipts(
    input_dir: Optional[Path] = None, output_dir: Optional[Path] = None
) -> Path:
    """Transform sale receipt data from raw to staging.

    Args:
        input_dir: Path to raw directory (default: data/00-raw/import_export/)
        output_dir: Path to staging directory (default: data/01-staging/import_export/)

    Returns:
        Path: Output file path
    """
    logger.info("=" * 70)
    logger.info("Starting sale receipt (CT.XUAT) transformation")
    logger.info("=" * 70)

    # Use defaults if not provided
    if input_dir is None:
        input_dir = DATA_RAW_DIR
    if output_dir is None:
        output_dir = DATA_STAGING_DIR

    # Find all CSV files
    csv_files = list(input_dir.glob(FILE_PATTERN))
    if not csv_files:
        logger.warning(f"No CSV files found matching {FILE_PATTERN} in {input_dir}")
        return None

    logger.info(f"Found {len(csv_files)} CSV files")

    # Group files by header
    grouped_files_by_header = {}
    for filepath in csv_files:
        combined_header = extract_and_combine_headers(filepath)
        header_tuple = tuple(combined_header)
        if header_tuple not in grouped_files_by_header:
            grouped_files_by_header[header_tuple] = []
        grouped_files_by_header[header_tuple].append(filepath)

    logger.info(f"Grouped files into {len(grouped_files_by_header)} header patterns")

    # Process all groups
    final_df = process_groups(grouped_files_by_header)
    if final_df.empty:
        logger.error("No data processed")
        return None

    logger.info(f"Initial DataFrame shape: {final_df.shape}")

    # Validate data
    validate_date_consistency(final_df)
    validate_data_completeness(final_df)

    # Clean up columns
    final_df = final_df.drop(
        columns=[col for col in AUXILIARY_COLUMNS_TO_DROP if col in final_df.columns]
    )

    # Rename columns
    filtered_rename_mapping = {k: v for k, v in RENAME_MAPPING.items() if v}
    final_df.rename(columns=filtered_rename_mapping, inplace=True)

    # Fill nulls and adjust quantities
    final_df = fill_and_adjust_rows(final_df)

    # Reorder columns
    final_df = final_df[[col for col in COLUMN_ORDER if col in final_df.columns]]

    # Generate output filename
    output_filename = generate_output_filename(final_df)

    # Standardize column types
    final_df = standardize_column_types(final_df)

    # Sort data
    if "Ngày" in final_df.columns and "Mã chứng từ" in final_df.columns:
        final_df["Ngày_dt"] = pd.to_datetime(final_df["Ngày"], errors="coerce")
        final_df = final_df.sort_values(
            by=["Ngày_dt", "Mã chứng từ"], na_position="last"
        )
        final_df = final_df.drop(columns=["Ngày_dt"])

    # Save output
    output_dir.mkdir(parents=True, exist_ok=True)
    output_filepath = output_dir / output_filename
    final_df.to_csv(output_filepath, index=False, encoding="utf-8")
    logger.info(f"Saved to: {output_filepath}")
    logger.info(f"Rows: {len(final_df)}, Columns: {len(final_df.columns)}")

    # Create summary dataframe (aggregate by Tháng, Năm)
    logger.info("=" * 70)
    logger.info("Creating summary aggregation by month/year")
    summary_df = final_df.groupby(["Tháng", "Năm"], as_index=False).agg(
        {"Số lượng": "sum", "Thành tiền": "sum"}
    )

    # Reorder columns
    summary_df = summary_df[["Năm", "Tháng", "Số lượng", "Thành tiền"]]

    # Generate summary filename
    summary_filename = output_filename.replace("Chi tiết xuất", "Tổng hợp xuất")
    summary_filepath = output_dir / summary_filename
    summary_df.to_csv(summary_filepath, index=False, encoding="utf-8")
    logger.info(f"Saved summary to: {summary_filepath}")
    logger.info(f"Summary rows: {len(summary_df)}, Columns: {len(summary_df.columns)}")
    logger.info("=" * 70)

    # Reconciliation check
    create_reconciliation_checkpoint(input_dir, output_filepath, "clean_receipts_sale")

    return output_filepath


if __name__ == "__main__":
    transform_sale_receipts()
