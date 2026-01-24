# -*- coding: utf-8 -*-
"""Schema validation for Google Sheets CSV exports.

Validates CSV files exported from Google Sheets against expected schemas.
Catches schema issues (missing columns, wrong data types, forbidden values)
before accepting data into staging.

Usage:
    from src.pipeline.validation import validate_schema

    if not validate_schema(csv_path, tab_name):
        logger.error(f"Schema validation failed: {csv_path}")
        csv_path.unlink()
"""

import logging
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


EXPECTED_SCHEMAS = {
    "Chi tiết nhập": {
        "required_columns": [
            "Ngày",
            "Mã hàng",
            "Tên hàng",
            "Số lượng",
            "Đơn giá",
            "Thành tiền",
        ],
        "date_columns": ["Ngày"],
        "numeric_columns": ["Số lượng", "Đơn giá", "Thành tiền"],
        "forbidden_values": {
            "Số lượng": [0, None, -1, -999],
            "Đơn giá": [0, None, -1, -999],
            "Thành tiền": [0, None, -1, -999],
        },
    },
    "Chi tiết xuất": {
        "required_columns": [
            "Ngày",
            "Mã hàng",
            "Tên hàng",
            "Số lượng",
            "Đơn giá",
            "Thành tiền",
        ],
        "date_columns": ["Ngày"],
        "numeric_columns": ["Số lượng", "Đơn giá", "Thành tiền"],
        "forbidden_values": {
            "Số lượng": [0, None, -1, -999],
            "Đơn giá": [0, None, -1, -999],
            "Thành tiền": [0, None, -1, -999],
        },
    },
    "Xuất nhập tồn": {
        "required_columns": ["Mã hàng", "Tên hàng", "Tồn cuối kỳ", "Giá trị cuối kỳ"],
        "optional_columns": ["Ngày"],
        "numeric_columns": ["Tồn cuối kỳ", "Giá trị cuối kỳ"],
        "forbidden_values": {
            "Tồn cuối kỳ": None,
            "Giá trị cuối kỳ": [0, None, -1, -999],
        },
    },
    "Chi tiết chi phí": {
        "required_columns": ["Mã hàng", "Tên hàng", "Số tiền", "Diễn giải"],
        "numeric_columns": ["Số tiền"],
        "forbidden_values": {"Số tiền": [0, None, -1, -999]},
    },
}


def _check_required_columns(df: pd.DataFrame, csv_path: Path, schema: Dict) -> bool:
    """Check that all required columns exist in DataFrame.

    Args:
        df: DataFrame to validate.
        csv_path: Path to CSV file (for error logging).
        schema: Schema definition with 'required_columns' key.

    Returns:
        True if all required columns present, False otherwise.
    """
    required = schema.get("required_columns", [])
    missing = [col for col in required if col not in df.columns]

    if missing:
        logger.error(f"{csv_path.name} missing required columns: {missing}")
        return False

    return True


def _check_numeric_columns(df: pd.DataFrame, csv_path: Path, schema: Dict) -> bool:
    """Check that numeric columns contain valid numeric data.

    Args:
        df: DataFrame to validate.
        csv_path: Path to CSV file (for error logging).
        schema: Schema definition with 'numeric_columns' key.

    Returns:
        True if all numeric columns valid, False otherwise.
    """
    numeric_cols = schema.get("numeric_columns", [])
    for col in numeric_cols:
        if col not in df.columns:
            continue

        if df[col].isna().all():
            logger.error(f"{csv_path.name}: Column {col} has all NaN values")
            return False

        if not pd.api.types.is_numeric_dtype(df[col]):
            logger.error(f"{csv_path.name}: Column {col} is not numeric")
            return False

    return True


def _check_forbidden_values(df: pd.DataFrame, csv_path: Path, schema: Dict) -> bool:
    """Check for forbidden values in columns.

    Args:
        df: DataFrame to validate.
        csv_path: Path to CSV file (for error logging).
        schema: Schema definition with 'forbidden_values' key.

    Returns:
        True if no forbidden values found, False otherwise.
    """
    forbidden_map = schema.get("forbidden_values", {})

    for col, forbidden in forbidden_map.items():
        if col not in df.columns:
            continue

        if forbidden is None:
            continue

        for val in forbidden:
            if val is None:
                if df[col].isna().any():
                    logger.error(f"{csv_path.name}: Column {col} contains NaN values")
                    return False
            else:
                if (df[col] == val).any():
                    logger.error(
                        f"{csv_path.name}: Column {col} contains forbidden value {val}"
                    )
                    return False

    return True


def _check_dataframe_not_empty(df: pd.DataFrame, csv_path: Path) -> bool:
    """Check that DataFrame is not empty.

    Args:
        df: DataFrame to validate.
        csv_path: Path to CSV file (for error logging).

    Returns:
        True if DataFrame has data, False otherwise.
    """
    if df.empty:
        logger.error(f"{csv_path.name}: Empty CSV file (no data rows)")
        return False

    return True


def validate_schema(csv_path: Path, tab_name: str) -> bool:
    """Validate CSV file meets expected schema.

    Performs comprehensive validation:
    - Checks required columns exist
    - Validates numeric columns
    - Checks for forbidden values (NaN, negative sentinel values)
    - Ensures DataFrame is not empty

    Args:
        csv_path: Path to CSV file to validate.
        tab_name: Name of tab type (e.g., "Chi tiết nhập", "Xuất nhập tồn").

    Returns:
        True if schema validation passes, False otherwise.

    Raises:
        ValueError: If tab_name is not a recognized schema type.

    Example:
        >>> csv_path = Path("data/01-staging/import_export/2025_01_Chi tiết nhập.csv")
        >>> if not validate_schema(csv_path, "Chi tiết nhập"):
        ...     logger.error("Schema validation failed")
    """
    if tab_name not in EXPECTED_SCHEMAS:
        raise ValueError(f"Unknown tab type: {tab_name}")

    schema = EXPECTED_SCHEMAS[tab_name]

    try:
        df = pd.read_csv(csv_path, encoding="utf-8")
    except Exception as e:
        logger.error(f"{csv_path.name}: Failed to read CSV: {e}")
        return False

    if not _check_dataframe_not_empty(df, csv_path):
        return False

    if not _check_required_columns(df, csv_path, schema):
        return False

    if not _check_numeric_columns(df, csv_path, schema):
        return False

    if not _check_forbidden_values(df, csv_path, schema):
        return False

    return True


def move_to_quarantine(csv_path: Path, quarantine_dir: Optional[Path] = None) -> Path:
    """Move rejected file to quarantine directory.

    Args:
        csv_path: Path to rejected CSV file.
        quarantine_dir: Custom quarantine directory. Defaults to data/00-rejected/.

    Returns:
        Path to quarantined file.

    Raises:
        FileNotFoundError: If source file does not exist.
    """
    if quarantine_dir is None:
        quarantine_dir = Path("data/00-rejected")

    quarantine_dir.mkdir(parents=True, exist_ok=True)

    dest_path = quarantine_dir / csv_path.name

    csv_path.rename(dest_path)

    logger.warning(f"Moved invalid file to quarantine: {dest_path}")

    return dest_path
