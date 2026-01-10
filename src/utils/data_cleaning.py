# -*- coding: utf-8 -*-
"""Consolidated data cleaning utilities for master data (customers, suppliers).

This module consolidates duplicate functions from:
- src/modules/receivable/generate_customers_xlsx.py
- src/modules/payable/generate_suppliers_xlsx.py

Functions are unified to reduce code duplication and ensure consistent data cleaning
across all master data processing modules.
"""

import logging
import re
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)


def clean_phone_number(phone: str) -> str:
    """Clean a single phone number by removing all dots, commas, spaces, and trailing punctuation.

    Args:
        phone: Raw phone number string

    Returns:
        Cleaned phone number with only digits
    """
    if not phone:
        return ""
    phone = str(phone).strip()
    phone = re.sub(r"[.,;:]", "", phone)
    phone = re.sub(r"\s+", "", phone)
    return phone


def split_phone_numbers(phone_str: str) -> List[str]:
    """Split phone numbers by common delimiters and clean them.

    Handles delimiters: /, -, and multiple spaces.

    Args:
        phone_str: Raw phone string potentially containing multiple numbers

    Returns:
        List of cleaned phone numbers
    """
    if not phone_str or pd.isna(phone_str):
        return []

    phone_str = str(phone_str).strip()
    if not phone_str or phone_str == "None":
        return []

    if "/" in phone_str or " - " in phone_str:
        phones = re.split(r"\s*[/-]\s*", phone_str)
    else:
        phones = [phone_str]

    cleaned = [clean_phone_number(p) for p in phones]
    cleaned = [p for p in cleaned if p]

    return cleaned


def parse_numeric(value: str) -> str:
    """Parse Vietnamese number format to raw number string.

    Handles:
    - Vietnamese thousands separator (dots): "1.500.000" -> "1500000"
    - Negative values in parentheses: "(30000)" -> "-30000"
    - Dash as zero: "-" -> "0"

    Args:
        value: Raw numeric string from Google Sheets

    Returns:
        Cleaned numeric string suitable for float conversion
    """
    if not value or pd.isna(value):
        return "0"

    value = str(value).strip()

    if value == "-" or value == "":
        return "0"

    value = value.replace(".", "").replace(" ", "")

    if value.startswith("(") and value.endswith(")"):
        value = "-" + value[1:-1]

    return value


def convert_date_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Standard date column conversion with error handling.

    Args:
        df: DataFrame to process
        column: Name of date column to convert

    Returns:
        DataFrame with date column converted to datetime
    """
    df = df.copy()
    if column in df.columns:
        df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def merge_master_data(
    master_df: pd.DataFrame,
    debts_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    name_column: str,
    debt_column: str,
) -> pd.DataFrame:
    """Generic merge function for master data (customers/suppliers).

    Merges three data sources:
    - master_df: Contact info from Google Sheets (MÃ CTY / Thong tin KH)
    - debts_df: Debt summary (TỔNG HỢP / TỔNG CÔNG NỢ)
    - transactions_df: Aggregated transactions from staging

    Args:
        master_df: Master data with contact information
        debts_df: Debt summary data
        transactions_df: Transaction aggregation data
        name_column: Column name for entity names ("Tên khách hàng" or "Tên nhà cung cấp")
        debt_column: Column name for debt values ("Nợ cần thu hiện tại" or "Nợ cần trả hiện tại")

    Returns:
        Merged DataFrame with all sources combined
    """
    logger.info(f"Merging all data sources for {name_column}...")

    all_entities = set()

    if not master_df.empty and name_column in master_df.columns:
        all_entities |= set(master_df[name_column].dropna().unique())

    if not debts_df.empty and name_column in debts_df.columns:
        all_entities |= set(debts_df[name_column].dropna().unique())

    if not transactions_df.empty and name_column in transactions_df.columns:
        all_entities |= set(transactions_df[name_column].dropna().unique())

    all_entities = {c for c in all_entities if c and str(c).strip()}

    if not all_entities:
        logger.info(f"No entities found in any source for {name_column}")
        return pd.DataFrame()

    logger.info(f"Total unique {name_column}: {len(all_entities)}")

    result = pd.DataFrame({name_column: sorted(list(all_entities))})

    if not master_df.empty and name_column in master_df.columns:
        master_df = master_df.copy()
        master_df[name_column] = master_df[name_column].str.strip()
        result = result.merge(master_df, on=name_column, how="left")

    if not debts_df.empty and name_column in debts_df.columns:
        debts_df = debts_df.copy()
        debts_df[name_column] = debts_df[name_column].str.strip()
        result = result.merge(debts_df, on=name_column, how="left")

    if not transactions_df.empty and name_column in transactions_df.columns:
        transactions_df = transactions_df.copy()
        transactions_df[name_column] = transactions_df[name_column].str.strip()
        result = result.merge(transactions_df, on=name_column, how="left")

    if debt_column in result.columns:
        result[debt_column] = pd.to_numeric(
            result[debt_column], errors="coerce"
        ).fillna(0)

    result = result.fillna("")
    return result


def generate_entity_codes(
    df: pd.DataFrame,
    name_column: str,
    code_column: str,
    code_prefix: str,
    date_column: str = "first_date",
    amount_column: str = "total_amount",
) -> pd.DataFrame:
    """Generate unified entity codes (KH000001, NCC000002, ...).

    Sorts entities by:
    1. First transaction date (ascending)
    2. Total transaction amount (descending)
    3. Entity name (ascending)

    Args:
        df: DataFrame to process
        name_column: Column name for entity names
        code_column: Column name to store generated codes
        code_prefix: Prefix for codes ("KH" for customers, "NCC" for suppliers)
        date_column: Date column for sorting (default: "first_date")
        amount_column: Amount column for sorting (default: "total_amount")

    Returns:
        DataFrame with generated codes added
    """
    if df.empty:
        return df

    df = df.copy()

    if date_column in df.columns:
        df[date_column] = pd.to_datetime(df[date_column], errors="coerce")
    else:
        df[date_column] = pd.NaT

    if amount_column not in df.columns:
        df[amount_column] = 0
    df[amount_column] = pd.to_numeric(df[amount_column], errors="coerce").fillna(0)

    df = df.sort_values(
        by=[date_column, amount_column, name_column],
        ascending=[True, False, True],
        na_position="last",
    ).reset_index(drop=True)

    df[code_column] = df.index.map(lambda x: f"{code_prefix}{x + 1:06d}")

    logger.info(f"Generated {len(df)} {code_prefix} codes")
    return df
