# -*- coding: utf-8 -*-
"""Tests for clean_receipts_sale module."""

import pandas as pd
from pathlib import Path
from src.modules.import_export_receipts.clean_receipts_sale import (
    extract_year_month_from_filename,
    combine_headers,
    process_dates,
    clean_text_column,
    standardize_column_types,
    fill_and_adjust_rows,
    generate_output_filename,
)


class TestExtractYearMonth:
    """Test year/month extraction from filename."""

    def test_valid_filename(self):
        """Test extraction from valid filename format."""
        filepath = Path("2024_03_CT.XUAT.csv")
        year, month = extract_year_month_from_filename(filepath)
        assert year == 2024
        assert month == 3

    def test_single_digit_month(self):
        """Test extraction with single digit month."""
        filepath = Path("2023_5_CT.XUAT.csv")
        year, month = extract_year_month_from_filename(filepath)
        assert year == 2023
        assert month == 5

    def test_invalid_filename(self):
        """Test extraction from invalid filename."""
        filepath = Path("invalid_file.csv")
        year, month = extract_year_month_from_filename(filepath)
        assert year is None
        assert month is None


class TestCombineHeaders:
    """Test header combining logic."""

    def test_combine_distinct_headers(self):
        """Test combining distinct header rows."""
        row1 = pd.Series(["Chứng từ", "Ngày", "Khách hàng"])
        row2 = pd.Series(["PXK", "01/01/2024", "Tên KH"])
        result = combine_headers(row1, row2)
        assert "Chứng từ_PXK" in result
        assert "Ngày_01/01/2024" in result
        assert "Khách hàng_Tên KH" in result

    def test_combine_with_empty_values(self):
        """Test combining with empty/NA values."""
        row1 = pd.Series(["Chứng từ", "", "Khách hàng"])
        row2 = pd.Series(["", "Ngày", ""])
        result = combine_headers(row1, row2)
        assert "Chứng từ" in result
        assert "Ngày" in result
        assert "Khách hàng" in result

    def test_header_uniqueness(self):
        """Test that duplicate headers get numbered."""
        row1 = pd.Series(["Số lượng", "Số lượng", "Số lượng"])
        row2 = pd.Series(["Bán lẻ", "Bán sì", "Kho"])
        result = combine_headers(row1, row2)
        assert len(result) == len(set(result))  # All unique


class TestCleanText:
    """Test text cleaning function."""

    def test_strip_whitespace(self):
        """Test stripping leading/trailing whitespace."""
        series = pd.Series(["  text  ", "\ttab\t", "\nline\n"])
        result = clean_text_column(series)
        assert result.iloc[0] == "text"
        assert result.iloc[1] == "tab"
        assert result.iloc[2] == "line"

    def test_normalize_internal_spaces(self):
        """Test normalizing internal spaces."""
        series = pd.Series(["text   with   spaces", "tab\t\ttab"])
        result = clean_text_column(series)
        assert result.iloc[0] == "text with spaces"
        assert result.iloc[1] == "tab tab"


class TestStandardizeColumnTypes:
    """Test column type standardization."""

    def test_text_columns_uppercase(self):
        """Test that Mã hàng is uppercased."""
        df = pd.DataFrame({"Mã hàng": ["abc", "def"]})
        result = standardize_column_types(df)
        assert result["Mã hàng"].iloc[0] == "ABC"
        assert result["Mã hàng"].iloc[1] == "DEF"

    def test_numeric_columns(self):
        """Test numeric column conversion."""
        df = pd.DataFrame({"Số lượng": ["10", "20.5", "invalid"]})
        result = standardize_column_types(df)
        assert result["Số lượng"].iloc[0] == 10.0
        assert result["Số lượng"].iloc[1] == 20.5
        assert pd.isna(result["Số lượng"].iloc[2])

    def test_integer_columns(self):
        """Test integer column conversion."""
        df = pd.DataFrame({"Năm": ["2024", "2023"], "Tháng": ["3", "12"]})
        result = standardize_column_types(df)
        assert result["Năm"].dtype == "Int64"
        assert result["Tháng"].dtype == "Int64"


class TestFillAndAdjustRows:
    """Test row filling and quantity adjustment."""

    def test_fill_null_prices(self):
        """Test filling null prices with 0 for non-null quantities."""
        df = pd.DataFrame(
            {
                "Số lượng": [10, 20, None],
                "Đơn giá": [None, None, 100],
                "Thành tiền": [None, 200, None],
            }
        )
        result = fill_and_adjust_rows(df)
        assert result.iloc[0]["Đơn giá"] == 0
        assert result.iloc[1]["Đơn giá"] == 0
        assert result.iloc[0]["Thành tiền"] == 0

    def test_fill_null_customer(self):
        """Test filling null customer names."""
        df = pd.DataFrame(
            {
                "Số lượng": [10, 20],
                "Tên khách hàng": [None, "Customer"],
            }
        )
        result = fill_and_adjust_rows(df)
        assert result.iloc[0]["Tên khách hàng"] == "KHÁCH LẺ"
        assert result.iloc[1]["Tên khách hàng"] == "Customer"

    def test_adjust_negative_prices(self):
        """Test adjusting quantities with negative prices."""
        df = pd.DataFrame(
            {
                "Số lượng": [10, -10],
                "Đơn giá": [-100, -100],
            }
        )
        result = fill_and_adjust_rows(df)
        assert result.iloc[0]["Số lượng"] == -10  # Flipped
        assert result.iloc[0]["Đơn giá"] == 100  # Made positive


class TestGenerateOutputFilename:
    """Test output filename generation."""

    def test_filename_generation(self):
        """Test generating filename from date range."""
        df = pd.DataFrame(
            {
                "Năm": [2024, 2024, 2024],
                "Tháng": [1, 2, 3],
            }
        )
        filename = generate_output_filename(df)
        assert "Chi tiết xuất" in filename
        assert "2024-01" in filename
        assert "2024-03" in filename

    def test_filename_generation_single_month(self):
        """Test filename when all data from same month."""
        df = pd.DataFrame(
            {
                "Năm": [2024, 2024],
                "Tháng": [5, 5],
            }
        )
        filename = generate_output_filename(df)
        assert "Chi tiết xuất 2024-05_2024-05.csv" == filename


class TestProcessDates:
    """Test date processing logic."""

    def test_parse_standard_date_format(self):
        """Test parsing dates in DD/MM/YYYY format."""
        df = pd.DataFrame({"Ngày": ["15/03/2024", "01/01/2024"]})
        result = process_dates(df, 2024, 3)
        assert result["Ngày"].iloc[0] == "2024-03-15"
        assert result["Ngày"].iloc[1] == "2024-01-01"

    def test_parse_day_only(self):
        """Test parsing day-only values with year/month from filename."""
        df = pd.DataFrame({"Ngày": ["15", "05"]})
        result = process_dates(df, 2024, 3)
        assert result["Ngày"].iloc[0] == "2024-03-15"
        assert result["Ngày"].iloc[1] == "2024-03-05"

    def test_handle_standard_date(self):
        """Test standard date parsing."""
        df = pd.DataFrame({"Ngày": ["15/03/2024"]})
        result = process_dates(df, 2024, 3)
        assert result["Ngày"].iloc[0] == "2024-03-15"
