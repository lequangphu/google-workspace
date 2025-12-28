"""Tests for src/modules/import_export_receipts/clean_receipts_purchase.py."""

import pandas as pd

from src.modules.import_export_receipts.clean_receipts_purchase import (
    combine_headers,
    is_float_check,
    try_parse_date,
    clean_text_column,
    standardize_column_types,
)


class TestCombineHeaders:
    """Test combine_headers function."""

    def test_combine_headers_basic(self):
        """Combine two header rows correctly."""
        # Create headers with correct length (29 elements for row1, 29 for row2)
        header_row1 = ["CT"] + ["col"] * 28
        header_row2 = ["col"] * 29

        headers, indices = combine_headers(header_row1, header_row2)

        assert len(headers) == 20
        assert len(indices) == 20
        assert indices[0] == 0

    def test_combine_headers_normalizes_whitespace(self):
        """Normalize whitespace in headers."""
        header_row1 = ["Chứng  từ"] + ["col"] * 28
        header_row2 = ["PNK ", "  Ngày"] + ["col"] * 27

        headers, indices = combine_headers(header_row1, header_row2)

        assert "Chứng từ_PNK" in headers[0]
        assert "Chứng từ_Ngày" in headers[1]


class TestIsFloatCheck:
    """Test is_float_check function."""

    def test_valid_float_string(self):
        """String convertible to float."""
        assert is_float_check("3.14") is True
        assert is_float_check("42") is True
        assert is_float_check("-5.5") is True

    def test_invalid_float_string(self):
        """String not convertible to float."""
        assert is_float_check("abc") is False
        assert is_float_check("") is False

    def test_numeric_types(self):
        """Numeric types."""
        assert is_float_check(3.14) is True
        assert is_float_check(42) is True
        assert is_float_check(None) is False


class TestTryParseDate:
    """Test try_parse_date function."""

    def test_valid_format(self):
        """Parse valid date with format."""
        result = try_parse_date("2023-01-15", "%Y-%m-%d")
        assert pd.notna(result)
        assert result.year == 2023
        assert result.month == 1
        assert result.day == 15

    def test_invalid_format(self):
        """Return NaT for invalid date."""
        result = try_parse_date("2023-13-45", "%Y-%m-%d")
        assert pd.isna(result)


class TestCleanTextColumn:
    """Test clean_text_column function."""

    def test_strip_whitespace(self):
        """Strip leading and trailing whitespace."""
        series = pd.Series(["  hello  ", "  world  "])
        result = clean_text_column(series)
        assert result[0] == "hello"
        assert result[1] == "world"

    def test_normalize_internal_spaces(self):
        """Normalize internal multiple spaces."""
        series = pd.Series(["hello   world", "foo  bar"])
        result = clean_text_column(series)
        assert result[0] == "hello world"
        assert result[1] == "foo bar"

    def test_mixed_whitespace(self):
        """Handle mixed leading/trailing and internal spaces."""
        series = pd.Series(["  hello   world  ", "  test  data  "])
        result = clean_text_column(series)
        assert result[0] == "hello world"
        assert result[1] == "test data"


class TestStandardizeColumnTypes:
    """Test standardize_column_types function."""

    def test_text_columns_uppercase(self):
        """Convert Mã hàng to uppercase."""
        df = pd.DataFrame({"Mã hàng": ["a1", "b2", "c3"]})
        result = standardize_column_types(df)
        assert result["Mã hàng"][0] == "A1"
        assert result["Mã hàng"][1] == "B2"

    def test_numeric_columns(self):
        """Convert numeric columns correctly."""
        df = pd.DataFrame(
            {
                "Số lượng": ["10", "20", "abc"],
                "Đơn giá": ["100.5", "200.3", "invalid"],
            }
        )
        result = standardize_column_types(df)
        assert pd.isna(result["Số lượng"][2])
        assert pd.isna(result["Đơn giá"][2])

    def test_integer_columns(self):
        """Convert Tháng and Năm to Int64."""
        df = pd.DataFrame({"Tháng": ["1", "2", "12"], "Năm": ["2023", "2024", "2025"]})
        result = standardize_column_types(df)
        assert result["Tháng"][0] == 1
        assert result["Năm"][0] == 2023
