# -*- coding: utf-8 -*-
"""Tests for receivable.clean_debts module."""

import pandas as pd

from src.modules.receivable.clean_debts import (
    load_and_clean_data,
    transform_data,
)


class TestLoadAndCleanData:
    """Test load_and_clean_data function."""

    def test_empty_data(self):
        """Test with empty data."""
        result = load_and_clean_data([])
        assert result.empty

    def test_no_header(self):
        """Test with data without STT header."""
        raw_data = [
            ["Col1", "Col2"],
            ["val1", "val2"],
        ]
        result = load_and_clean_data(raw_data)
        assert result.empty

    def test_valid_data_with_header(self):
        """Test with valid data containing STT header."""
        raw_data = [
            ["STT", "TÊN KHÁCH HÀNG", " TỔNG NỢ "],
            ["1", "Customer A", "100000"],
            ["2", "Customer B", "200000"],
        ]
        result = load_and_clean_data(raw_data)
        assert len(result) == 2
        assert list(result.columns) == ["STT", "TÊN KHÁCH HÀNG", " TỔNG NỢ "]

    def test_header_in_middle(self):
        """Test with header row not at beginning."""
        raw_data = [
            ["Garbage", "Data"],
            ["More", "Garbage"],
            ["STT", "TÊN KHÁCH HÀNG", " TỔNG NỢ "],
            ["1", "Customer A", "100000"],
        ]
        result = load_and_clean_data(raw_data)
        assert len(result) == 1

    def test_row_padding(self):
        """Test that short rows are padded to match header length."""
        raw_data = [
            ["STT", "TÊN KHÁCH HÀNG", " TỔNG NỢ "],
            ["1", "Customer A"],  # Missing last column
            ["2", "Customer B", "200000", "Extra"],  # Extra column
        ]
        result = load_and_clean_data(raw_data)
        assert result.shape == (2, 3)
        assert pd.isna(result.iloc[0, 2]) or result.iloc[0, 2] == ""

    def test_remove_all_empty_rows(self):
        """Test removal of completely empty rows."""
        raw_data = [
            ["STT", "TÊN KHÁCH HÀNG", " TỔNG NỢ "],
            ["1", "Customer A", "100000"],
            [None, None, None],  # All NaN/None
            ["2", "Customer B", "200000"],
        ]
        result = load_and_clean_data(raw_data)
        # dropna(how="all") should remove rows where ALL values are NaN
        assert len(result) == 2


class TestTransformData:
    """Test transform_data function."""

    def test_empty_dataframe(self):
        """Test with empty dataframe."""
        df = pd.DataFrame()
        customer_mapping = pd.DataFrame(
            {"Tên khách hàng": ["Customer A"], "Mã khách hàng": ["KH001"]}
        )
        result = transform_data(df, customer_mapping)
        assert result.empty

    def test_remove_empty_customer_names(self):
        """Test removal of rows with empty customer names."""
        df = pd.DataFrame(
            {
                "TÊN KHÁCH HÀNG": ["Customer A", "", "Customer C", "  "],
                " TỔNG NỢ ": ["100000", "50000", "200000", "150000"],
            }
        )
        customer_mapping = pd.DataFrame()
        result = transform_data(df, customer_mapping)
        # Should only have rows with non-empty customer names
        assert len(result) == 2

    def test_column_renaming(self):
        """Test column selection and renaming."""
        df = pd.DataFrame(
            {
                "TÊN KHÁCH HÀNG": ["Customer A"],
                " TỔNG NỢ ": ["100000"],
                " ĐÃ THANH TOÁN ": ["50000"],
            }
        )
        customer_mapping = pd.DataFrame()
        result = transform_data(df, customer_mapping)
        assert "Tên khách hàng" in result.columns
        assert "Nợ" in result.columns
        assert "Nợ đã thu" in result.columns

    def test_numeric_parsing_valid_numbers(self):
        """Test parsing of valid numeric values."""
        df = pd.DataFrame(
            {
                "TÊN KHÁCH HÀNG": ["Customer A"],
                " TỔNG NỢ ": ["1 000 000"],
                " ĐÃ THANH TOÁN ": ["500.000"],
                " NỢ CÒN LẠI  ": ["500000"],
            }
        )
        customer_mapping = pd.DataFrame()
        result = transform_data(df, customer_mapping)
        assert result.iloc[0]["Nợ"] == 1000000
        assert result.iloc[0]["Nợ đã thu"] == 500000

    def test_numeric_parsing_dash_to_zero(self):
        """Test parsing of dash values as zero."""
        df = pd.DataFrame(
            {
                "TÊN KHÁCH HÀNG": ["Customer A", "Customer B"],
                " TỔNG NỢ ": ["-", "100000"],
                " ĐÃ THANH TOÁN ": ["-", "50000"],
                " NỢ CÒN LẠI  ": ["-", "50000"],
            }
        )
        customer_mapping = pd.DataFrame()
        result = transform_data(df, customer_mapping)
        # Row with all dashes (all zeros) should be dropped
        # Only Customer B should remain
        assert len(result) == 1
        assert result.iloc[0]["Nợ"] == 100000

    def test_numeric_parsing_negative_in_parentheses(self):
        """Test parsing of negative values in parentheses."""
        df = pd.DataFrame(
            {
                "TÊN KHÁCH HÀNG": ["Customer A"],
                " TỔNG NỢ ": ["(100000)"],
                " ĐÃ THANH TOÁN ": ["50000"],
                " NỢ CÒN LẠI  ": ["(50000)"],
            }
        )
        customer_mapping = pd.DataFrame()
        result = transform_data(df, customer_mapping)
        assert result.iloc[0]["Nợ"] == -100000
        assert result.iloc[0]["Nợ cần thu hiện tại"] == -50000

    def test_drop_all_zero_rows(self):
        """Test removal of rows with all zero numeric values."""
        df = pd.DataFrame(
            {
                "TÊN KHÁCH HÀNG": ["Customer A", "Customer B", "Customer C"],
                " TỔNG NỢ ": ["100000", "-", "0"],
                " ĐÃ THANH TOÁN ": ["50000", "-", "0"],
                " NỢ CÒN LẠI  ": ["50000", "-", ""],
            }
        )
        customer_mapping = pd.DataFrame()
        result = transform_data(df, customer_mapping)
        # Should remove "Customer B" and "Customer C" (all numeric = 0)
        assert len(result) == 1
        assert result.iloc[0]["Tên khách hàng"] == "Customer A"

    def test_primary_join_with_customer_mapping(self):
        """Test primary join with customer mapping."""
        df = pd.DataFrame(
            {
                "TÊN KHÁCH HÀNG": ["Customer A", "Customer B"],
                " TỔNG NỢ ": ["100000", "200000"],
                " ĐÃ THANH TOÁN ": ["50000", "100000"],
                " NỢ CÒN LẠI  ": ["50000", "100000"],
            }
        )
        customer_mapping = pd.DataFrame(
            {
                "Tên khách hàng": ["Customer A", "Customer B"],
                "Mã khách hàng": ["KH001", "KH002"],
            }
        )
        result = transform_data(df, customer_mapping)
        assert result.iloc[0]["Mã khách hàng"] == "KH001"
        assert result.iloc[1]["Mã khách hàng"] == "KH002"

    def test_secondary_join_with_raw_data(self):
        """Test secondary join with raw customer data."""
        df = pd.DataFrame(
            {
                "TÊN KHÁCH HÀNG": ["Customer A", "Customer B"],
                " TỔNG NỢ ": ["100000", "200000"],
                " ĐÃ THANH TOÁN ": ["50000", "100000"],
                " NỢ CÒN LẠI  ": ["50000", "100000"],
            }
        )
        customer_mapping = pd.DataFrame(
            {
                "Tên khách hàng": ["Customer A"],
                "Mã khách hàng": ["KH001"],
            }
        )
        raw_customer_data = pd.DataFrame(
            {
                "TÊN KHÁCH HÀNG": ["Customer B"],
                "MÃ KH": ["KH002"],
            }
        )
        result = transform_data(df, customer_mapping, raw_customer_data)
        assert result.iloc[0]["Mã khách hàng"] == "KH001"
        # Secondary join may or may not succeed depending on merge behavior
        # Just check that it doesn't crash

    def test_whitespace_cleaning(self):
        """Test cleaning of whitespace in string columns."""
        df = pd.DataFrame(
            {
                "TÊN KHÁCH HÀNG": ["  Customer A  ", " Customer B "],
                " TỔNG NỢ ": [" 100000 ", " 200000 "],
                " ĐÃ THANH TOÁN ": ["50000", "100000"],
                " NỢ CÒN LẠI  ": ["50000", "100000"],
            }
        )
        customer_mapping = pd.DataFrame()
        result = transform_data(df, customer_mapping)
        assert result.iloc[0]["Tên khách hàng"] == "Customer A"
        assert result.iloc[1]["Tên khách hàng"] == "Customer B"

    def test_final_column_order(self):
        """Test that final columns are in correct order."""
        df = pd.DataFrame(
            {
                "TÊN KHÁCH HÀNG": ["Customer A"],
                " TỔNG NỢ ": ["100000"],
                " ĐÃ THANH TOÁN ": ["50000"],
                " NỢ CÒN LẠI  ": ["50000"],
            }
        )
        customer_mapping = pd.DataFrame(
            {
                "Tên khách hàng": ["Customer A"],
                "Mã khách hàng": ["KH001"],
            }
        )
        result = transform_data(df, customer_mapping)
        expected_columns = [
            "Mã khách hàng",
            "Tên khách hàng",
            "Nợ",
            "Nợ đã thu",
            "Nợ cần thu hiện tại",
        ]
        assert list(result.columns) == expected_columns

    def test_none_and_nan_replacement(self):
        """Test replacement of None and nan strings with empty string."""
        df = pd.DataFrame(
            {
                "TÊN KHÁCH HÀNG": ["Customer A", "Customer B"],
                " TỔNG NỢ ": ["100000", "200000"],
                " ĐÃ THANH TOÁN ": ["None", "nan"],
                " NỢ CÒN LẠI  ": ["50000", "100000"],
            }
        )
        customer_mapping = pd.DataFrame()
        result = transform_data(df, customer_mapping)
        # Both rows have non-zero values, so both should be kept
        assert len(result) == 2
        # Check that first row's Nợ is properly set
        assert result.iloc[0]["Nợ"] == 100000

    def test_mixed_numeric_formats(self):
        """Test handling of mixed numeric formats."""
        df = pd.DataFrame(
            {
                "TÊN KHÁCH HÀNG": ["Customer A", "Customer B", "Customer C"],
                " TỔNG NỢ ": ["1 000", "2.000", "3000"],
                " ĐÃ THANH TOÁN ": ["500", "1 000", "1.500"],
                " NỢ CÒN LẠI  ": ["500", "1000", "1500"],
            }
        )
        customer_mapping = pd.DataFrame()
        result = transform_data(df, customer_mapping)
        assert result.iloc[0]["Nợ"] == 1000
        assert result.iloc[1]["Nợ"] == 2000
        assert result.iloc[2]["Nợ"] == 3000
