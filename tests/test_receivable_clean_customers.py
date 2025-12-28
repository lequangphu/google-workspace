# -*- coding: utf-8 -*-
"""Tests for clean_customers module."""

import logging

import pandas as pd

from src.modules.receivable.clean_customers import (
    clean_phone_number,
    load_and_clean_data,
    split_phone_numbers,
    transform_data,
)

logger = logging.getLogger(__name__)


class TestPhoneProcessing:
    """Test phone number processing functions."""

    def test_clean_phone_number(self):
        """Test cleaning a single phone number."""
        result = clean_phone_number("0123456789.")
        assert result == "0123456789"

    def test_clean_phone_number_with_comma(self):
        """Test cleaning phone with trailing comma."""
        result = clean_phone_number("0987654321,")
        assert result == "0987654321"

    def test_clean_phone_number_empty(self):
        """Test cleaning empty phone."""
        result = clean_phone_number("")
        assert result == ""

    def test_split_phone_numbers_single(self):
        """Test splitting single phone number."""
        result = split_phone_numbers("0123456789")
        assert result == ["0123456789"]

    def test_split_phone_numbers_slash_delimited(self):
        """Test splitting phone numbers by slash."""
        result = split_phone_numbers("0123456789 / 0987654321")
        assert len(result) == 2
        assert "0123456789" in result
        assert "0987654321" in result

    def test_split_phone_numbers_dash_delimited(self):
        """Test splitting phone numbers by dash."""
        result = split_phone_numbers("0123456789 - 0987654321")
        assert len(result) == 2

    def test_split_phone_numbers_empty(self):
        """Test splitting empty phone."""
        result = split_phone_numbers("")
        assert result == []

    def test_split_phone_numbers_none(self):
        """Test splitting None."""
        result = split_phone_numbers(None)
        assert result == []


class TestDataCleaning:
    """Test data cleaning functions."""

    def test_load_and_clean_data_empty(self):
        """Test loading empty data."""
        result = load_and_clean_data([])
        assert result.empty

    def test_load_and_clean_data_with_header(self):
        """Test loading data with header row."""
        raw_data = [
            ["STT", "MÃ KH", "TÊN KHÁCH HÀNG"],
            [1, "KH001", "Customer One"],
            [2, "KH002", "Customer Two"],
        ]
        result = load_and_clean_data(raw_data)

        assert len(result) == 2
        assert "MÃ KH" in result.columns
        assert result.iloc[0]["MÃ KH"] == "KH001"

    def test_load_and_clean_data_removes_subheader(self):
        """Test removing sub-header row with column numbers."""
        raw_data = [
            ["STT", "MÃ KH", "TÊN KHÁCH HÀNG"],
            ["1", "2", "3"],  # Sub-header (as strings)
            ["4", "KH001", "Customer One"],
        ]
        result = load_and_clean_data(raw_data)

        # Sub-header should be removed
        assert len(result) == 1
        assert result.iloc[0]["MÃ KH"] == "KH001"


class TestTransformation:
    """Test data transformation."""

    def test_transform_data_column_selection(self):
        """Test that only mapped columns are selected."""
        df = pd.DataFrame(
            {
                "MÃ KH": ["KH001"],
                "TÊN KHÁCH HÀNG": ["Customer"],
                "Địa chỉ ": ["Address"],
                "Tel": ["0123456789"],
                "Extra Column": ["Extra"],  # Should be removed
            }
        )
        result = transform_data(df)

        assert "Mã khách hàng" in result.columns
        assert "Extra Column" not in result.columns

    def test_transform_data_removes_empty_customer_code(self):
        """Test removing rows with empty customer code."""
        df = pd.DataFrame(
            {
                "MÃ KH": ["KH001", "", "KH003"],
                "TÊN KHÁCH HÀNG": ["Customer 1", "Customer 2", "Customer 3"],
                "Địa chỉ ": ["Addr1", "Addr2", "Addr3"],
                "Tel": ["0123", "0124", "0125"],
            }
        )
        result = transform_data(df)

        assert len(result) == 2
        assert "KH001" in result["Mã khách hàng"].values
        assert "KH003" in result["Mã khách hàng"].values

    def test_transform_data_splits_phones(self):
        """Test that multiple phones are split into columns."""
        df = pd.DataFrame(
            {
                "MÃ KH": ["KH001"],
                "TÊN KHÁCH HÀNG": ["Customer"],
                "Địa chỉ ": ["Address"],
                "Tel": ["0123456789 / 0987654321"],
            }
        )
        result = transform_data(df)

        assert "Điện thoại" in result.columns
        assert "Điện thoại 2" in result.columns

    def test_transform_data_formats_phones_as_text(self):
        """Test that phone columns are formatted as text."""
        df = pd.DataFrame(
            {
                "MÃ KH": ["KH001"],
                "TÊN KHÁCH HÀNG": ["Customer"],
                "Địa chỉ ": ["Address"],
                "Tel": ["0123456789"],
            }
        )
        result = transform_data(df)

        # Phone should be formatted with leading apostrophe for text
        phone_value = result["Điện thoại"].iloc[0]
        assert phone_value.startswith("'")


class TestCustomerProcessing:
    """Test customer processing with mock data."""

    def test_process_with_mock_data(self):
        """Test full processing pipeline with mock data."""
        from src.modules.receivable.clean_customers import (
            load_and_clean_data,
            transform_data,
        )

        raw_data = [
            ["STT", "MÃ KH", "TÊN KHÁCH HÀNG", "Địa chỉ ", "Tel"],
            ["1", "2", "3", "4", "5"],  # Sub-header
            ["1", "KH001", "Customer One", "123 Main St", "0123456789"],
            ["2", "KH002", "Customer Two", "456 Oak Ave", "0987654321 / 0111111111"],
        ]

        df = load_and_clean_data(raw_data)
        assert len(df) == 2

        df = transform_data(df)
        assert len(df) == 2
        assert "Mã khách hàng" in df.columns
        assert "Điện thoại 2" in df.columns

        logger.info(f"Successfully processed {len(df)} customers")
