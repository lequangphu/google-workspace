# -*- coding: utf-8 -*-
"""
DEPRECATED: This test file is deprecated as of December 2025.

Tests have been migrated to:
- tests/test_import_export_receipts_generate_products_xlsx.py

This file will be removed on 2026-01-14 (2 weeks from migration).

---

Tests for extract_products module.
"""

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest

from src.modules.import_export_receipts.extract_products import (
    aggregate_by_year,
    calculate_fifo_cost,
    find_input_file,
    get_longest_name,
    get_sort_key,
    standardize_brand_names,
)

logger = logging.getLogger(__name__)


class TestHelperFunctions:
    """Test helper functions."""

    def test_get_longest_name(self):
        """Test getting longest product name."""
        df = pd.DataFrame({"Tên hàng": ["Short", "This is a longer name", "Medium"]})
        result = get_longest_name(df)
        assert result == "This is a longer name"

    def test_get_longest_name_with_nans(self):
        """Test with NaN values."""
        df = pd.DataFrame({"Tên hàng": [None, "Valid name", "Another"]})
        result = get_longest_name(df)
        assert result == "Valid name"

    def test_standardize_brand_names(self):
        """Test brand name standardization."""
        df = pd.DataFrame(
            {"Tên hàng": ["CHENGSIN tire", "michenlin wheel", "caosumina"]}
        )
        result = standardize_brand_names(df)

        assert "CHENGSHIN" in result["Tên hàng"].iloc[0]
        assert "MICHELIN" in result["Tên hàng"].iloc[1]
        assert "CASUMINA" in result["Tên hàng"].iloc[2]

    def test_aggregate_by_year(self):
        """Test year aggregation."""
        df = pd.DataFrame(
            {
                "Số lượng": [10, 20, 15],
                "Thành tiền": [100, 200, 150],
                "Năm": [2023, 2023, 2024],
            }
        )
        result = aggregate_by_year(df, include_year_breakdown=True)

        assert result["Tổng số lượng"] == 45
        assert result["Tổng thành tiền"] == 450
        assert result["Tổng số lượng - 2023"] == 30
        assert result["Tổng số lượng - 2024"] == 15

    def test_get_sort_key(self):
        """Test sort key generation."""
        df = pd.DataFrame(
            {
                "Ngày": ["2023-01-15"],
                "Mã chứng từ": ["CT001"],
                "Thành tiền": [1000.0],
            }
        )
        result = get_sort_key(df)

        assert len(result) == 3
        assert str(result[1]) == "CT001"
        assert result[2] == 1000.0


class TestFIFOCost:
    """Test FIFO costing logic."""

    def test_calculate_fifo_cost_simple(self):
        """Test FIFO cost with simple data."""
        nhap_df = pd.DataFrame(
            {
                "Ngày": ["2023-01-01", "2023-01-02"],
                "Số lượng": [10.0, 10.0],
                "Thành tiền": [100.0, 120.0],
            }
        )

        result = calculate_fifo_cost(nhap_df, 15.0)

        # FIFO: oldest sold first, so remaining 15 units come from:
        # - 10 from newest batch at 12/unit = 120
        # - 5 from second batch at 10/unit = 50
        # Total cost: 170, per unit: 170/15 = 11.33
        assert result["total_cost_remaining"] > 0
        assert result["unit_cost_fifo"] > 0

    def test_calculate_fifo_cost_zero_remaining(self):
        """Test FIFO cost with zero remaining quantity."""
        nhap_df = pd.DataFrame(
            {
                "Ngày": ["2023-01-01"],
                "Số lượng": [10.0],
                "Thành tiền": [100.0],
            }
        )

        result = calculate_fifo_cost(nhap_df, 0)

        assert result["total_cost_remaining"] == 0
        assert result["unit_cost_fifo"] == 0


class TestFindInputFile:
    """Test file finding logic."""

    def test_find_input_file_not_found(self):
        """Test error when file not found."""
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            with pytest.raises(FileNotFoundError):
                find_input_file(temp_path, "*.csv")

    def test_find_input_file_latest(self):
        """Test finding latest file by modification time."""
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test files
            file1 = temp_path / "data_2023.csv"
            file2 = temp_path / "data_2024.csv"

            file1.write_text("test1")
            file2.write_text("test2")

            # file2 should be latest
            result = find_input_file(temp_path, "data_*.csv")

            assert result.name == "data_2024.csv"


class TestProductExtraction:
    """Test product extraction with real data."""

    def test_process_with_real_data(self):
        """Test extraction pipeline with real staged data."""
        staging_dir = Path.cwd() / "data" / "01-staging" / "import_export"

        if not staging_dir.exists():
            pytest.skip(f"Staging directory not found: {staging_dir}")

        # Check if required files exist
        nhap_files = list(staging_dir.glob("chi_tiet_nhap*.csv"))
        xuat_files = list(staging_dir.glob("chi_tiet_xuat*.csv"))

        if not nhap_files or not xuat_files:
            pytest.skip("Required staged receipt files not found")

        # Import here to avoid issues when staging dir doesn't exist
        from src.modules.import_export_receipts.extract_products import process

        result = process(staging_dir)

        assert result is not None
        assert result.exists()

        # Check output files
        assert (result / "product_info.csv").exists()

        logger.info(f"Successfully extracted products to {result}")
