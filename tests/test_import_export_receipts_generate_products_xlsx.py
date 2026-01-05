# -*- coding: utf-8 -*-
"""Tests for generate_products_xlsx module."""

import logging
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from src.modules.import_export_receipts.generate_products_xlsx import (
    calculate_max_selling_price,
    find_latest_file,
    get_latest_inventory,
    get_sort_key,
    standardize_brand_names,
)

logger = logging.getLogger(__name__)


class TestHelperFunctions:
    """Test helper functions."""

    def test_get_sort_key_with_date(self):
        """Test sort key generation with valid date."""
        nhap_df = pd.DataFrame(
            {
                "Mã hàng": ["A", "A", "B"],
                "Ngày": ["2025-01-15", "2025-01-20", "2025-02-01"],
                "Mã chứng từ": ["CT001", "CT002", "CT003"],
                "Thành tiền": [1000, 2000, 3000],
            }
        )

        result = get_sort_key("A", nhap_df)
        assert result[0] is not pd.NaT
        assert result[1] == "CT001"
        assert result[2] == 1000

    def test_get_sort_key_product_not_found(self):
        """Test sort key for non-existent product."""
        nhap_df = pd.DataFrame(
            {
                "Mã hàng": ["A"],
                "Ngày": ["2025-01-15"],
                "Mã chứng từ": ["CT001"],
                "Thành tiền": [1000],
            }
        )

        result = get_sort_key("X", nhap_df)
        assert result == (pd.NaT, "", 0)

    def test_standardize_brand_names(self):
        """Test brand name standardization."""
        df = pd.DataFrame(
            {"Tên hàng": ["CHENGSIN tire", "michenlin wheel", "caosumina product"]}
        )
        result = standardize_brand_names(df)

        assert "CHENGSHIN" in result["Tên hàng"].iloc[0]
        assert "MICHELIN" in result["Tên hàng"].iloc[1]
        assert "CASUMINA" in result["Tên hàng"].iloc[2]

    def test_standardize_brand_names_no_ten_hang_column(self):
        """Test standardization when Tên hàng column is missing."""
        df = pd.DataFrame({"Other": [1, 2, 3]})
        result = standardize_brand_names(df)
        assert result.equals(df)


class TestCalculateMaxSellingPrice:
    """Test max selling price calculation."""

    def test_calculate_max_selling_price(self):
        """Test calculating max selling price per product."""
        xuat_df = pd.DataFrame(
            {
                "Mã hàng": ["A", "A", "B", "B"],
                "Số lượng": [10, 10, 5, 5],
                "Thành tiền": [100, 150, 50, 60],
            }
        )

        result = calculate_max_selling_price(xuat_df)

        assert len(result) == 2
        product_a = result[result["Mã hàng"] == "A"]
        product_b = result[result["Mã hàng"] == "B"]

        assert product_a["Giá bán"].iloc[0] == 15.0
        assert product_b["Giá bán"].iloc[0] == 12.0

    def test_calculate_max_selling_price_empty(self):
        """Test with empty DataFrame."""
        result = calculate_max_selling_price(pd.DataFrame())
        assert result.empty


class TestFindLatestFile:
    """Test find_latest_file function."""

    def test_find_latest_file(self, tmp_path):
        """Test finding the latest file by modification time."""
        file1 = tmp_path / "file1.csv"
        file2 = tmp_path / "file2.csv"
        file1.write_text("a")
        file2.write_text("b")

        result = find_latest_file(tmp_path, "*.csv")

        assert result == file2

    def test_find_latest_file_no_match(self, tmp_path):
        """Test when no files match pattern."""
        with pytest.raises(FileNotFoundError):
            find_latest_file(tmp_path, "*.csv")


class TestGetLatestInventory:
    """Test get_latest_inventory function."""

    def test_get_latest_inventory(self, tmp_path):
        """Test extracting latest month inventory per product."""
        xnt_file = tmp_path / "xuat_nhap_ton_2025_01_12.csv"
        xnt_file.write_text("""Mã hàng,Ngày,Số lượng cuối kỳ,Đơn giá cuối kỳ
A,2025-01-01,100,10.0
A,2025-02-01,80,11.0
B,2025-01-01,50,20.0
B,2025-02-01,45,21.0
""")

        result = get_latest_inventory(tmp_path)

        assert len(result) == 2
        product_a = result[result["Mã hàng"] == "A"]
        product_b = result[result["Mã hàng"] == "B"]

        assert product_a["Số lượng cuối kỳ"].iloc[0] == 80
        assert product_a["Đơn giá cuối kỳ"].iloc[0] == 11.0
        assert product_b["Số lượng cuối kỳ"].iloc[0] == 45
        assert product_b["Đơn giá cuối kỳ"].iloc[0] == 21.0

    def test_get_latest_inventory_no_files(self, tmp_path):
        """Test when no XNT files exist."""
        with pytest.raises(FileNotFoundError):
            get_latest_inventory(tmp_path)

    def test_get_latest_inventory_no_date_column(self, tmp_path):
        """Test when XNT file has no Ngày column."""
        xnt_file = tmp_path / "xuat_nhap_ton_2025_01_12.csv"
        xnt_file.write_text("""Mã hàng,Số lượng cuối kỳ,Đơn giá cuối kỳ
A,100,10.0
B,50,20.0
""")

        with pytest.raises(ValueError):
            get_latest_inventory(tmp_path)


class TestProcessIntegration:
    """Integration tests for process() function."""

    def test_process_creates_output_file(self, tmp_path):
        """Test that process() creates Products.xlsx."""
        staging_dir = tmp_path / "import_export"
        staging_dir.mkdir()

        export_dir = tmp_path / "export"
        export_dir.mkdir()

        nhap_file = staging_dir / "Chi tiết nhập_2025_01.csv"
        nhap_file.write_text("""Mã chứng từ,Ngày,Mã hàng,Tên hàng,Số lượng,Thành tiền
CT001,2025-01-15,A,Product A,100,10000
CT002,2025-01-16,B,Product B,50,5000
""")

        xuat_file = staging_dir / "Chi tiết xuất_2025_01.csv"
        xuat_file.write_text("""Mã chứng từ,Ngày,Tên khách hàng,Mã hàng,Tên hàng,Số lượng,Thành tiền
CT003,2025-01-17,Khách A,A,Product A,20,2500
CT004,2025-01-18,Khách B,B,Product B,10,1200
""")

        xnt_file = staging_dir / "xuat_nhap_ton_2025_01_12.csv"
        xnt_file.write_text("""Mã hàng,Ngày,Số lượng cuối kỳ,Đơn giá cuối kỳ
A,2025-01-31,80,100.0
B,2025-01-31,40,120.0
""")

        from src.modules.import_export_receipts.generate_products_xlsx import (
            CONFIG,
            process as generate_products,
        )

        with (
            patch(
                "src.modules.import_export_receipts.generate_products_xlsx.fetch_product_lookup"
            ) as mock_lookup,
            patch.dict(CONFIG, {"export_dir": export_dir}, clear=False),
        ):
            mock_lookup.return_value = pd.DataFrame(
                {
                    "Mã hàng": ["A", "B"],
                    "Nhóm hàng(3 Cấp)": ["Category A", "Category B"],
                    "Thương hiệu": ["Brand A", "Brand B"],
                    "Tên hàng": ["Product A", "Product B"],
                }
            )

            result = generate_products(staging_dir=staging_dir)

            assert result is not None
            assert result.exists()
            assert result.name == "Products.xlsx"
            assert result.parent == export_dir

    def test_process_missing_staging_dir(self):
        """Test that process() returns None when staging dir missing."""
        from src.modules.import_export_receipts.generate_products_xlsx import (
            process as generate_products,
        )

        result = generate_products(staging_dir=Path("/nonexistent"))
        assert result is None
