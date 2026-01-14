# -*- coding: utf-8 -*-
"""Tests for upload_cleaned_to_sheets module."""

import pandas as pd
import pytest
from pathlib import Path
from src.modules.import_export_receipts.upload_cleaned_to_sheets import (
    prepare_df_for_upload,
    validate_years,
    split_cleaned_data_by_period,
)


class TestValidateYears:
    """Test year validation function."""

    def test_single_year(self):
        """Test validation of single year."""
        result = validate_years("2025")
        assert result == ["2025"]

    def test_multiple_years(self):
        """Test validation of multiple comma-separated years."""
        result = validate_years("2024,2025")
        assert result == ["2024", "2025"]

    def test_unordered_years(self):
        """Test that years are sorted regardless of input order."""
        result = validate_years("2025,2024,2023")
        assert result == ["2023", "2024", "2025"]

    def test_year_with_whitespace(self):
        """Test that whitespace around years is trimmed."""
        result = validate_years("2024, 2025 , 2026")
        assert result == ["2024", "2025", "2026"]

    def test_invalid_year_not_digits(self):
        """Test that non-digit years raise ValueError."""
        with pytest.raises(ValueError, match="Invalid year"):
            validate_years("abcd")

    def test_invalid_year_not_4_digits(self):
        """Test that years with fewer than 4 digits raise ValueError."""
        with pytest.raises(ValueError, match="Invalid year"):
            validate_years("25")

    def test_invalid_year_more_than_4_digits(self):
        """Test that years with more than 4 digits raise ValueError."""
        with pytest.raises(ValueError, match="Invalid year"):
            validate_years("20255")

    def test_invalid_mixed_years(self):
        """Test that mixed valid and invalid years raise ValueError."""
        with pytest.raises(ValueError, match="Invalid year 'abc'"):
            validate_years("2024,abc,2025")

    def test_empty_year(self):
        """Test that empty year raises ValueError."""
        with pytest.raises(ValueError, match="Invalid year"):
            validate_years("")


class TestSplitCleanedDataByPeriod:
    """Test period splitting function."""

    def test_split_data_by_period(self, tmp_path):
        """Test splitting DataFrame by Năm and Tháng columns."""
        df = pd.DataFrame(
            {
                "Năm": [2024, 2024, 2025, 2025],
                "Tháng": [3, 4, 1, 2],
                "Mã hàng": ["A001", "B002", "C003", "D004"],
            }
        )

        period_dfs = split_cleaned_data_by_period(df)

        assert len(period_dfs) == 4
        assert "2024_03" in period_dfs
        assert "2024_04" in period_dfs
        assert "2025_01" in period_dfs
        assert "2025_02" in period_dfs
        assert len(period_dfs["2024_03"]) == 1
        assert len(period_dfs["2025_01"]) == 1

    def test_split_with_duplicate_periods(self, tmp_path):
        """Test handling of multiple rows with same period."""
        df = pd.DataFrame(
            {
                "Năm": [2024, 2024, 2024],
                "Tháng": [3, 3, 3],
                "Mã hàng": ["A001", "B002", "C003"],
            }
        )

        period_dfs = split_cleaned_data_by_period(df)

        assert len(period_dfs) == 1
        assert len(period_dfs["2024_03"]) == 3

    def test_split_missing_period_columns(self, tmp_path):
        """Test handling of DataFrames without Năm or Tháng columns."""
        df = pd.DataFrame({"Mã hàng": ["A001", "B002"]})

        period_dfs = split_cleaned_data_by_period(df)

        assert period_dfs == {}

    def test_split_empty_dataframe(self, tmp_path):
        """Test handling of empty DataFrame."""
        df = pd.DataFrame({"Năm": [], "Tháng": []})

        period_dfs = split_cleaned_data_by_period(df)

        assert period_dfs == {}


class TestPrepareDfForUpload:
    """Test DataFrame preparation for upload."""

    def test_move_ngay_to_first_position(self):
        df = pd.DataFrame(
            {
                "Mã hàng": ["A001", "B002"],
                "Số lượng": [10, 20],
                "Ngày": ["2025-01-01", "2025-01-02"],
                "Năm": [2025, 2025],
                "Tháng": [1, 1],
            }
        )

        result = prepare_df_for_upload(df, "Chi tiết nhập")

        assert result.columns[0] == "Ngày"
        assert "Ngày" not in result.columns[1:]

    def test_drop_nam_thang_columns(self):
        df = pd.DataFrame(
            {
                "Mã hàng": ["A001", "B002"],
                "Ngày": ["2025-01-01", "2025-01-02"],
                "Năm": [2025, 2025],
                "Tháng": [1, 1],
            }
        )

        result = prepare_df_for_upload(df, "Chi tiết nhập")

        assert "Năm" not in result.columns
        assert "Tháng" not in result.columns
        assert "Mã hàng" in result.columns
        assert "Ngày" in result.columns

    def test_drop_financial_metrics_xuat_nhap_ton(self):
        df = pd.DataFrame(
            {
                "Mã hàng": ["A001", "B002"],
                "Ngày": ["2025-01-01", "2025-01-02"],
                "Năm": [2025, 2025],
                "Tháng": [1, 1],
                "Doanh thu cuối kỳ": [100000, 200000],
                "Lãi gộp cuối kỳ": [10000, 20000],
            }
        )

        result = prepare_df_for_upload(df, "xuat_nhap_ton")

        assert "Doanh thu cuối kỳ" not in result.columns
        assert "Lãi gộp cuối kỳ" not in result.columns
        assert "Mã hàng" in result.columns

    def test_no_financial_drop_for_other_types(self):
        df = pd.DataFrame(
            {
                "Mã hàng": ["A001", "B002"],
                "Ngày": ["2025-01-01", "2025-01-02"],
                "Năm": [2025, 2025],
                "Tháng": [1, 1],
                "Doanh thu cuối kỳ": [100000, 200000],
            }
        )

        result = prepare_df_for_upload(df, "Chi tiết nhập")

        assert "Doanh thu cuối kỳ" in result.columns

    def test_handle_missing_ngay_column(self):
        df = pd.DataFrame(
            {
                "Năm": [2025, 2025],
                "Tháng": [1, 1],
                "Tên chi phí": ["Chi phí A", "Chi phí B"],
                "Thành tiền": [100000, 200000],
            }
        )

        result = prepare_df_for_upload(df, "bao_cao_tai_chinh")

        assert "Ngày" not in result.columns
        assert "Năm" not in result.columns
        assert "Tháng" not in result.columns
        assert "Tên chi phí" in result.columns

    def test_df_copy_not_modified(self):
        df = pd.DataFrame(
            {
                "Mã hàng": ["A001", "B002"],
                "Ngày": ["2025-01-01", "2025-01-02"],
                "Năm": [2025, 2025],
                "Tháng": [1, 1],
            }
        )

        original_cols = df.columns.tolist()
        prepare_df_for_upload(df, "Chi tiết nhập")

        assert df.columns.tolist() == original_cols


class TestYearFilteringIntegration:
    """Integration tests using real staging data."""

    def test_filter_staging_sale_csv(self):
        """Test filtering of real Chi tiết xuất CSV by year."""
        staging_dir = Path.cwd() / "data" / "01-staging" / "import_export"
        sale_file = staging_dir / "Chi tiết xuất 2020-04_2025-12.csv"

        if not sale_file.exists():
            pytest.skip(f"Test file not found: {sale_file}")

        df = pd.read_csv(sale_file)

        assert "Năm" in df.columns
        assert "Tháng" in df.columns

        period_dfs = split_cleaned_data_by_period(df)

        period_dfs_filtered = {
            period: df_period
            for period, df_period in period_dfs.items()
            if period.startswith("2025_")
        }

        assert len(period_dfs_filtered) < len(period_dfs)
        assert all(period.startswith("2025_") for period in period_dfs_filtered.keys())

    def test_filter_staging_purchase_csv(self):
        """Test filtering of real Chi tiết nhập CSV by multiple years."""
        staging_dir = Path.cwd() / "data" / "01-staging" / "import_export"
        purchase_file = staging_dir / "Chi tiết nhập 2020-04_2025-12.csv"

        if not purchase_file.exists():
            pytest.skip(f"Test file not found: {purchase_file}")

        df = pd.read_csv(purchase_file)

        assert "Năm" in df.columns
        assert "Tháng" in df.columns

        period_dfs = split_cleaned_data_by_period(df)

        years_filter = ["2024", "2025"]
        period_dfs_filtered = {
            period: df_period
            for period, df_period in period_dfs.items()
            if any(period.startswith(f"{year}_") for year in years_filter)
        }

        assert len(period_dfs_filtered) < len(period_dfs)
        assert all(
            any(period.startswith(f"{year}_") for year in years_filter)
            for period in period_dfs_filtered.keys()
        )

    def test_filter_nonexistent_year(self):
        """Test filtering with year that doesn't exist in data."""
        staging_dir = Path.cwd() / "data" / "01-staging" / "import_export"
        sale_file = staging_dir / "Chi tiết xuất 2020-04_2025-12.csv"

        if not sale_file.exists():
            pytest.skip(f"Test file not found: {sale_file}")

        df = pd.read_csv(sale_file)
        period_dfs = split_cleaned_data_by_period(df)

        period_dfs_filtered = {
            period: df_period
            for period, df_period in period_dfs.items()
            if period.startswith("2030_")
        }

        assert len(period_dfs_filtered) == 0
