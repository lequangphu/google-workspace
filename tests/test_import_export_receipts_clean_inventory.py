# -*- coding: utf-8 -*-
"""Tests for clean_inventory module."""

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest

from src.modules.import_export_receipts.clean_inventory import (
    combine_headers,
    extract_date_from_filename,
    process,
)

logger = logging.getLogger(__name__)


class TestCombineHeaders:
    """Test header combination logic."""

    def test_combine_headers_with_parent_child(self):
        """Test combining parent-child headers."""
        h1 = ["TỒN_ĐẦU_KỲ", "TỒN_ĐẦU_KỲ", "NHẬP_TRONG_KỲ"]
        h2 = ["S_LƯỢNG", "Đ_GIÁ", "S_LƯỢNG"]
        result = combine_headers(h1, h2)

        assert "TỒN_ĐẦU_KỲ_S_LƯỢNG" in result
        assert "TỒN_ĐẦU_KỲ_Đ_GIÁ" in result
        assert "NHẬP_TRONG_KỲ_S_LƯỢNG" in result

    def test_combine_headers_with_empty_cells(self):
        """Test handling of empty cells in headers."""
        h1 = ["TỒN_ĐẦU_KỲ", "", "NHẬP_TRONG_KỲ"]
        h2 = ["S_LƯỢNG", "Đ_GIÁ", ""]
        result = combine_headers(h1, h2)

        assert len(result) == 3
        assert "TỒN_ĐẦU_KỲ_S_LƯỢNG" in result

    def test_combine_headers_duplicate_handling(self):
        """Test handling of duplicate column names."""
        h1 = ["Mã_SP", "Mã_SP"]
        h2 = ["", ""]
        result = combine_headers(h1, h2)

        assert len(result) == 2
        # Duplicates should be made unique
        assert result[0] != result[1]


class TestExtractDateFromFilename:
    """Test date extraction from filenames."""

    def test_extract_date_valid_format(self):
        """Test extraction with valid filename format."""
        year, month, date_str = extract_date_from_filename("2024_01_XNT.csv")

        assert year == "2024"
        assert month == "01"
        assert date_str == "01-01-2024"

    def test_extract_date_single_digit_month(self):
        """Test extraction with single-digit month."""
        year, month, date_str = extract_date_from_filename("2024_1_XNT.csv")

        assert year == "2024"
        assert month == "01"
        assert date_str == "01-01-2024"

    def test_extract_date_invalid_format(self):
        """Test extraction with invalid filename format."""
        result = extract_date_from_filename("invalid_filename.csv")

        assert result is None


class TestInventoryProcessing:
    """Test inventory processing with real data."""

    def test_process_with_real_data(self):
        """Test processing with actual XNT CSV files from data/00-raw/import_export."""
        raw_dir = Path.cwd() / "data" / "00-raw" / "import_export"

        # Only run if raw data exists and has XNT files
        if not raw_dir.exists():
            pytest.skip(f"Raw data directory not found: {raw_dir}")

        xnt_files = list(raw_dir.glob("*XNT.csv"))
        if not xnt_files:
            pytest.skip("No XNT CSV files found in raw data")

        with TemporaryDirectory() as temp_dir:
            staging_dir = Path(temp_dir)
            output_path = process(raw_dir, staging_dir)

            # Verify output
            assert output_path is not None
            assert output_path.exists()

            # Load and validate output
            df = pd.read_csv(output_path)
            assert len(df) > 0
            assert "Mã hàng" in df.columns
            assert "Tên hàng" in df.columns
            assert "Ngày" in df.columns

            logger.info(f"Successfully processed {len(df)} rows to {output_path}")

    def test_process_empty_directory(self):
        """Test processing with empty input directory."""
        with TemporaryDirectory() as temp_input, TemporaryDirectory() as temp_staging:
            input_dir = Path(temp_input)
            staging_dir = Path(temp_staging)

            with pytest.raises(FileNotFoundError):
                process(input_dir, staging_dir)
