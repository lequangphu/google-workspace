# -*- coding: utf-8 -*-
"""Tests for src/pipeline/validation.py."""

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest

from src.pipeline.validation import (
    EXPECTED_SCHEMAS,
    _check_dataframe_not_empty,
    _check_forbidden_values,
    _check_numeric_columns,
    _check_required_columns,
    move_to_quarantine,
    validate_schema,
)

logger = logging.getLogger(__name__)


class TestExpectedSchemas:
    """Test expected schema definitions."""

    def test_all_4_tab_types_defined(self):
        """All 4 tab types have schema definitions."""
        assert "Chi tiết nhập" in EXPECTED_SCHEMAS
        assert "Chi tiết xuất" in EXPECTED_SCHEMAS
        assert "Xuất nhập tồn" in EXPECTED_SCHEMAS
        assert "Chi tiết chi phí" in EXPECTED_SCHEMAS

    def test_chitietnhap_schema(self):
        """Chi tiết nhập has required columns."""
        schema = EXPECTED_SCHEMAS["Chi tiết nhập"]
        required = schema["required_columns"]
        assert "Ngày" in required
        assert "Mã hàng" in required
        assert "Tên hàng" in required
        assert "Số lượng" in required
        assert "Đơn giá" in required
        assert "Thành tiền" in required

    def test_chitietxuat_schema(self):
        """Chi tiết xuất has required columns."""
        schema = EXPECTED_SCHEMAS["Chi tiết xuất"]
        required = schema["required_columns"]
        assert "Ngày" in required
        assert "Mã hàng" in required
        assert "Tên hàng" in required
        assert "Số lượng" in required
        assert "Đơn giá" in required
        assert "Thành tiền" in required

    def test_xuatnhapton_schema(self):
        """Xuất nhập tồn has required columns."""
        schema = EXPECTED_SCHEMAS["Xuất nhập tồn"]
        required = schema["required_columns"]
        assert "Mã hàng" in required
        assert "Tên hàng" in required
        assert "Tồn cuối kỳ" in required
        assert "Giá trị cuối kỳ" in required

    def test_chitietchiph_schema(self):
        """Chi tiết chi phí has required columns."""
        schema = EXPECTED_SCHEMAS["Chi tiết chi phí"]
        required = schema["required_columns"]
        assert "Mã hàng" in required
        assert "Tên hàng" in required
        assert "Số tiền" in required
        assert "Diễn giải" in required


class TestCheckRequiredColumns:
    """Test _check_required_columns function."""

    def test_all_columns_present(self, tmp_path):
        """All required columns present returns True."""
        df = pd.DataFrame(
            {
                "Ngày": ["2025-01-01"],
                "Mã hàng": ["A"],
                "Tên hàng": ["Product A"],
                "Số lượng": [10],
                "Đơn giá": [100.0],
                "Thành tiền": [1000.0],
            }
        )
        schema = {"required_columns": ["Ngày", "Mã hàng", "Tên hàng", "Số lượng"]}
        result = _check_required_columns(df, tmp_path / "test.csv", schema)
        assert result is True

    def test_missing_required_column(self, tmp_path):
        """Missing required column returns False."""
        df = pd.DataFrame({"Mã hàng": ["A"], "Tên hàng": ["Product A"]})
        schema = {"required_columns": ["Mã hàng", "Tên hàng", "Ngày"]}
        result = _check_required_columns(df, tmp_path / "test.csv", schema)
        assert result is False

    def test_missing_multiple_columns(self, tmp_path):
        """Multiple missing columns returns False."""
        df = pd.DataFrame({"Mã hàng": ["A"]})
        schema = {"required_columns": ["Mã hàng", "Tên hàng", "Ngày"]}
        result = _check_required_columns(df, tmp_path / "test.csv", schema)
        assert result is False


class TestCheckNumericColumns:
    """Test _check_numeric_columns function."""

    def test_valid_numeric_columns(self, tmp_path):
        """Valid numeric columns return True."""
        df = pd.DataFrame({"Số lượng": [10, 20], "Đơn giá": [100.0, 200.0]})
        schema = {"numeric_columns": ["Số lượng", "Đơn giá"]}
        result = _check_numeric_columns(df, tmp_path / "test.csv", schema)
        assert result is True

    def test_column_not_numeric(self, tmp_path):
        """Non-numeric column returns False."""
        df = pd.DataFrame({"Số lượng": ["10", "20"]})
        schema = {"numeric_columns": ["Số lượng"]}
        result = _check_numeric_columns(df, tmp_path / "test.csv", schema)
        assert result is False

    def test_all_nan_in_numeric_column(self, tmp_path):
        """All NaN in numeric column returns False."""
        df = pd.DataFrame({"Số lượng": [None, None]})
        schema = {"numeric_columns": ["Số lượng"]}
        result = _check_numeric_columns(df, tmp_path / "test.csv", schema)
        assert result is False

    def test_partial_nan_in_numeric_column(self, tmp_path):
        """Partial NaN in numeric column returns True (not all NaN)."""
        df = pd.DataFrame({"Số lượng": [10, None, 20]})
        schema = {"numeric_columns": ["Số lượng"]}
        result = _check_numeric_columns(df, tmp_path / "test.csv", schema)
        assert result is True


class TestCheckForbiddenValues:
    """Test _check_forbidden_values function."""

    def test_no_forbidden_values(self, tmp_path):
        """No forbidden values returns True."""
        df = pd.DataFrame({"Số lượng": [10, 20], "Đơn giá": [100.0, 200.0]})
        schema = {"forbidden_values": {"Số lượng": [0, None], "Đơn giá": [0]}}
        result = _check_forbidden_values(df, tmp_path / "test.csv", schema)
        assert result is True

    def test_forbidden_value_present(self, tmp_path):
        """Forbidden value present returns False."""
        df = pd.DataFrame({"Số lượng": [0, 20]})
        schema = {"forbidden_values": {"Số lượng": [0, None]}}
        result = _check_forbidden_values(df, tmp_path / "test.csv", schema)
        assert result is False

    def test_nan_in_forbidden_list(self, tmp_path):
        """NaN in forbidden values column returns False."""
        df = pd.DataFrame({"Số lượng": [10, None]})
        schema = {"forbidden_values": {"Số lượng": [None]}}
        result = _check_forbidden_values(df, tmp_path / "test.csv", schema)
        assert result is False

    def test_none_forbidden_map(self, tmp_path):
        """None forbidden_map returns True."""
        df = pd.DataFrame({"Số lượng": [10, 20]})
        schema = {}
        result = _check_forbidden_values(df, tmp_path / "test.csv", schema)
        assert result is True


class TestCheckDataframeNotEmpty:
    """Test _check_dataframe_not_empty function."""

    def test_non_empty_dataframe(self, tmp_path):
        """Non-empty DataFrame returns True."""
        df = pd.DataFrame({"col": [1, 2, 3]})
        result = _check_dataframe_not_empty(df, tmp_path / "test.csv")
        assert result is True

    def test_empty_dataframe(self, tmp_path):
        """Empty DataFrame returns False."""
        df = pd.DataFrame(columns=["col1", "col2"])
        result = _check_dataframe_not_empty(df, tmp_path / "test.csv")
        assert result is False


class TestValidateSchema:
    """Test validate_schema function."""

    def test_valid_chitietnhap_schema(self, tmp_path):
        """Valid Chi tiết nhập schema passes validation."""
        csv_path = tmp_path / "test.csv"
        df = pd.DataFrame(
            {
                "Ngày": ["2025-01-01"],
                "Mã hàng": ["A"],
                "Tên hàng": ["Product A"],
                "Số lượng": [10],
                "Đơn giá": [100.0],
                "Thành tiền": [1000.0],
            }
        )
        df.to_csv(csv_path, index=False, encoding="utf-8")

        result = validate_schema(csv_path, "Chi tiết nhập")
        assert result is True

    def test_valid_chitietxuat_schema(self, tmp_path):
        """Valid Chi tiết xuất schema passes validation."""
        csv_path = tmp_path / "test.csv"
        df = pd.DataFrame(
            {
                "Ngày": ["2025-01-01"],
                "Mã hàng": ["A"],
                "Tên hàng": ["Product A"],
                "Số lượng": [10],
                "Đơn giá": [100.0],
                "Thành tiền": [1000.0],
            }
        )
        df.to_csv(csv_path, index=False, encoding="utf-8")

        result = validate_schema(csv_path, "Chi tiết xuất")
        assert result is True

    def test_valid_xuatnhapton_schema(self, tmp_path):
        """Valid Xuất nhập tồn schema passes validation."""
        csv_path = tmp_path / "test.csv"
        df = pd.DataFrame(
            {
                "Mã hàng": ["A", "B"],
                "Tên hàng": ["Product A", "Product B"],
                "Tồn cuối kỳ": [100, 50],
                "Giá trị cuối kỳ": [1000.0, 500.0],
            }
        )
        df.to_csv(csv_path, index=False, encoding="utf-8")

        result = validate_schema(csv_path, "Xuất nhập tồn")
        assert result is True

    def test_valid_chitietchiph_schema(self, tmp_path):
        """Valid Chi tiết chi phí schema passes validation."""
        csv_path = tmp_path / "test.csv"
        df = pd.DataFrame(
            {
                "Mã hàng": ["A"],
                "Tên hàng": ["Product A"],
                "Số tiền": [1000.0],
                "Diễn giải": ["Test expense"],
            }
        )
        df.to_csv(csv_path, index=False, encoding="utf-8")

        result = validate_schema(csv_path, "Chi tiết chi phí")
        assert result is True

    def test_missing_required_column_fails(self, tmp_path):
        """Missing required column fails validation."""
        csv_path = tmp_path / "test.csv"
        df = pd.DataFrame(
            {
                "Mã hàng": ["A"],
                "Tên hàng": ["Product A"],
                "Số lượng": [10],
                "Đơn giá": [100.0],
            }
        )
        df.to_csv(csv_path, index=False, encoding="utf-8")

        result = validate_schema(csv_path, "Chi tiết nhập")
        assert result is False

    def test_forbidden_value_fails(self, tmp_path):
        """Forbidden value fails validation."""
        csv_path = tmp_path / "test.csv"
        df = pd.DataFrame(
            {
                "Ngày": ["2025-01-01"],
                "Mã hàng": ["A"],
                "Tên hàng": ["Product A"],
                "Số lượng": [0],
                "Đơn giá": [100.0],
                "Thành tiền": [1000.0],
            }
        )
        df.to_csv(csv_path, index=False, encoding="utf-8")

        result = validate_schema(csv_path, "Chi tiết nhập")
        assert result is False

    def test_empty_file_fails(self, tmp_path):
        """Empty CSV file fails validation."""
        csv_path = tmp_path / "test.csv"
        df = pd.DataFrame(columns=["Ngày", "Mã hàng", "Tên hàng"])
        df.to_csv(csv_path, index=False, encoding="utf-8")

        result = validate_schema(csv_path, "Chi tiết nhập")
        assert result is False

    def test_invalid_file_fails(self, tmp_path):
        """Invalid tab type raises ValueError."""
        csv_path = tmp_path / "test.csv"
        df = pd.DataFrame({"col1": [1]})
        df.to_csv(csv_path, index=False)

        with pytest.raises(ValueError) as exc_info:
            validate_schema(csv_path, "Invalid Tab")
        assert "Unknown tab type" in str(exc_info.value)

    def test_nan_in_numeric_column_fails(self, tmp_path):
        """All NaN in numeric column fails validation."""
        csv_path = tmp_path / "test.csv"
        df = pd.DataFrame(
            {
                "Ngày": ["2025-01-01"],
                "Mã hàng": ["A"],
                "Tên hàng": ["Product A"],
                "Số lượng": [None],
                "Đơn giá": [100.0],
                "Thành tiền": [1000.0],
            }
        )
        df.to_csv(csv_path, index=False, encoding="utf-8")

        result = validate_schema(csv_path, "Chi tiết nhập")
        assert result is False


class TestMoveToQuarantine:
    """Test move_to_quarantine function."""

    def test_move_to_default_quarantine(self, tmp_path):
        """Move file to default quarantine directory."""
        source = tmp_path / "test.csv"
        source.write_text("data")

        dest_path = move_to_quarantine(source)

        assert not source.exists()
        assert dest_path.name == "test.csv"
        assert "00-rejected" in str(dest_path)

    def test_move_to_custom_quarantine(self, tmp_path):
        """Move file to custom quarantine directory."""
        source = tmp_path / "test.csv"
        source.write_text("data")
        custom_dir = tmp_path / "custom_rejected"

        dest_path = move_to_quarantine(source, quarantine_dir=custom_dir)

        assert not source.exists()
        assert dest_path == custom_dir / "test.csv"
        assert dest_path.exists()
        assert dest_path.read_text() == "data"

    def test_quarantine_dir_created(self, tmp_path):
        """Quarantine directory created if not exists."""
        source = tmp_path / "test.csv"
        source.write_text("data")
        quarantine_dir = tmp_path / "rejected"

        assert not quarantine_dir.exists()

        dest_path = move_to_quarantine(source, quarantine_dir=quarantine_dir)

        assert quarantine_dir.exists()
        assert dest_path.exists()

    def test_source_not_found(self, tmp_path):
        """FileNotFoundError if source file does not exist."""
        source = tmp_path / "nonexistent.csv"

        with pytest.raises(FileNotFoundError):
            move_to_quarantine(source)
