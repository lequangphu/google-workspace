# -*- coding: utf-8 -*-
"""Tests for receivable.generate_customers_xlsx module."""

import logging
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src.modules.receivable import generate_customers_xlsx as gc

logger = logging.getLogger(__name__)


class TestCleanPhoneNumber:
    """Test individual phone number cleaning."""

    def test_clean_trailing_dot(self):
        result = gc.clean_phone_number("0912345678.")
        assert result == "0912345678"

    def test_clean_trailing_commas(self):
        result = gc.clean_phone_number("0912345678,;;")
        assert result == "0912345678"

    def test_clean_empty(self):
        result = gc.clean_phone_number("")
        assert result == ""

    def test_clean_none(self):
        result = gc.clean_phone_number(None)
        assert result == ""


class TestParseNumeric:
    """Test numeric value parsing."""

    def test_parse_simple_number(self):
        result = gc.parse_numeric("1500000")
        assert result == "1500000"

    def test_parse_with_dots(self):
        result = gc.parse_numeric("1.500.000")
        assert result == "1500000"

    def test_parse_negative(self):
        result = gc.parse_numeric("(30000)")
        assert result == "-30000"

    def test_parse_dash(self):
        result = gc.parse_numeric("-")
        assert result == "0"

    def test_parse_empty(self):
        result = gc.parse_numeric("")
        assert result == "0"


class TestAggregateTransactions:
    """Test transaction aggregation."""

    def test_aggregate_single_customer(self):
        df = pd.DataFrame(
            {
                "Tên khách hàng": ["Customer A", "Customer A", "Customer A"],
                "Ngày": ["2024-01-15", "2024-02-20", "2024-03-10"],
                "Thành tiền": [1000000, 2000000, 3000000],
            }
        )

        result = gc.aggregate_transactions(df)

        assert len(result) == 1
        assert result.iloc[0]["Tên khách hàng"] == "Customer A"
        assert result.iloc[0]["transaction_count"] == 3
        assert result.iloc[0]["total_amount"] == 6000000

    def test_aggregate_multiple_customers(self):
        df = pd.DataFrame(
            {
                "Tên khách hàng": ["Customer A", "Customer A", "Customer B"],
                "Ngày": ["2024-01-15", "2024-02-20", "2024-03-10"],
                "Thành tiền": [1000000, 2000000, 500000],
            }
        )

        result = gc.aggregate_transactions(df)

        assert len(result) == 2

    def test_aggregate_empty(self):
        result = gc.aggregate_transactions(pd.DataFrame())
        assert result.empty

    def test_aggregate_missing_columns(self):
        result = gc.aggregate_transactions(pd.DataFrame({"wrong": [1, 2, 3]}))
        assert result.empty


class TestMergeAllData:
    """Test merging data from multiple sources."""

    def test_merge_all_sources(self):
        customers = pd.DataFrame(
            {
                "Tên khách hàng": ["Customer A", "Customer B"],
                "Điện thoại": ["0912345678", "0987654321"],
                "Địa chỉ": ["Address A", "Address B"],
            }
        )

        debts = pd.DataFrame(
            {
                "Tên khách hàng": ["Customer A", "Customer C"],
                "Nợ cần thu hiện tại": [1000000, 500000],
            }
        )

        transactions = pd.DataFrame(
            {
                "Tên khách hàng": ["Customer A", "Customer B", "Customer D"],
                "total_amount": [5000000, 3000000, 1000000],
            }
        )

        result = gc.merge_all_data(customers, debts, transactions)

        assert len(result) == 4
        assert set(result["Tên khách hàng"]) == {
            "Customer A",
            "Customer B",
            "Customer C",
            "Customer D",
        }

    def test_merge_empty_transactions(self):
        customers = pd.DataFrame(
            {
                "Tên khách hàng": ["Customer A", "Customer B"],
                "Điện thoại": ["0912345678", "0987654321"],
            }
        )

        debts = pd.DataFrame(
            {
                "Tên khách hàng": ["Customer A"],
                "Nợ cần thu hiện tại": [1000000],
            }
        )

        result = gc.merge_all_data(customers, debts, pd.DataFrame())

        assert len(result) == 2
        assert "Customer A" in result["Tên khách hàng"].values
        assert "Customer B" in result["Tên khách hàng"].values

    def test_merge_all_empty(self):
        result = gc.merge_all_data(pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
        assert result.empty


class TestGenerateCustomerCodes:
    """Test customer code generation."""

    def test_generate_codes_sorted(self):
        df = pd.DataFrame(
            {
                "Tên khách hàng": ["Customer A", "Customer B", "Customer C"],
                "first_date": pd.to_datetime(
                    ["2024-02-01", "2024-01-15", "2024-01-15"]
                ),
                "total_amount": [1000, 3000, 5000],
            }
        )

        result = gc.generate_customer_codes(df)

        assert result.iloc[0]["Mã khách hàng"] == "KH000001"
        assert result.iloc[0]["Tên khách hàng"] == "Customer C"
        assert result.iloc[1]["Mã khách hàng"] == "KH000002"
        assert result.iloc[1]["Tên khách hàng"] == "Customer B"
        assert result.iloc[2]["Mã khách hàng"] == "KH000003"
        assert result.iloc[2]["Tên khách hàng"] == "Customer A"

    def test_generate_codes_format(self):
        df = pd.DataFrame(
            {
                "Tên khách hàng": [f"Customer {i}" for i in range(100)],
                "first_date": pd.date_range("2024-01-01", periods=100),
                "total_amount": range(100),
            }
        )

        result = gc.generate_customer_codes(df)

        assert result.iloc[0]["Mã khách hàng"] == "KH000001"
        assert result.iloc[99]["Mã khách hàng"] == "KH000100"

    def test_generate_codes_empty(self):
        result = gc.generate_customer_codes(pd.DataFrame())
        assert result.empty


class TestMapToKiotvietTemplate:
    """Test mapping to KiotViet template."""

    def test_map_success(self):
        df = pd.DataFrame(
            {
                "Mã khách hàng": ["KH000001", "KH000002"],
                "Tên khách hàng": ["Customer A", "Customer B"],
                "Điện thoại": ["0912345678", "0987654321"],
                "Địa chỉ": ["123 Đường A", "456 Đường B"],
                "last_date": pd.to_datetime(["2024-06-20", "2024-05-15"]),
                "total_amount": [5000000, 3000000],
                "Nợ cần thu hiện tại": [1000000, 0],
                "Ghi chú": ["Note A", "Note B"],
                "Mã KH cũ": ["OLD001", "OLD002"],
            }
        )

        result = gc.map_to_kiotviet_template(df)

        template = gc.CustomerTemplate()
        assert list(result.columns) == template.get_column_names()
        assert result.iloc[0]["Loại khách"] == "Cá nhân"
        assert result.iloc[0]["Mã khách hàng"] == "KH000001"
        assert result.iloc[0]["Điện thoại"] == "0912345678"
        assert result.iloc[0]["Trạng thái"] == 1

    def test_map_inactive_customer(self):
        from datetime import datetime, timedelta

        old_date = datetime.now() - timedelta(days=180)
        df = pd.DataFrame(
            {
                "Mã khách hàng": ["KH000001"],
                "Tên khách hàng": ["Customer A"],
                "last_date": [old_date],
                "Nợ cần thu hiện tại": [0],
            }
        )

        result = gc.map_to_kiotviet_template(df)

        assert result.iloc[0]["Trạng thái"] == 0

    def test_map_active_customer_with_debt(self):
        df = pd.DataFrame(
            {
                "Mã khách hàng": ["KH000001"],
                "Tên khách hàng": ["Customer A"],
                "last_date": [pd.NaT],
                "Nợ cần thu hiện tại": [1000000],
            }
        )

        result = gc.map_to_kiotviet_template(df)

        assert result.iloc[0]["Trạng thái"] == 1

    def test_map_active_customer_with_recent_transaction(self):
        from datetime import datetime, timedelta

        recent_date = datetime.now() - timedelta(days=30)
        df = pd.DataFrame(
            {
                "Mã khách hàng": ["KH000001"],
                "Tên khách hàng": ["Customer A"],
                "last_date": [recent_date],
                "Nợ cần thu hiện tại": [0],
            }
        )

        result = gc.map_to_kiotviet_template(df)

        assert result.iloc[0]["Trạng thái"] == 1

    def test_map_empty(self):
        result = gc.map_to_kiotviet_template(pd.DataFrame())
        assert result.empty

    def test_map_khu_vuc_giao_hang_empty(self):
        df = pd.DataFrame(
            {
                "Mã khách hàng": ["KH000001"],
                "Tên khách hàng": ["Customer A"],
                "Địa chỉ": ["123 Nguyễn Trãi, Phường 5, Quận 5, TP HCM"],
            }
        )

        result = gc.map_to_kiotviet_template(df)

        assert result.iloc[0]["Khu vực giao hàng"] == ""

    def test_map_ghi_chu_with_old_code(self):
        df = pd.DataFrame(
            {
                "Mã khách hàng": ["KH000001"],
                "Tên khách hàng": ["Customer A"],
                "Ghi chú": ["Original note"],
                "Mã KH cũ": ["OLD001"],
            }
        )

        result = gc.map_to_kiotviet_template(df)

        assert "Original note" in result.iloc[0]["Ghi chú"]
        assert "Mã cũ: OLD001" in result.iloc[0]["Ghi chú"]


class TestLoadThongTinKh:
    """Test loading Thong tin KH data."""

    def test_load_thong_tin_kh_structure(self, tmp_path):
        with patch.object(gc, "authenticate_google") as mock_auth:
            mock_service = MagicMock()
            mock_auth.return_value = mock_service

            mock_sheets = MagicMock()
            with patch("src.modules.google_api.build", return_value=mock_sheets):
                with patch.object(gc, "read_sheet_data") as mock_read:
                    mock_read.return_value = [
                        [
                            "STT",
                            "MÃ KH",
                            "TÊN KHÁCH HÀNG",
                            "Địa chỉ ",
                            "Tel",
                            "Ghi chú",
                        ],
                        [
                            "1",
                            "KH001",
                            "Customer A",
                            "123 Address A",
                            "0912345678",
                            "Note A",
                        ],
                        [
                            "2",
                            "KH002",
                            "Customer B",
                            "456 Address B",
                            "0987654321",
                            "Note B",
                        ],
                    ]

                    result = gc.load_thong_tin_kh(mock_sheets)

                    assert len(result) == 2
                    assert "Tên khách hàng" in result.columns
                    assert "Điện thoại" in result.columns
                    assert "Địa chỉ" in result.columns

    def test_load_thong_tin_kh_filters_system_rows(self):
        with patch.object(gc, "authenticate_google") as mock_auth:
            mock_service = MagicMock()
            mock_auth.return_value = mock_service

            mock_sheets = MagicMock()
            with patch("src.modules.google_api.build", return_value=mock_sheets):
                with patch.object(gc, "read_sheet_data") as mock_read:
                    mock_read.return_value = [
                        [
                            "STT",
                            "MÃ KH",
                            "TÊN KHÁCH HÀNG",
                            "Địa chỉ ",
                            "Tel",
                            "Ghi chú",
                        ],
                        [
                            "1",
                            "KH001",
                            "Customer A",
                            "123 Address A",
                            "0912345678",
                            "Note A",
                        ],
                        [
                            "2",
                            "",
                            "Customer B",
                            "456 Address B",
                            "0987654321",
                            "Note B",
                        ],
                        ["", "", "TỔNG CỘNG", "", "", ""],
                        ["", "", "NL", "", "", ""],
                        ["", "", "NGƯỜI LẬP", "", "", ""],
                    ]

                    result = gc.load_thong_tin_kh(mock_sheets)

                    assert len(result) == 1
                    assert result.iloc[0]["Tên khách hàng"] == "Customer A"

    def test_load_thong_tin_kh_requires_ma_kh(self):
        with patch.object(gc, "authenticate_google") as mock_auth:
            mock_service = MagicMock()
            mock_auth.return_value = mock_service

            mock_sheets = MagicMock()
            with patch("src.modules.google_api.build", return_value=mock_sheets):
                with patch.object(gc, "read_sheet_data") as mock_read:
                    mock_read.return_value = [
                        [
                            "STT",
                            "MÃ KH",
                            "TÊN KHÁCH HÀNG",
                            "Địa chỉ ",
                            "Tel",
                            "Ghi chú",
                        ],
                        [
                            "1",
                            "KH001",
                            "Customer A",
                            "123 Address A",
                            "0912345678",
                            "Note A",
                        ],
                        [
                            "2",
                            "",
                            "Customer B",
                            "456 Address B",
                            "0987654321",
                            "Note B",
                        ],
                        [
                            "3",
                            "KH003",
                            "Customer C",
                            "789 Address C",
                            "0922222222",
                            "Note C",
                        ],
                    ]

                    result = gc.load_thong_tin_kh(mock_sheets)

                    assert len(result) == 2
                    names = result["Tên khách hàng"].tolist()
                    assert "Customer A" in names
                    assert "Customer C" in names
                    assert "Customer B" not in names


class TestLoadTongCongNo:
    """Test loading TỔNG CÔNG NỢ data."""

    def test_load_tong_cong_no_structure(self):
        with patch.object(gc, "authenticate_google") as mock_auth:
            mock_service = MagicMock()
            mock_auth.return_value = mock_service

            mock_sheets = MagicMock()
            with patch("src.modules.google_api.build", return_value=mock_sheets):
                with patch.object(gc, "read_sheet_data") as mock_read:
                    mock_read.return_value = [
                        [
                            "STT",
                            "TÊN KHÁCH HÀNG",
                            " TỔNG NỢ ",
                            " ĐÃ THANH TOÁN ",
                            " NỢ CÒN LẠI  ",
                        ],
                        ["1", "Customer A", "10000000", "5000000", "5000000"],
                        ["2", "Customer B", "2000000", "2000000", "0"],
                    ]

                    result = gc.load_tong_cong_no(mock_sheets)

                    assert len(result) == 2
                    assert "Tên khách hàng" in result.columns
                    assert "Nợ cần thu hiện tại" in result.columns
                    assert result.iloc[0]["Nợ cần thu hiện tại"] == 5000000

    def test_load_tong_cong_no_filters_system_rows(self):
        with patch.object(gc, "authenticate_google") as mock_auth:
            mock_service = MagicMock()
            mock_auth.return_value = mock_service

            mock_sheets = MagicMock()
            with patch("src.modules.google_api.build", return_value=mock_sheets):
                with patch.object(gc, "read_sheet_data") as mock_read:
                    mock_read.return_value = [
                        [
                            "STT",
                            "TÊN KHÁCH HÀNG",
                            " TỔNG NỢ ",
                            " ĐÃ THANH TOÁN ",
                            " NỢ CÒN LẠI  ",
                        ],
                        ["1", "Customer A", "10000000", "5000000", "5000000"],
                        ["", "TỔNG CỘNG", "1000000", "500000", "500000"],
                        ["", "NL", "", "", ""],
                        ["", "NGƯỜI LẬP", "", "", ""],
                        ["", "TYPO", "", "", ""],
                        ["", "TRUNGKL", "", "", ""],
                    ]

                    result = gc.load_tong_cong_no(mock_sheets)

                    assert len(result) == 1
                    assert result.iloc[0]["Tên khách hàng"] == "Customer A"


class TestIntegration:
    """Integration tests."""

    def test_full_pipeline_structure(self, tmp_path):
        customers = pd.DataFrame(
            {
                "Mã KH cũ": ["OLD001"],
                "Tên khách hàng": ["Customer A"],
                "Điện thoại": ["0912345678"],
                "Địa chỉ": ["123 Nguyễn Trãi, Phường 5, Quận 5, TP HCM"],
                "Ghi chú": ["Note A"],
            }
        )

        debts = pd.DataFrame(
            {
                "Tên khách hàng": ["Customer A"],
                "Nợ cần thu hiện tại": [1000000],
            }
        )

        transactions = pd.DataFrame(
            {
                "Tên khách hàng": ["Customer A"],
                "first_date": pd.to_datetime(["2024-01-15"]),
                "last_date": pd.to_datetime(["2024-06-20"]),
                "total_amount": [5000000],
                "transaction_count": [5],
            }
        )

        merged = gc.merge_all_data(customers, debts, transactions)
        assert len(merged) == 1

        coded = gc.generate_customer_codes(merged)
        assert coded.iloc[0]["Mã khách hàng"] == "KH000001"

        template = gc.map_to_kiotviet_template(coded)
        assert len(template.columns) == 20
        assert template.iloc[0]["Loại khách"] == "Cá nhân"
        assert template.iloc[0]["Mã khách hàng"] == "KH000001"
        assert template.iloc[0]["Trạng thái"] == 1

    def test_inactive_customer_detection(self):
        from datetime import datetime, timedelta

        customers = pd.DataFrame(
            {
                "Tên khách hàng": ["Active Customer", "Inactive Customer"],
            }
        )

        debts = pd.DataFrame(
            {
                "Tên khách hàng": ["Active Customer"],
                "Nợ cần thu hiện tại": [1000000],
            }
        )

        recent_date = datetime.now() - timedelta(days=30)
        old_date = datetime.now() - timedelta(days=180)

        transactions = pd.DataFrame(
            {
                "Tên khách hàng": ["Active Customer", "Inactive Customer"],
                "total_amount": [5000000, 1000000],
                "first_date": pd.to_datetime(["2024-01-01", "2024-01-01"]),
                "last_date": pd.to_datetime([recent_date, old_date]),
            }
        )

        merged = gc.merge_all_data(customers, debts, transactions)
        coded = gc.generate_customer_codes(merged)
        template = gc.map_to_kiotviet_template(coded)

        active = template[template["Tên khách hàng"] == "Active Customer"]
        inactive = template[template["Tên khách hàng"] == "Inactive Customer"]

        assert active.iloc[0]["Trạng thái"] == 1
        assert inactive.iloc[0]["Trạng thái"] == 0
