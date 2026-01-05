# -*- coding: utf-8 -*-
"""Tests for payable.generate_suppliers_xlsx module."""

import logging

import pandas as pd

from src.modules.payable import generate_suppliers_xlsx as gs

logger = logging.getLogger(__name__)


class TestCleanPhoneNumber:
    def test_clean_trailing_dot(self):
        result = gs.clean_phone_number("0912345678.")
        assert result == "0912345678"

    def test_clean_trailing_commas(self):
        result = gs.clean_phone_number("0912345678,;;")
        assert result == "0912345678"

    def test_clean_empty(self):
        result = gs.clean_phone_number("")
        assert result == ""

    def test_clean_none(self):
        result = gs.clean_phone_number(None)
        assert result == ""


class TestParseNumeric:
    def test_parse_simple_number(self):
        result = gs.parse_numeric("1500000")
        assert result == "1500000"

    def test_parse_with_dots(self):
        result = gs.parse_numeric("1.500.000")
        assert result == "1500000"

    def test_parse_negative(self):
        result = gs.parse_numeric("(30000)")
        assert result == "-30000"

    def test_parse_dash(self):
        result = gs.parse_numeric("-")
        assert result == "0"

    def test_parse_empty(self):
        result = gs.parse_numeric("")
        assert result == "0"

    def test_parse_none(self):
        result = gs.parse_numeric(None)
        assert result == "0"


class TestSplitPhoneNumbers:
    def test_split_single(self):
        result = gs.split_phone_numbers("0912345678")
        assert result == ["0912345678"]

    def test_split_with_slash(self):
        result = gs.split_phone_numbers("0912345678/0987654321")
        assert result == ["0912345678", "0987654321"]

    def test_split_with_dash(self):
        result = gs.split_phone_numbers("0912345678 - 0987654321")
        assert result == ["0912345678", "0987654321"]

    def test_split_empty(self):
        result = gs.split_phone_numbers("")
        assert result == []

    def test_split_none(self):
        result = gs.split_phone_numbers(None)
        assert result == []


class TestGenerateSupplierCodes:
    def test_generate_codes_empty(self):
        df = pd.DataFrame()
        result = gs.generate_supplier_codes(df)
        assert result.empty

    def test_generate_codes_single(self):
        df = pd.DataFrame({"Tên nhà cung cấp": ["Supplier A"]})
        result = gs.generate_supplier_codes(df)
        assert result["Mã nhà cung cấp"].iloc[0] == "NCC000001"

    def test_generate_codes_multiple(self):
        df = pd.DataFrame(
            {"Tên nhà cung cấp": ["Supplier A", "Supplier B", "Supplier C"]}
        )
        result = gs.generate_supplier_codes(df)
        codes = result["Mã nhà cung cấp"].tolist()
        assert codes == ["NCC000001", "NCC000002", "NCC000003"]

    def test_generate_codes_with_dates(self):
        df = pd.DataFrame(
            {
                "Tên nhà cung cấp": ["Supplier A", "Supplier B"],
                "first_date": pd.to_datetime(["2024-01-01", "2023-01-01"]),
            }
        )
        result = gs.generate_supplier_codes(df)
        assert result["Mã nhà cung cấp"].iloc[0] == "NCC000001"


class TestMapToKiotVietTemplate:
    def test_map_empty(self):
        df = pd.DataFrame()
        result = gs.map_to_kiotviet_template(df)
        assert result.empty

    def test_map_basic_columns(self):
        df = pd.DataFrame(
            {
                "Mã nhà cung cấp": ["NCC000001"],
                "Tên nhà cung cấp": ["Supplier A"],
            }
        )
        result = gs.map_to_kiotviet_template(df)
        assert result["Mã nhà cung cấp"].iloc[0] == "NCC000001"
        assert result["Tên nhà cung cấp"].iloc[0] == "Supplier A"

    def test_map_with_phone(self):
        df = pd.DataFrame(
            {
                "Mã nhà cung cấp": ["NCC000001"],
                "Tên nhà cung cấp": ["Supplier A"],
                "Điện thoại": ["0912345678/0987654321"],
            }
        )
        result = gs.map_to_kiotviet_template(df)
        assert result["Điện thoại"].iloc[0] == "0912345678"

    def test_map_with_debt(self):
        df = pd.DataFrame(
            {
                "Mã nhà cung cấp": ["NCC000001"],
                "Tên nhà cung cấp": ["Supplier A"],
                "Nợ cần trả hiện tại": [1500000],
            }
        )
        result = gs.map_to_kiotviet_template(df)
        assert result["Nợ cần trả hiện tại"].iloc[0] == 1500000


class TestValidateData:
    def test_validate_valid_data(self):
        df = pd.DataFrame(
            {
                "Mã nhà cung cấp": ["NCC000001"],
                "Tên nhà cung cấp": ["Supplier A"],
                "Email": ["test@example.com"],
                "Điện thoại": ["0912345678"],
                "Địa chỉ": ["123 Test St"],
                "Mã số thuế": ["1234567890"],
                "Ghi chú": [""],
                "Nhóm nhà cung cấp": [""],
                "Trạng thái": [1],
                "Tổng mua (Không Import)": [0],
                "Nợ cần trả hiện tại": [0],
                "Tổng mua trừ trả hàng": [0],
                "Công ty": [""],
                "Khu vực": [""],
                "Phường/Xã": [""],
            }
        )
        is_valid, errors = gs.validate_data(df)
        assert is_valid is True
        assert len(errors) == 0

    def test_validate_missing_required(self):
        df = pd.DataFrame(
            {
                "Tên nhà cung cấp": ["Supplier A"],
            }
        )
        is_valid, errors = gs.validate_data(df)
        assert is_valid is False
        assert len(errors) > 0


class TestMergeAllData:
    def test_merge_empty(self):
        result = gs.merge_all_data(pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
        assert result.empty

    def test_merge_single_source(self):
        suppliers_df = pd.DataFrame(
            {
                "Tên nhà cung cấp": ["Supplier A", "Supplier B"],
                "Điện thoại": ["0912345678", "0987654321"],
            }
        )
        result = gs.merge_all_data(suppliers_df, pd.DataFrame(), pd.DataFrame())
        assert len(result) == 2

    def test_merge_multiple_sources(self):
        suppliers_df = pd.DataFrame(
            {
                "Tên nhà cung cấp": ["Supplier A"],
                "Điện thoại": ["0912345678"],
            }
        )
        debts_df = pd.DataFrame(
            {
                "Tên nhà cung cấp": ["Supplier A", "Supplier B"],
                "Nợ cần trả hiện tại": [1000000, 2000000],
            }
        )
        result = gs.merge_all_data(suppliers_df, debts_df, pd.DataFrame())
        assert len(result) == 2


class TestFormatValue:
    def test_format_text(self):
        result = gs.format_value("test", "text")
        assert result == "test"

    def test_format_number_int(self):
        result = gs.format_value(1500000, "number")
        assert result == 1500000

    def test_format_number_float(self):
        result = gs.format_value(1500000.5, "number")
        assert result == 1500000.5

    def test_format_number_string(self):
        result = gs.format_value("1500000", "number")
        assert result == 1500000.0

    def test_format_number_with_commas(self):
        result = gs.format_value("1,500,000", "number")
        assert result == 1500000.0

    def test_format_number_negative_parens(self):
        result = gs.format_value("(30000)", "number")
        assert result == -30000.0

    def test_format_date_string(self):
        result = gs.format_value("01/01/2024", "date")
        assert result == "2024-01-01"

    def test_format_date_empty(self):
        result = gs.format_value("", "date")
        assert result == ""

    def test_format_none(self):
        result = gs.format_value(None, "text")
        assert result == ""


class TestUploadToGoogleSheet:
    def test_upload_to_google_sheet_constants(self):
        assert (
            gs.EXPORT_SPREADSHEET_ID == "11vk-p0iL9JcNH180n4uV5VTuPnhJ97lBgsLEfCnWx_k"
        )
        assert gs.EXPORT_SHEET_NAME == "suppliers_to_import"
