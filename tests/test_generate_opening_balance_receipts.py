# -*- coding: utf-8 -*-
"""Tests for generate_opening_balance_receipts.py module."""

import json
from pathlib import Path

import pandas as pd
import pytest

from src.modules.import_export_receipts.generate_opening_balance_receipts import (
    append_to_purchases,
    create_reconciliation_checkpoint,
    detect_earliest_month,
    detect_existing_synthetic_records,
    extract_opening_balances,
    generate_synthetic_receipts,
    load_opening_balance_config,
    load_pipeline_config,
    validate_opening_balances,
)


@pytest.fixture
def sample_inventory_data():
    return pd.DataFrame(
        {
            "Mã hàng": ["SPC001", "SPC002", "SPC003"],
            "Tên hàng": ["Lốp 195/65R15", "Lốp 205/55R16", "Lốp 175/70R14"],
            "Số lượng đầu kỳ": [100, 50, 75],
            "Đơn giá đầu kỳ": [50000, 45000, 60000],
            "Thành tiền đầu kỳ": [5000000, 2250000, 4500000],
            "Số lượng nhập trong kỳ": [20, 10, 15],
            "Đơn giá nhập trong kỳ": [48000, 44000, 58000],
            "Thành tiền nhập trong kỳ": [960000, 440000, 870000],
            "Số lượng xuất trong kỳ": [50, 30, 40],
            "Đơn giá xuất trong kỳ": [52000, 48000, 62000],
            "Thành tiền xuất trong kỳ": [2600000, 1440000, 2480000],
            "Số lượng cuối kỳ": [70, 30, 50],
            "Đơn giá cuối kỳ": [51000, 46000, 59000],
            "Thành tiền cuối kỳ": [3570000, 1380000, 2950000],
            "Doanh thu cuối kỳ": [3000000, 1500000, 2500000],
            "Lãi gộp cuối kỳ": [1000000, 500000, 800000],
            "Năm": [2020, 2020, 2020],
            "Tháng": [4, 5, 6],
            "Ngày": pd.to_datetime(["2020-04-01", "2020-05-01", "2020-06-01"]),
        }
    )


@pytest.fixture
def sample_purchase_data():
    return pd.DataFrame(
        {
            "Mã hàng": ["SPC004", "SPC005"],
            "Tên hàng": ["Lốp 165/70R14", "Lốp 185/70R14"],
            "Số lượng": [10, 20],
            "Đơn giá": [47000, 48000],
            "Thành tiền": [470000, 960000],
            "Ngày": ["2020-04-01", "2020-04-02"],
            "Năm": [2020, 2020],
            "Tháng": [4, 4],
            "Mã chứng từ": ["PN-2020-04-01-001", "PN-2020-04-02-001"],
            "Tên nhà cung cấp": ["Nhà CC A", "Nhà CC B"],
        }
    )


def test_load_pipeline_config():
    config = load_pipeline_config()
    assert "dirs" in config
    assert "raw_data" in config["dirs"]
    assert "staging" in config["dirs"]


def test_load_opening_balance_config():
    config = load_opening_balance_config()
    assert "receipt_date" in config
    assert "receipt_code_prefix" in config
    assert "supplier_name" in config
    assert config["receipt_date"] == "2020-03-31"


def test_detect_earliest_month(sample_inventory_data):
    year, month = detect_earliest_month(sample_inventory_data)
    assert year == 2020
    assert month == 4


def test_detect_earliest_month_no_data():
    df_empty = pd.DataFrame({"col1": [1, 2]})
    with pytest.raises(ValueError, match="Inventory data missing"):
        detect_earliest_month(df_empty)


def test_extract_opening_balances(sample_inventory_data):
    opening_balances = extract_opening_balances(sample_inventory_data, 2020, 4)

    assert len(opening_balances) == 3
    assert "Mã hàng" in opening_balances.columns
    assert "Số lượng đầu kỳ" in opening_balances.columns
    assert list(opening_balances["Số lượng đầu kỳ"]) == [100, 50, 75]


def test_extract_opening_balances_empty_month():
    df_no_month = pd.DataFrame(
        {
            "Mã hàng": ["SPC001"],
            "Số lượng đầu kỳ": [100],
            "Năm": [2021],
            "Tháng": [10],
        }
    )

    opening_balances = extract_opening_balances(df_no_month, 2020, 12)
    assert opening_balances.empty


def test_validate_opening_balances_valid():
    df_valid = pd.DataFrame(
        {
            "Mã hàng": ["SPC001", "SPC002"],
            "Tên hàng": ["Lốp 195/65R15", "Lốp 205/55R16"],
            "Số lượng đầu kỳ": [100, 50],
            "Đơn giá đầu kỳ": [50000, 45000],
            "Thành tiền đầu kỳ": [5000000, 2250000],
        }
    )

    result = validate_opening_balances(df_valid, tolerance_pct=5.0)
    assert len(result) == 2
    assert all(result["Số lượng đầu kỳ"] > 0)


def test_validate_opening_balances_invalid():
    df_invalid = pd.DataFrame(
        {
            "Mã hàng": ["SPC001"],
            "Tên hàng": ["Lốp 195/65R15"],
            "Số lượng đầu kỳ": [100],
            "Đơn giá đầu kỳ": [50000],
            "Thành tiền đầu kỳ": [
                6000000
            ],  # Wrong total (100*50000=5000000, not 6000000)
        }
    )

    result = validate_opening_balances(df_invalid, tolerance_pct=5.0)
    assert result.empty


def test_generate_synthetic_receipts(sample_inventory_data):
    opening_balances = extract_opening_balances(sample_inventory_data, 2020, 4)
    synthetic_df = generate_synthetic_receipts(
        opening_balances,
        receipt_date="2020-03-31",
        receipt_code_prefix="PN-OB",
        supplier_name="Kho đầu kỳ",
    )

    assert len(synthetic_df) == 3
    assert "Mã hàng" in synthetic_df.columns
    assert "Mã chứng từ" in synthetic_df.columns
    assert synthetic_df["Ngày"].iloc[0] == "2020-03-31"
    assert synthetic_df["Mã chứng từ"].iloc[0] == "PN-OB-2020-03-31-0000"
    assert synthetic_df["Tên nhà cung cấp"].iloc[0] == "Kho đầu kỳ"


def test_detect_existing_synthetic_records():
    df_with_synthetic = pd.DataFrame(
        {
            "Mã hàng": ["SPC001", "SPC002"],
            "Mã chứng từ": ["PN-OB-2020-03-31-0000", "PN-OB-2020-03-31-0001"],
        }
    )

    assert detect_existing_synthetic_records(df_with_synthetic, "PN-OB")

    df_without_synthetic = pd.DataFrame(
        {
            "Mã hàng": ["SPC001", "SPC002"],
            "Mã chứng từ": ["PN-2020-04-01-001", "PN-2020-04-02-001"],
        }
    )

    assert not detect_existing_synthetic_records(df_without_synthetic, "PN-OB")

    df_missing_column = pd.DataFrame(
        {
            "Mã hàng": ["SPC001"],
        }
    )

    assert not detect_existing_synthetic_records(df_missing_column, "PN-OB")


def test_append_to_purchases(sample_purchase_data):
    opening_balances = pd.DataFrame(
        {
            "Mã hàng": ["SPC003"],
            "Tên hàng": ["Lốp 175/70R14"],
            "Số lượng đầu kỳ": [75],
            "Đơn giá đầu kỳ": [60000],
            "Thành tiền đầu kỳ": [4500000],
        }
    )

    synthetic_df = generate_synthetic_receipts(
        opening_balances,
        receipt_date="2020-03-31",
        receipt_code_prefix="PN-OB",
        supplier_name="Kho đầu kỳ",
    )

    combined_df = append_to_purchases(sample_purchase_data, synthetic_df, "2020-03-31")

    assert len(combined_df) == 3
    assert combined_df.iloc[0]["Mã chứng từ"] == "PN-OB-2020-03-31-0000"
    assert combined_df.iloc[1]["Mã chứng từ"] == "PN-2020-04-01-001"
    assert combined_df.iloc[2]["Mã chứng từ"] == "PN-2020-04-02-001"


def test_create_reconciliation_checkpoint(tmp_path):
    inventory_df = pd.DataFrame(
        {
            "Mã hàng": ["SPC001", "SPC002"],
            "Số lượng đầu kỳ": [100, 50],
            "Thành tiền đầu kỳ": [5000000, 2250000],
        }
    )

    synthetic_df = pd.DataFrame(
        {
            "Mã hàng": ["SPC001", "SPC002"],
            "Số lượng": [100, 50],
            "Thành tiền": [5000000, 2250000],
        }
    )

    report = create_reconciliation_checkpoint(
        inventory_df, synthetic_df, tmp_path, "test_script"
    )

    assert "inventory" in report
    assert "synthetic_records" in report
    assert "validation" in report
    assert report["inventory"]["total_quantity"] == 150.0
    assert report["synthetic_records"]["total_quantity"] == 150.0
    assert report["validation"]["quantity_match"]
    assert report["validation"]["value_match"]

    report_file = tmp_path.parent / "reconciliation_report_test_script.json"
    assert report_file.exists()

    with open(report_file, "r", encoding="utf-8") as f:
        saved_report = json.load(f)
        assert saved_report == report


@pytest.fixture
def tmp_path(tmp_path):
    return tmp_path / "test_output.csv"
