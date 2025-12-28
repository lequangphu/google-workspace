# -*- coding: utf-8 -*-
"""Tests for receivable.extract_customer_ids module."""

import logging
from pathlib import Path

import pandas as pd
import pytest

from src.modules.receivable.extract_customer_ids import (
    aggregate_customer_data,
    find_input_file,
    rank_and_generate_ids,
    read_sale_receipt_data,
    save_to_csv,
)

# ============================================================================
# FIXTURES
# ============================================================================


@pytest.fixture
def sample_sale_receipt_data():
    """Sample sale receipt data with multiple transactions per customer."""
    return pd.DataFrame(
        {
            "Tên khách hàng": [
                "Customer A",
                "Customer A",
                "Customer B",
                "Customer B",
                "Customer B",
                "Customer C",
            ],
            "Ngày": [
                "2024-01-15",
                "2024-02-20",
                "2024-01-10",
                "2024-03-05",
                "2024-03-20",
                "2024-02-01",
            ],
            "Thành tiền": [1000, 2000, 500, 1500, 2500, 3000],
        }
    )


@pytest.fixture
def sample_aggregated_data():
    """Sample aggregated customer data (output of aggregate_customer_data)."""
    return pd.DataFrame(
        {
            "Tên khách hàng": ["Customer A", "Customer B", "Customer C"],
            "first_date": pd.to_datetime(["2024-01-15", "2024-01-10", "2024-02-01"]),
            "last_date": pd.to_datetime(["2024-02-20", "2024-03-20", "2024-02-01"]),
            "total_amount": [3000, 4500, 3000],
            "transaction_count": [2, 3, 1],
        }
    )


@pytest.fixture
def temp_staging_dir(tmp_path):
    """Create a temporary staging directory structure."""
    import_export_dir = tmp_path / "01-staging" / "import_export"
    receivable_dir = tmp_path / "01-staging" / "receivable"
    import_export_dir.mkdir(parents=True, exist_ok=True)
    receivable_dir.mkdir(parents=True, exist_ok=True)
    return {
        "import_export": import_export_dir,
        "receivable": receivable_dir,
        "root": tmp_path,
    }


# ============================================================================
# TEST: find_input_file
# ============================================================================


def test_find_input_file_exists(temp_staging_dir):
    """Test finding existing input file."""
    import_export_dir = temp_staging_dir["import_export"]

    # Create test file
    test_file = import_export_dir / "clean_receipts_sale_2024.csv"
    test_file.write_text("test")

    result = find_input_file(import_export_dir)
    assert result == test_file


def test_find_input_file_multiple_files(temp_staging_dir):
    """Test finding most recent file when multiple exist."""
    import_export_dir = temp_staging_dir["import_export"]

    # Create multiple test files
    file1 = import_export_dir / "clean_receipts_sale_2024_01.csv"
    file2 = import_export_dir / "clean_receipts_sale_2024_02.csv"
    file1.write_text("test")
    file2.write_text("test")

    # Make file2 newer
    file2.touch()

    result = find_input_file(import_export_dir)
    assert result == file2


def test_find_input_file_not_found(temp_staging_dir):
    """Test when no input file exists."""
    import_export_dir = temp_staging_dir["import_export"]
    result = find_input_file(import_export_dir)
    assert result is None


def test_find_input_file_directory_not_found(tmp_path):
    """Test when staging directory doesn't exist."""
    nonexistent_dir = tmp_path / "nonexistent"
    result = find_input_file(nonexistent_dir)
    assert result is None


# ============================================================================
# TEST: read_sale_receipt_data
# ============================================================================


def test_read_sale_receipt_data_success(temp_staging_dir, sample_sale_receipt_data):
    """Test successfully reading sale receipt data."""
    input_file = temp_staging_dir["import_export"] / "clean_receipts_sale_test.csv"
    sample_sale_receipt_data.to_csv(input_file, index=False, encoding="utf-8")

    result = read_sale_receipt_data(input_file)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 6
    assert "Tên khách hàng" in result.columns
    assert "Ngày" in result.columns


def test_read_sale_receipt_data_file_not_found():
    """Test reading non-existent file."""
    nonexistent_file = Path("/nonexistent/file.csv")

    with pytest.raises(Exception):
        read_sale_receipt_data(nonexistent_file)


def test_read_sale_receipt_data_invalid_encoding(temp_staging_dir):
    """Test reading file with invalid encoding."""
    input_file = temp_staging_dir["import_export"] / "invalid_encoding.csv"
    # Write file with different encoding
    input_file.write_text("test data", encoding="latin-1")

    # Should still work due to encoding fallback
    result = read_sale_receipt_data(input_file)
    assert isinstance(result, pd.DataFrame)


# ============================================================================
# TEST: aggregate_customer_data
# ============================================================================


def test_aggregate_customer_data_success(sample_sale_receipt_data):
    """Test successful customer data aggregation."""
    result = aggregate_customer_data(sample_sale_receipt_data)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3  # 3 unique customers
    assert list(result.columns) == [
        "Tên khách hàng",
        "first_date",
        "last_date",
        "total_amount",
        "transaction_count",
    ]
    assert (
        result[result["Tên khách hàng"] == "Customer A"]["transaction_count"].values[0]
        == 2
    )
    assert (
        result[result["Tên khách hàng"] == "Customer B"]["transaction_count"].values[0]
        == 3
    )
    assert (
        result[result["Tên khách hàng"] == "Customer C"]["transaction_count"].values[0]
        == 1
    )


def test_aggregate_customer_data_missing_customer_column(sample_sale_receipt_data):
    """Test error when customer column missing."""
    df = sample_sale_receipt_data.drop("Tên khách hàng", axis=1)

    with pytest.raises(ValueError, match="Missing required columns"):
        aggregate_customer_data(df)


def test_aggregate_customer_data_missing_date_column(sample_sale_receipt_data):
    """Test error when date column missing."""
    df = sample_sale_receipt_data.drop("Ngày", axis=1)

    with pytest.raises(ValueError, match="Missing required columns"):
        aggregate_customer_data(df)


def test_aggregate_customer_data_with_null_customers(sample_sale_receipt_data):
    """Test that null customer names are removed."""
    sample_sale_receipt_data.loc[0, "Tên khách hàng"] = None

    result = aggregate_customer_data(sample_sale_receipt_data)

    assert len(result) == 3  # Still 3 after removing null
    assert result["Tên khách hàng"].isnull().sum() == 0


def test_aggregate_customer_data_date_parsing(sample_sale_receipt_data):
    """Test that dates are parsed correctly."""
    result = aggregate_customer_data(sample_sale_receipt_data)

    # Check that dates are datetime objects
    assert isinstance(result["first_date"].iloc[0], pd.Timestamp)
    assert isinstance(result["last_date"].iloc[0], pd.Timestamp)


def test_aggregate_customer_data_total_amount_without_thanhTien(
    sample_sale_receipt_data,
):
    """Test aggregation when Thành tiền column is missing."""
    df = sample_sale_receipt_data.drop("Thành tiền", axis=1)
    df["Số lượng"] = [1, 2, 3, 4, 5, 6]

    result = aggregate_customer_data(df)

    assert "total_amount" in result.columns
    # Should use Số lượng as fallback
    assert (
        result[result["Tên khách hàng"] == "Customer A"]["total_amount"].values[0] == 3
    )


# ============================================================================
# TEST: rank_and_generate_ids
# ============================================================================


def test_rank_and_generate_ids_sorting(sample_aggregated_data):
    """Test correct sorting by first_date then total_amount."""
    result = rank_and_generate_ids(sample_aggregated_data)

    # Verify sorting: Customer B (earliest first_date=2024-01-10) should be first
    assert result.iloc[0]["Tên khách hàng"] == "Customer B"
    assert result.iloc[0]["Mã khách hàng mới"] == "KH000001"

    # Customer A (2024-01-15) should be second
    assert result.iloc[1]["Tên khách hàng"] == "Customer A"
    assert result.iloc[1]["Mã khách hàng mới"] == "KH000002"

    # Customer C (2024-02-01) should be third
    assert result.iloc[2]["Tên khách hàng"] == "Customer C"
    assert result.iloc[2]["Mã khách hàng mới"] == "KH000003"


def test_rank_and_generate_ids_format(sample_aggregated_data):
    """Test correct ID format (KH000001, etc)."""
    result = rank_and_generate_ids(sample_aggregated_data)

    for idx, row in result.iterrows():
        expected_id = f"KH{idx + 1:06d}"
        assert row["Mã khách hàng mới"] == expected_id


def test_rank_and_generate_ids_column_order(sample_aggregated_data):
    """Test output column order and naming."""
    result = rank_and_generate_ids(sample_aggregated_data)

    expected_columns = [
        "Mã khách hàng mới",
        "Tên khách hàng",
        "Ngày giao dịch đầu",
        "Ngày giao dịch cuối",
        "Tổng tiền",
        "Số lần giao dịch",
    ]
    assert list(result.columns) == expected_columns


def test_rank_and_generate_ids_large_dataset():
    """Test with large number of customers."""
    df = pd.DataFrame(
        {
            "Tên khách hàng": [f"Customer {i}" for i in range(1000)],
            "first_date": pd.date_range("2024-01-01", periods=1000),
            "last_date": pd.date_range("2024-02-01", periods=1000),
            "total_amount": range(1000, 2000),
            "transaction_count": [1] * 1000,
        }
    )

    result = rank_and_generate_ids(df)

    assert len(result) == 1000
    # Check first and last IDs
    assert result.iloc[0]["Mã khách hàng mới"] == "KH000001"
    assert result.iloc[999]["Mã khách hàng mới"] == "KH001000"


# ============================================================================
# TEST: save_to_csv
# ============================================================================


def test_save_to_csv_creates_directory(temp_staging_dir, sample_aggregated_data):
    """Test that save_to_csv creates parent directories."""
    output_path = temp_staging_dir["root"] / "new_dir" / "output.csv"

    result = rank_and_generate_ids(sample_aggregated_data)
    save_to_csv(result, output_path)

    assert output_path.exists()
    assert output_path.is_file()


def test_save_to_csv_content(temp_staging_dir, sample_aggregated_data):
    """Test that CSV content is saved correctly."""
    output_path = temp_staging_dir["receivable"] / "output.csv"

    result = rank_and_generate_ids(sample_aggregated_data)
    save_to_csv(result, output_path)

    # Read back and verify
    saved_df = pd.read_csv(output_path)
    assert len(saved_df) == len(result)
    assert list(saved_df.columns) == list(result.columns)


def test_save_to_csv_encoding(temp_staging_dir, sample_aggregated_data):
    """Test that CSV is saved with UTF-8 encoding."""
    output_path = temp_staging_dir["receivable"] / "output.csv"

    result = rank_and_generate_ids(sample_aggregated_data)
    save_to_csv(result, output_path)

    # Try reading with UTF-8
    df = pd.read_csv(output_path, encoding="utf-8")
    assert len(df) == len(result)


# ============================================================================
# INTEGRATION TESTS
# ============================================================================


def test_full_pipeline_success(temp_staging_dir, sample_sale_receipt_data):
    """Test full pipeline: read → aggregate → rank → save."""
    # Setup
    input_file = temp_staging_dir["import_export"] / "clean_receipts_sale_test.csv"
    sample_sale_receipt_data.to_csv(input_file, index=False, encoding="utf-8")

    output_file = temp_staging_dir["receivable"] / "extract_customer_ids.csv"

    # Execute
    df = read_sale_receipt_data(input_file)
    aggregated = aggregate_customer_data(df)
    ranked = rank_and_generate_ids(aggregated)
    save_to_csv(ranked, output_file)

    # Verify
    assert output_file.exists()
    result = pd.read_csv(output_file)
    assert len(result) == 3
    assert result.iloc[0]["Mã khách hàng mới"] == "KH000001"


def test_full_pipeline_with_edge_cases(temp_staging_dir):
    """Test pipeline with edge cases (null, duplicates, etc)."""
    # Create sample data with edge cases
    df = pd.DataFrame(
        {
            "Tên khách hàng": [
                "Customer A",
                "Customer A",
                None,
                "Customer B",
                "",
                "Customer B",
            ],
            "Ngày": [
                "2024-01-15",
                "2024-02-20",
                "2024-01-10",
                "2024-01-10",
                "2024-03-05",
                "2024-03-20",
            ],
            "Thành tiền": [1000, 2000, 500, 1500, 2500, 3000],
        }
    )

    input_file = temp_staging_dir["import_export"] / "edge_cases.csv"
    df.to_csv(input_file, index=False, encoding="utf-8")

    output_file = temp_staging_dir["receivable"] / "output.csv"

    # Execute
    df = read_sale_receipt_data(input_file)
    aggregated = aggregate_customer_data(df)
    ranked = rank_and_generate_ids(aggregated)
    save_to_csv(ranked, output_file)

    # Verify
    result = pd.read_csv(output_file)
    # Should have 2 customers (Customer A and B, null removed)
    assert len(result) == 2


# ============================================================================
# LOGGING TESTS
# ============================================================================


def test_logging_output(temp_staging_dir, sample_sale_receipt_data, caplog):
    """Test that appropriate log messages are generated."""
    with caplog.at_level(logging.INFO):
        input_file = temp_staging_dir["import_export"] / "test.csv"
        sample_sale_receipt_data.to_csv(input_file, index=False, encoding="utf-8")

        df = read_sale_receipt_data(input_file)
        _ = aggregate_customer_data(df)

        # Check for expected log messages
        assert "Loaded data from" in caplog.text
        assert "Aggregated" in caplog.text
