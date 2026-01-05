"""Tests for ERP exporter functions.

DEPRECATED: The export_products_xlsx function has been migrated to
generate_products_xlsx.py. These tests will be removed on 2026-01-14.
"""

import pytest

pytest.importorskip("openpyxl")

import pandas as pd

from src.erp import (
    export_customers_xlsx,
    export_pricebook_xlsx,
)


@pytest.fixture
def sample_product_info_csv(tmp_path):
    """Create sample product_info.csv."""
    data = {
        "Mã hàng mới": ["HH000001", "HH000002"],
        "Mã hàng": ["SP001", "SP002"],
        "Tên hàng": ["Sản phẩm 1", "Sản phẩm 2"],
    }
    df = pd.DataFrame(data)
    path = tmp_path / "product_info.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def sample_inventory_csv(tmp_path):
    """Create sample inventory.csv."""
    data = {
        "Mã hàng mới": ["HH000001", "HH000002"],
        "Tồn số lượng": [100, 50],
        "Giá vốn FIFO": [50.00, 75.00],
    }
    df = pd.DataFrame(data)
    path = tmp_path / "inventory.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def sample_price_sale_csv(tmp_path):
    """Create sample price_sale.csv."""
    data = {
        "Mã hàng mới": ["HH000001", "HH000002"],
        "Giá xuất cuối": [100.00, 150.00],
    }
    df = pd.DataFrame(data)
    path = tmp_path / "price_sale.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def sample_enrichment_csv(tmp_path):
    """Create sample enrichment.csv."""
    data = {
        "Mã hàng": ["SP001", "SP002"],
        "Nhóm hàng(3 Cấp)": ["Nhóm A > Chi tiết A", "Nhóm B > Chi tiết B"],
        "Thương hiệu": ["Brand A", "Brand B"],
    }
    df = pd.DataFrame(data)
    path = tmp_path / "enrichment.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture
def sample_customer_ids_csv(tmp_path):
    """Create sample extract_customer_ids.csv."""
    data = {
        "Mã khách hàng": ["KH000001", "KH000002"],
        "Tên khách hàng": ["Khách hàng 1", "Khách hàng 2"],
        "Điện thoại": ["0123456789", "0987654321"],
        "Email": ["kh1@example.com", "kh2@example.com"],
    }
    df = pd.DataFrame(data)
    path = tmp_path / "customer_ids.csv"
    df.to_csv(path, index=False)
    return path


@pytest.mark.skip(
    reason="export_products_xlsx deprecated, migrated to generate_products_xlsx.py"
)
def test_export_products_xlsx(
    sample_product_info_csv,
    sample_inventory_csv,
    sample_price_sale_csv,
    sample_enrichment_csv,
    tmp_path,
):
    """Test export_products_xlsx creates XLSX file with data."""
    output_path = tmp_path / "Products.xlsx"

    result_path, stats = export_products_xlsx(
        product_info_path=sample_product_info_csv,
        inventory_path=sample_inventory_csv,
        price_sale_path=sample_price_sale_csv,
        enrichment_path=sample_enrichment_csv,
        output_path=output_path,
    )

    assert result_path == output_path
    assert output_path.exists()
    assert stats["products_exported"] == 2
    assert "Products.xlsx" in stats["output_file"]

    # Verify XLSX structure
    df = pd.read_excel(output_path)
    assert len(df) == 2
    assert "Mã hàng" in df.columns
    assert "Tên hàng" in df.columns
    assert "Giá bán" in df.columns
    assert "Giá vốn" in df.columns
    assert "Tồn kho" in df.columns


def test_export_customers_xlsx(sample_customer_ids_csv, tmp_path):
    """Test export_customers_xlsx creates XLSX file with customer data."""
    output_path = tmp_path / "Customers.xlsx"

    result_path, stats = export_customers_xlsx(
        customer_ids_path=sample_customer_ids_csv,
        enrichment_path=sample_customer_ids_csv,
        output_path=output_path,
    )

    assert result_path == output_path
    assert output_path.exists()
    assert stats["customers_exported"] == 2
    assert "Customers.xlsx" in stats["output_file"]

    # Verify XLSX structure
    df = pd.read_excel(output_path)
    assert len(df) == 2
    assert "Loại khách" in df.columns
    assert "Mã khách hàng" in df.columns
    assert "Tên khách hàng" in df.columns


def test_export_pricebook_xlsx(
    sample_product_info_csv,
    sample_price_sale_csv,
    tmp_path,
):
    """Test export_pricebook_xlsx creates XLSX file with price data."""
    output_path = tmp_path / "PriceBook.xlsx"

    result_path, stats = export_pricebook_xlsx(
        product_info_path=sample_product_info_csv,
        price_sale_path=sample_price_sale_csv,
        output_path=output_path,
    )

    assert result_path == output_path
    assert output_path.exists()
    assert stats["products_in_pricebook"] == 2
    assert "PriceBook.xlsx" in stats["output_file"]

    # Verify XLSX structure
    df = pd.read_excel(output_path)
    assert len(df) == 2
    assert "Mã hàng" in df.columns
    assert "Tên hàng" in df.columns


@pytest.mark.skip(
    reason="export_products_xlsx deprecated, migrated to generate_products_xlsx.py"
)
def test_export_products_xlsx_missing_required_column(
    sample_product_info_csv,
    sample_inventory_csv,
    tmp_path,
):
    """Test export_products_xlsx fails with missing required column."""
    # Create price_sale without Giá xuất cuối
    bad_price_csv = tmp_path / "bad_price.csv"
    pd.DataFrame({"Mã hàng mới": ["HH000001"]}).to_csv(bad_price_csv, index=False)

    enrichment_csv = tmp_path / "enrichment.csv"
    pd.DataFrame({"Mã hàng": ["SP001"]}).to_csv(enrichment_csv, index=False)

    output_path = tmp_path / "Products.xlsx"

    with pytest.raises(ValueError, match="Missing required column"):
        export_products_xlsx(
            product_info_path=sample_product_info_csv,
            inventory_path=sample_inventory_csv,
            price_sale_path=bad_price_csv,
            enrichment_path=enrichment_csv,
            output_path=output_path,
        )


def test_export_customers_xlsx_missing_required_column(tmp_path):
    """Test export_customers_xlsx fails with missing required column."""
    # Create customer CSV without Tên khách hàng
    bad_customer_csv = tmp_path / "bad_customer.csv"
    pd.DataFrame({"Mã khách hàng": ["KH000001"]}).to_csv(bad_customer_csv, index=False)

    output_path = tmp_path / "Customers.xlsx"

    with pytest.raises(ValueError, match="Missing required column"):
        export_customers_xlsx(
            customer_ids_path=bad_customer_csv,
            enrichment_path=bad_customer_csv,
            output_path=output_path,
        )
