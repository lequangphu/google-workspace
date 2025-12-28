"""Tests for ERP template definitions."""

import pandas as pd

from src.erp import (
    CustomerTemplate,
    ERPTemplateRegistry,
    PriceBookTemplate,
    ProductTemplate,
    SupplierTemplate,
    TemplateType,
)


def test_product_template_structure():
    """Test ProductTemplate has correct structure."""
    template = ProductTemplate()
    assert len(template.COLUMNS) == 27
    assert template.COLUMNS[0].name == "Loại hàng"
    assert template.COLUMNS[2].name == "Mã hàng"
    assert template.COLUMNS[6].name == "Giá bán"


def test_product_template_required_columns():
    """Test ProductTemplate identifies required columns."""
    template = ProductTemplate()
    required = [col for col in template.COLUMNS if col.required]
    assert len(required) > 0
    assert all(
        col.name
        in ["Loại hàng", "Mã hàng", "Tên hàng", "Giá bán", "Giá vốn", "Tồn kho"]
        for col in required
    )


def test_product_template_validation_missing_required():
    """Test validation fails with missing required columns."""
    template = ProductTemplate()
    df = pd.DataFrame(
        {
            "Loại hàng": ["Hàng hóa"],
            # Missing "Mã hàng", "Tên hàng", etc.
        }
    )
    is_valid, errors = template.validate_dataframe(df)
    assert not is_valid
    assert len(errors) > 0
    assert any("Mã hàng" in err for err in errors)


def test_product_template_validation_success():
    """Test validation succeeds with all required columns."""
    template = ProductTemplate()
    df = pd.DataFrame(
        {
            "Loại hàng": ["Hàng hóa"],
            "Mã hàng": ["HH000001"],
            "Tên hàng": ["Sản phẩm test"],
            "Giá bán": [100.00],
            "Giá vốn": [50.00],
            "Tồn kho": [10],
        }
    )
    is_valid, errors = template.validate_dataframe(df)
    assert is_valid
    assert len(errors) == 0


def test_pricebook_template_structure():
    """Test PriceBookTemplate has correct structure."""
    template = PriceBookTemplate()
    assert len(template.COLUMNS) == 5
    assert template.COLUMNS[0].name == "Mã hàng"
    assert template.COLUMNS[1].name == "Tên hàng"


def test_customer_template_structure():
    """Test CustomerTemplate has correct structure."""
    template = CustomerTemplate()
    assert len(template.COLUMNS) == 20
    assert template.COLUMNS[0].name == "Loại khách"
    assert template.COLUMNS[1].name == "Mã khách hàng"


def test_supplier_template_structure():
    """Test SupplierTemplate has correct structure."""
    template = SupplierTemplate()
    assert len(template.COLUMNS) == 15
    assert template.COLUMNS[0].name == "Mã nhà cung cấp"
    assert template.COLUMNS[1].name == "Tên nhà cung cấp"


def test_erp_template_registry():
    """Test ERPTemplateRegistry returns correct templates."""
    product_template = ERPTemplateRegistry.get_template(TemplateType.PRODUCTS)
    assert isinstance(product_template, ProductTemplate)

    pricebook_template = ERPTemplateRegistry.get_template(TemplateType.PRICEBOOK)
    assert isinstance(pricebook_template, PriceBookTemplate)

    customer_template = ERPTemplateRegistry.get_template(TemplateType.CUSTOMERS)
    assert isinstance(customer_template, CustomerTemplate)

    supplier_template = ERPTemplateRegistry.get_template(TemplateType.SUPPLIERS)
    assert isinstance(supplier_template, SupplierTemplate)


def test_erp_template_registry_validation():
    """Test ERPTemplateRegistry validation method."""
    df = pd.DataFrame(
        {
            "Loại hàng": ["Hàng hóa"],
            "Mã hàng": ["HH000001"],
            "Tên hàng": ["Sản phẩm test"],
            "Giá bán": [100.00],
            "Giá vốn": [50.00],
            "Tồn kho": [10],
        }
    )
    is_valid, errors = ERPTemplateRegistry.validate_dataframe(TemplateType.PRODUCTS, df)
    assert is_valid


def test_get_column_names():
    """Test getting column names from templates."""
    product_template = ProductTemplate()
    column_names = product_template.get_column_names()
    assert len(column_names) == 27
    assert column_names[0] == "Loại hàng"
    assert column_names[2] == "Mã hàng"
