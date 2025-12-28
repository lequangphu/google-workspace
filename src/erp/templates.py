"""KiotViet ERP template specifications.

Defines column requirements, data types, and validation rules for each
KiotViet import template (Products, PriceBook, Customers, Suppliers).
"""

from dataclasses import dataclass
from enum import Enum
from typing import List, Tuple

import pandas as pd


class TemplateType(Enum):
    """KiotViet template types."""

    PRODUCTS = "PRODUCTS"
    PRICEBOOK = "PRICEBOOK"
    CUSTOMERS = "CUSTOMERS"
    SUPPLIERS = "SUPPLIERS"


@dataclass
class ColumnSpec:
    """Specification for a single column in a KiotViet template."""

    name: str  # Vietnamese column name (e.g., "Mã hàng")
    column_index: int  # 0-based column index
    data_type: str  # "text", "number", "date"
    format_code: str | None  # Excel format code (e.g., "#,0.##0")
    required: bool  # Is column required in import?


class ProductTemplate:
    """27-column Products template for KiotViet (Sản phẩm)."""

    COLUMNS = [
        ColumnSpec("Loại hàng", 0, "text", None, required=True),
        ColumnSpec("Nhóm hàng(3 Cấp)", 1, "text", None, required=False),
        ColumnSpec("Mã hàng", 2, "text", None, required=True),
        ColumnSpec("Mã vạch", 3, "text", None, required=False),
        ColumnSpec("Tên hàng", 4, "text", None, required=True),
        ColumnSpec("Thương hiệu", 5, "text", None, required=False),
        ColumnSpec("Giá bán", 6, "number", "#,0.##0", required=True),
        ColumnSpec("Giá vốn", 7, "number", "#,0.##0", required=True),
        ColumnSpec("Tồn kho", 8, "number", "#,0.##0", required=True),
        ColumnSpec("Tồn nhỏ nhất", 9, "number", "#,0.##0", required=False),
        ColumnSpec("Tồn lớn nhất", 10, "number", "#,0.##0", required=False),
        ColumnSpec("ĐVT", 11, "text", None, required=False),
        ColumnSpec("Mã ĐVT Cơ bản", 12, "text", None, required=False),
        ColumnSpec("Quy đổi", 13, "number", "#,0.##0", required=False),
        ColumnSpec("Thuộc tính", 14, "text", None, required=False),
        ColumnSpec("Mã HH Liên quan", 15, "text", None, required=False),
        ColumnSpec("Hình ảnh (url1,url2...)", 16, "text", None, required=False),
        ColumnSpec("Sử dụng Imei", 17, "number", None, required=False),
        ColumnSpec("Trọng lượng", 18, "number", "#,0.##0", required=False),
        ColumnSpec("Đang kinh doanh", 19, "number", None, required=False),
        ColumnSpec("Được bán trực tiếp", 20, "number", None, required=False),
        ColumnSpec("Mô tả", 21, "text", None, required=False),
        ColumnSpec("Mẫu ghi chú", 22, "text", None, required=False),
        ColumnSpec("Vị trí", 23, "text", None, required=False),
        ColumnSpec("Hàng thành phần", 24, "text", None, required=False),
        ColumnSpec("Bảo hành", 25, "text", None, required=False),
        ColumnSpec("Bảo trì định kỳ", 26, "text", None, required=False),
    ]

    def validate_dataframe(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate DataFrame against template.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Check required columns
        for col_spec in self.COLUMNS:
            if col_spec.required and col_spec.name not in df.columns:
                errors.append(f"Missing required column: {col_spec.name}")

        # Check data types (basic)
        for col_spec in self.COLUMNS:
            if col_spec.name not in df.columns:
                continue
            if col_spec.data_type == "number":
                try:
                    pd.to_numeric(df[col_spec.name], errors="coerce")
                except Exception as e:
                    errors.append(
                        f"Column '{col_spec.name}' has invalid numeric data: {str(e)}"
                    )

        return len(errors) == 0, errors

    def get_column_names(self) -> List[str]:
        """Get all column names in order."""
        return [col.name for col in self.COLUMNS]


class PriceBookTemplate:
    """5-column PriceBook template for KiotViet (Bảng giá)."""

    COLUMNS = [
        ColumnSpec("Mã hàng", 0, "text", None, required=True),
        ColumnSpec("Tên hàng", 1, "text", None, required=True),
        ColumnSpec("Tên bảng giá 1", 2, "number", "#,0.##0", required=False),
        ColumnSpec("Tên bảng giá 2", 3, "number", "#,0.##0", required=False),
        ColumnSpec("Tên bảng giá 3", 4, "number", "#,0.##0", required=False),
    ]

    def validate_dataframe(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate DataFrame against template.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Check required columns
        for col_spec in self.COLUMNS:
            if col_spec.required and col_spec.name not in df.columns:
                errors.append(f"Missing required column: {col_spec.name}")

        # Check data types (basic)
        for col_spec in self.COLUMNS:
            if col_spec.name not in df.columns:
                continue
            if col_spec.data_type == "number":
                try:
                    pd.to_numeric(df[col_spec.name], errors="coerce")
                except Exception as e:
                    errors.append(
                        f"Column '{col_spec.name}' has invalid numeric data: {str(e)}"
                    )

        return len(errors) == 0, errors

    def get_column_names(self) -> List[str]:
        """Get all column names in order."""
        return [col.name for col in self.COLUMNS]


class CustomerTemplate:
    """20-column Customers template for KiotViet (Khách hàng)."""

    COLUMNS = [
        ColumnSpec("Loại khách", 0, "text", None, required=True),
        ColumnSpec("Mã khách hàng", 1, "text", None, required=True),
        ColumnSpec("Tên khách hàng", 2, "text", None, required=True),
        ColumnSpec("Điện thoại", 3, "text", None, required=False),
        ColumnSpec("Địa chỉ", 4, "text", None, required=False),
        ColumnSpec("Khu vực giao hàng", 5, "text", None, required=False),
        ColumnSpec("Phường/Xã", 6, "text", None, required=False),
        ColumnSpec("Công ty", 7, "text", None, required=False),
        ColumnSpec("Mã số thuế", 8, "text", None, required=False),
        ColumnSpec("Số CMND/CCCD", 9, "text", None, required=False),
        ColumnSpec("Ngày sinh", 10, "date", "dd/MM/yyyy", required=False),
        ColumnSpec("Giới tính", 11, "text", None, required=False),
        ColumnSpec("Email", 12, "text", None, required=False),
        ColumnSpec("Facebook", 13, "text", None, required=False),
        ColumnSpec("Nhóm khách hàng", 14, "text", None, required=False),
        ColumnSpec("Ghi chú", 15, "text", None, required=False),
        ColumnSpec("Ngày giao dịch cuối", 16, "date", "dd/MM/yyyy", required=False),
        ColumnSpec("Nợ cần thu hiện tại", 17, "number", "#,##0", required=False),
        ColumnSpec("Tổng bán (Không import)", 18, "number", "#,##0", required=False),
        ColumnSpec("Trạng thái", 19, "number", None, required=False),
    ]

    def validate_dataframe(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate DataFrame against template.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Check required columns
        for col_spec in self.COLUMNS:
            if col_spec.required and col_spec.name not in df.columns:
                errors.append(f"Missing required column: {col_spec.name}")

        # Check data types (basic)
        for col_spec in self.COLUMNS:
            if col_spec.name not in df.columns:
                continue
            if col_spec.data_type == "number":
                try:
                    pd.to_numeric(df[col_spec.name], errors="coerce")
                except Exception as e:
                    errors.append(
                        f"Column '{col_spec.name}' has invalid numeric data: {str(e)}"
                    )

        return len(errors) == 0, errors

    def get_column_names(self) -> List[str]:
        """Get all column names in order."""
        return [col.name for col in self.COLUMNS]


class SupplierTemplate:
    """15-column Suppliers template for KiotViet (Nhà cung cấp)."""

    COLUMNS = [
        ColumnSpec("Mã nhà cung cấp", 0, "text", None, required=True),
        ColumnSpec("Tên nhà cung cấp", 1, "text", None, required=True),
        ColumnSpec("Email", 2, "text", None, required=False),
        ColumnSpec("Điện thoại", 3, "text", None, required=False),
        ColumnSpec("Địa chỉ", 4, "text", None, required=False),
        ColumnSpec("Khu vực", 5, "text", None, required=False),
        ColumnSpec("Phường/Xã", 6, "text", None, required=False),
        ColumnSpec("Tổng mua (Không Import)", 7, "number", "#,##0", required=False),
        ColumnSpec("Nợ cần trả hiện tại", 8, "number", "#,##0", required=False),
        ColumnSpec("Mã số thuế", 9, "text", None, required=False),
        ColumnSpec("Ghi chú", 10, "text", None, required=False),
        ColumnSpec("Nhóm nhà cung cấp", 11, "text", None, required=False),
        ColumnSpec("Trạng thái", 12, "number", None, required=False),
        ColumnSpec("Tổng mua trừ trả hàng", 13, "number", "#,##0", required=False),
        ColumnSpec("Công ty", 14, "text", None, required=False),
    ]

    def validate_dataframe(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate DataFrame against template.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Check required columns
        for col_spec in self.COLUMNS:
            if col_spec.required and col_spec.name not in df.columns:
                errors.append(f"Missing required column: {col_spec.name}")

        # Check data types (basic)
        for col_spec in self.COLUMNS:
            if col_spec.name not in df.columns:
                continue
            if col_spec.data_type == "number":
                try:
                    pd.to_numeric(df[col_spec.name], errors="coerce")
                except Exception as e:
                    errors.append(
                        f"Column '{col_spec.name}' has invalid numeric data: {str(e)}"
                    )

        return len(errors) == 0, errors

    def get_column_names(self) -> List[str]:
        """Get all column names in order."""
        return [col.name for col in self.COLUMNS]


class ERPTemplateRegistry:
    """Registry for accessing all ERP templates."""

    _templates = {
        TemplateType.PRODUCTS: ProductTemplate(),
        TemplateType.PRICEBOOK: PriceBookTemplate(),
        TemplateType.CUSTOMERS: CustomerTemplate(),
        TemplateType.SUPPLIERS: SupplierTemplate(),
    }

    @classmethod
    def get_template(cls, template_type: TemplateType):
        """Get a template by type.

        Args:
            template_type: TemplateType enum value

        Returns:
            Template instance
        """
        if template_type not in cls._templates:
            raise ValueError(f"Unknown template type: {template_type}")
        return cls._templates[template_type]

    @classmethod
    def validate_dataframe(
        cls, template_type: TemplateType, df: pd.DataFrame
    ) -> Tuple[bool, List[str]]:
        """Validate a DataFrame against a specific template.

        Args:
            template_type: TemplateType enum value
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        template = cls.get_template(template_type)
        return template.validate_dataframe(df)
