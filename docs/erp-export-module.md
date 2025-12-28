# ERP Export Module – Templates & Exporter

Documentation for the ERP integration module that exports validated data to KiotViet XLSX templates.

## Overview

The ERP export module bridges the gap between validated CSV data (`data/02-validated/`) and KiotViet's strict XLSX requirements (`data/03-erp-export/`).

**Core components:**
1. **templates.py** – Define KiotViet template specifications (columns, types, validation)
2. **exporter.py** – Convert validated DataFrames → XLSX with formatting

## Module Structure

### `src/erp/templates.py`

Defines template specifications for all KiotViet import types.

**Classes:**
- `ColumnSpec` – Single column definition (name, index, type, format, required flag)
- `ProductTemplate` – 27-column Products template
- `PriceBookTemplate` – 5-column PriceBook template
- `CustomerTemplate` – 20-column Customers template
- `SupplierTemplate` – 15-column Suppliers template
- `ERPTemplateRegistry` – Central registry for template access & validation

**Key methods:**
```python
# Get a template by type
template = ERPTemplateRegistry.get_template(TemplateType.PRODUCTS)

# Validate DataFrame against template
is_valid, errors = template.validate_dataframe(df)

# Get column names in order
columns = template.get_column_names()
```

### `src/erp/exporter.py`

Exports validated DataFrames to KiotViet XLSX files.

**Export functions:**

#### `export_products_xlsx()`
Exports 9-column Product data to XLSX.

**Inputs:**
- `product_info_path` – Contains: Mã hàng mới, Mã hàng, Tên hàng
- `inventory_path` – Contains: Mã hàng mới, Tồn số lượng, Giá vốn FIFO
- `price_sale_path` – Contains: Mã hàng mới, Giá xuất cuối
- `enrichment_path` – Contains: Mã hàng, Nhóm hàng(3 Cấp), Thương hiệu

**Output columns (mapped to KiotViet template):**
1. Loại hàng ← "Hàng hóa" (constant)
2. Nhóm hàng(3 Cấp) ← enrichment
3. Mã hàng ← product_info (HH000001...)
4. Mã vạch ← product_info (original code)
5. Tên hàng ← product_info
6. Thương hiệu ← enrichment
7. Giá bán ← price_sale (max selling price)
8. Giá vốn ← inventory (FIFO unit cost)
9. Tồn kho ← inventory (stock quantity)

**Returns:**
- `(output_path, stats_dict)` where stats_dict contains product count and output file path

**Example:**
```python
from src.erp import export_products_xlsx
from pathlib import Path

output_path, stats = export_products_xlsx(
    product_info_path=Path("data/02-validated/product_info.csv"),
    inventory_path=Path("data/02-validated/inventory.csv"),
    price_sale_path=Path("data/02-validated/price_sale.csv"),
    enrichment_path=Path("data/01-staging/enrichment.csv"),
    output_path=Path("data/03-erp-export/Products.xlsx"),
)
print(f"Exported {stats['products_exported']} products")
```

#### `export_customers_xlsx()`
Exports Customer data with minimum 3 required columns.

**Inputs:**
- `customer_ids_path` – Contains: Mã khách hàng, Tên khách hàng, (optional: Điện thoại, Email, Ghi chú)
- `enrichment_path` – Additional customer enrichment data (optional)

**Output columns:**
1. Loại khách ← "Cá nhân" (constant)
2. Mã khách hàng ← customer_ids
3. Tên khách hàng ← customer_ids
4+ Điện thoại, Email, Ghi chú (if present)

#### `export_pricebook_xlsx()`
Exports Price Book data with product IDs and prices.

**Inputs:**
- `product_info_path` – Contains: Mã hàng mới, Tên hàng
- `price_sale_path` – Contains: Mã hàng mới, Giá xuất cuối

**Output columns:**
1. Mã hàng
2. Tên hàng
3. Tên bảng giá 1 (selling price)
4-5. (optional price lists)

## Data Flow

```
data/02-validated/
├── product_info.csv
├── inventory.csv
├── price_sale.csv
└── (+ other reports)

data/01-staging/
└── enrichment.csv (Nhóm hàng, Thương hiệu)

                        ↓ export_products_xlsx()

data/03-erp-export/
└── Products.xlsx  ← Ready for KiotViet import
```

## Validation

All templates validate:
- **Required columns** – Raises `ValueError` if missing
- **Data types** – Checks numeric columns for valid numbers
- **Format codes** – Excel format strings applied (e.g., "#,0.##0")

**Validation examples:**
```python
from src.erp import ERPTemplateRegistry, TemplateType
import pandas as pd

df = pd.read_csv("data/02-validated/product_info.csv")
is_valid, errors = ERPTemplateRegistry.validate_dataframe(TemplateType.PRODUCTS, df)

if not is_valid:
    for error in errors:
        print(f"Error: {error}")
```

## Excel Formatting

Products XLSX includes:
- **Header row** – Blue background (#4472C4), white bold font
- **Column widths** – 18pt for readability
- **Number formats** – Applied per KiotViet spec:
  - Prices: `#,0.##0` (thousands separator, 2-3 decimals)
  - Stock: `#,0.##0` (integer with thousands separator)
  - Text: Left-aligned
  - Numbers: Right-aligned

## Configuration

**Dependencies added to `pyproject.toml`:**
```toml
openpyxl>=3.0.0  # XLSX reading/writing and formatting
```

**Install:**
```bash
uv sync
```

## Testing

**Test files:**
- `tests/test_templates.py` – Template structure and validation (10 tests)
- `tests/test_exporter.py` – Export functions and XLSX creation (5 tests)

**Run tests:**
```bash
uv run pytest tests/test_templates.py tests/test_exporter.py -v
```

**Test results:** ✅ All 15 tests passing

**Test coverage:**
- Template structure and column validation
- DataFrame validation with missing required columns
- Template registry access
- XLSX file creation and formatting
- Error handling for missing data
- Data quality checks (null values, column counts)

**Real data validation:**
- Tested on 50 sample products extracted from 356K+ actual sales records
- Successfully merged 4 data sources (product_info, inventory, price_sale, enrichment)
- Generated valid Products.xlsx with proper formatting
- Verified all required columns and data types

## Usage in Orchestrator

Future integration in orchestrator (`src/pipeline/orchestrator.py`):

```python
from src.erp import export_products_xlsx

# After extract_products() completes:
output_path, stats = export_products_xlsx(
    product_info_path=validated_dir / "product_info.csv",
    inventory_path=validated_dir / "inventory.csv",
    price_sale_path=validated_dir / "price_sale.csv",
    enrichment_path=staging_dir / "enrichment.csv",
    output_path=export_dir / "Products.xlsx",
)
logger.info(f"Exported {stats['products_exported']} products to {output_path}")
```

## Future Enhancements

1. **Supplier export** – `export_suppliers_xlsx()`
2. **Advanced enrichment** – Multi-source joins (Google Sheets, external databases)
3. **Batch operations** – Export multiple templates in parallel
4. **Error recovery** – Rollback on validation failure
5. **Audit logging** – Track all exported rows with lineage
