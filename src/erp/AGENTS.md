# src/erp/ - ERP Integration Module

## OVERVIEW
Core ERP layer: 4 templates (27/20/15/5 columns), validation, export functions.

## STRUCTURE
```
src/erp/
├── __init__.py         # Exports templates and export functions
├── templates.py        # 4 templates, ERPTemplateRegistry, ColumnSpec
└── exporter.py         # export_products_xlsx(), export_customers_xlsx(), export_pricebook_xlsx()
```

## WHERE TO LOOK
| Task | Location | Key |
|------|----------|-----|
| Product template (27 cols) | `templates.py:34` | `ProductTemplate.COLUMNS` |
| Customer template (20 cols) | `templates.py:148` | `CustomerTemplate.COLUMNS` |
| Validate DataFrame | `templates.py:290` | `ERPTemplateRegistry.validate_dataframe()` |
| Export products | `exporter.py:135` | `export_products_xlsx()` |

## CONVENTIONS

### KiotViet Column Names (Exact Vietnamese Required)
```
"Mã hàng", "Tên hàng", "Nhóm hàng(3 Cấp)", "Giá bán", "Giá vốn", "Đang kinh doanh"
```
ColumnSpec: `name` (exact VN), `column_index` (0-based), `data_type` (`text`/`number`/`date`), `format_code`, `required`

### Export Pattern
```python
# Build DataFrame, validate, then export
template = ProductTemplate()
is_valid, errors = template.validate_dataframe(df)
if not is_valid:
    raise ValueError(f"Validation failed: {errors}")
df.to_excel(output_path, index=False, engine="openpyxl")
_format_product_xlsx(output_path, template)
```

## ANTI-PATTERNS
- ❌ Skip validation → ALWAYS validate before `to_excel()`
- ❌ English column names → MUST use exact Vietnamese strings
- ❌ Write directly to export dir → create parent dirs first
- ❌ Hardcode column names → use `template.COLUMNS` or `get_column_names()`
