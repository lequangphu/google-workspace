# src/ - Source Code

## OVERVIEW
Python modules implementing 4-stage pipeline (ingest → transform → validate → export) organized by raw data source.

## STRUCTURE

```
src/
├── modules/          # Raw source processors (import_export, receivable, payable)
│   ├── import_export_receipts/   # CT.NHAP, CT.XUAT, XNT → Products, PriceBook
│   ├── receivable/               # Công nợ → Customers
│   ├── payable/                 # Công nợ NCC → Suppliers
│   ├── google_api.py            # Drive/Sheets API with manifest caching
│   └── ingest.py                # Google Sheets → data/00-raw/
├── erp/                          # KiotViet template specs and XLSX export
│   ├── templates.py             # 27-column template definitions (Vietnamese)
│   └── exporter.py              # Validates then writes XLSX (never direct)
└── pipeline/
    └── orchestrator.py          # CLI entry point, module orchestration
```

## WHERE TO LOOK

| Component | File | Purpose |
|-----------|------|---------|
| **Ingest** | `modules/ingest.py` + `modules/google_api.py` | Download Google Sheets, manifest caching |
| **Products** | `import_export_receipts/extract_products.py` | Google Sheets enrichment, FIFO costing |
| **Products XLSX** | `import_export_receipts/generate_products_xlsx.py` | Validated → KiotViet Products template |
| **PriceBook** | `import_export_receipts/generate_products_xlsx.py` | Exports price list from products |
| **Customers** | `receivable/generate_customers_xlsx_v2.py` | Phone cleaning, debt aggregation |
| **Suppliers** | `payable/generate_suppliers_xlsx.py` | Supplier master, phone cleaning |
| **Inventory** | `import_export_receipts/clean_inventory.py` | Multi-month consolidation, column filtering |
| **Templates** | `erp/templates.py` | Column specs, data types, validation rules |
| **Export** | `erp/exporter.py` | Validates before XLSX write |

## CONVENTIONS

**Module Organization**: Group by raw data source (`import_export_receipts`, `receivable`, `payable`) NOT by processing phase. Each source has dedicated subdirectory with all related transforms.

**Module Size**: Strong preference for files < 300 lines. Split complex modules (e.g., `extract_products.py`) into smaller focused scripts.

**Configuration Pattern**: Each module defines `CONFIG` dict at module level (loaded once at import), with values from `pipeline.toml`.

```python
_CONFIG = load_pipeline_config()
CONFIG = {
    "file_pattern": r"(\d{4})_(\d{1,2})_XNT\.csv",
    "min_non_null_percentage": 90,
}
```

**Path Handling**: Always use `pathlib.Path` (never `os.path` or strings).

**Naming**: Vietnamese column names exact, case-sensitive (`"Mã hàng"`, `"Tên hàng"`, `"Nhóm hàng(3 Cấp)"`).

**Export Rule**: Never write directly to `data/03-erp-export/`. Always: staging → validate → promote.
