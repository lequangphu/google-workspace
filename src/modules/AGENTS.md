# src/modules - Raw Data Source Modules

## OVERVIEW
Organizes data transformation logic by raw data source (import_export_receipts, receivable, payable), not by processing phase.

## STRUCTURE
```
src/modules/
├── import_export_receipts/   # Products, PriceBook, inventory reconciliation
├── receivable/               # Customer master data, debt aggregation
├── payable/                  # Supplier master data
├── google_api.py             # Google Drive/Sheets API wrapper with manifest caching
├── ingest.py                 # Drive data ingestion (uses google_api.py)
└── __init__.py
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Google API auth & caching | `google_api.py` | Manifest caching, request throttling, exponential backoff |
| Ingest from Drive | `ingest.py` | Downloads CSVs to `data/00-raw/` |
| Products/PriceBook | `import_export_receipts/` | See `import_export_receipts/AGENTS.md` |
| Customers | `receivable/generate_customers_xlsx_v2.py` | Phone cleaning, debt aggregation |
| Suppliers | `payable/generate_suppliers_xlsx.py` | Phone cleaning, supplier master |

## CONVENTIONS

### Module Organization
- Group by **raw data source** (import_export_receipts / receivable / payable)
- NOT by processing phase (no separate "transform", "export" directories)
- Each `.py` file **< 300 lines** (strong safety preference)

### Naming Patterns
| What | Pattern | Example |
|------|---------|---------|
| Module directories | snake_case | `import_export_receipts`, `receivable`, `payable` |
| Module files | `action_entity.py` | `clean_inventory.py`, `generate_customers_xlsx_v2.py` |
| Functions | snake_case | `process()`, `calculate_fifo_cost()`, `extract_attributes()` |

### Code Patterns
- Use `pathlib.Path` (not `os.path` or strings)
- Load configuration from `pipeline.toml` via `load_pipeline_config()`
- Test file per module: `tests/test_<raw_source>_<script>.py`
- Reconciliation reports after every `*_clean_*.py` script

## ANTI-PATTERNS (Module-Specific)

### ❌ Processing Phase Organization
- **Don't**: Create directories like `transform/`, `export/`, `validate/`
- **Do**: Group by data source within `import_export_receipts/`, `receivable/`, `payable/`

### ❌ File Size Violations
- **Don't**: Create `.py` files >300 lines
- **Do**: Split large files into focused functions or helper modules
- **Exception**: `orchestrator.py` (996 lines) - violates but documented

### ❌ Direct Google API Calls
- **Don't**: Call Google Drive/Sheets API directly from modules
- **Do**: Use `google_api.py` wrappers (manifest caching, throttling, backoff)

### ❌ Duplicate Utilities
- **Don't**: Implement phone cleaning, data validation in multiple places
- **Do**: Extract shared utilities to common files or `google_api.py` if applicable

### ❌ Mixing Data Sources
- **Don't**: Put receivable logic in `import_export_receipts/` or vice versa
- **Do**: Keep modules isolated by raw data source
