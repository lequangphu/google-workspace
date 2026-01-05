# Tire Shop ERP Migration - AI Agent Knowledge Base

**Generated**: January 4, 2026
**Project**: Data pipeline for migrating Google Sheets → KiotViet ERP

## OVERVIEW
Python data pipeline ingesting business data from Google Sheets, transforming by raw data source, validating against ERP templates, and exporting to XLSX files.

## STRUCTURE
```
./
├── src/
│   ├── modules/          # Raw data source modules (import_export_receipts, receivable, payable)
│   ├── erp/              # Template definitions & XLSX export functions
│   └── pipeline/         # Orchestrator with CLI for running full pipeline
├── tests/                # Real-data integration tests (no mocks)
├── legacy/               # Deprecated scripts (reference only)
├── data/                 # 4-stage data flow: 00-raw → 01-staging → 02-validated → 03-erp-export
├── docs/                 # ADRs, I/O mapping, refactoring roadmap
└── pipeline.toml         # Central config (paths, IDs, sources, migration filters)
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Run full pipeline | `src/pipeline/orchestrator.py` | CLI: `uv run src/pipeline/orchestrator.py [--step transform] [--period 2025_01]` |
| Ingest from Drive | `src/modules/ingest.py` | Uses `src/modules/google_api.py` for auth & manifest caching |
| Products/PriceBook | `src/modules/import_export_receipts/` | Complex FIFO costing, Google Sheets enrichment |
| Customers | `src/modules/receivable/generate_customers_xlsx_v2.py` | Phone cleaning, debt aggregation |
| Suppliers | `src/modules/payable/generate_suppliers_xlsx.py` | Phone cleaning, supplier master |
| ERP templates | `src/erp/templates.py` | KiotViet 27-column template definitions |
| XLSX export | `src/erp/exporter.py` | Validates before export (never write directly) |
| Test patterns | `tests/` | Test on real CSVs from `data/00-raw/` only |

## CONVENTIONS (Deviation from Standard)

### Module Organization
- Group by **raw data source** (import_export_receipts, receivable, payable) NOT by processing phase
- Each `.py` file **< 300 lines** (strong safety preference)
- Test file per module: `tests/test_<raw_source>_<script>.py`

### Naming
| What | Pattern | Example |
|------|---------|---------|
| Functions & files | snake_case | `clean_receipts_purchase.py`, `extract_products()` |
| Classes | PascalCase | `ERPTemplate`, `DataValidator` |
| KiotViet columns | Exact Vietnamese, case-sensitive | "Mã hàng", "Tên hàng", "Nhóm hàng(3 Cấp)" |
| Config keys | snake_case | `erp_export`, `bundle_modules` |

### Tooling
- ONLY `uv` commands: `uv run script.py`, `uv run pytest`, `uv add package_name`, `uv sync`
- FORBIDDEN: `python`, `pip`, `poetry` (all variants)

### Git Workflow
- Branch: `feature/<description>`, `bugfix/<description>`, `refactor/<description>`, `docs/<description>`
- Commit: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, `chore:`
- Pre-commit: `uv run pytest tests/ -v`, `uv run ruff format src/ tests/`, `uv run ruff check src/`

### Code Patterns
- Transformations: Validate before export using `ERPTemplateRegistry.validate_dataframe()`
- Paths: Always use `pathlib.Path` (not `os.path` or strings)
- Logging: Major steps with `logger.info("=" * 70)` separator

## ANTI-PATTERNS (This Project)

### Data Handling
- ❌ Hardcode paths/IDs/periods → only from `pipeline.toml`
- ❌ Skip row tracking
- ❌ Write directly to `data/03-erp-export/` → always: staging → validate → promote
- ❌ Test on mock data → **only real CSV files from `/data/00-raw/`**
- ❌ Silently skip errors → always log reason
- ❌ Migrate script without testing on raw data → **always run on real data before commit**

### File Management
- ❌ Create new `.md` files (except in `docs/`)

### Architecture
- ❌ Group modules by processing phase
- ❌ Use `python`, `pip`, `poetry` commands

## UNIQUE STYLES

### Pipeline Flow
4-stage invariant: **Ingest (Google Drive) → Transform (by raw source) → Validate → Export XLSX**
- `data/00-raw/` - Downloaded CSVs
- `data/01-staging/` - Versioned transforms
- `data/02-validated/` - Ready for export
- `data/03-erp-export/` - Final XLSX only

### External Enrichment
Product lookup from Google Sheets (Nhóm hàng, Thương hiệu):
- Spreadsheet ID: `16bGN2gjWspCqlFD4xB--7WtkYtTpDaWzRQx9sV97ed8`
- Used by: `extract_products.py`

### Legacy Migration
100% complete. All legacy scripts in `./legacy/` deprecated, migrated to `src/modules/`. See `docs/refactoring-roadmap.md`.

## COMMANDS
```bash
# Development
uv sync                              # Install dependencies
uv run src/pipeline/orchestrator.py  # Run full pipeline
uv run pytest tests/ -v              # Run all tests
uv run pytest tests/test_<module>_*.py -v  # One module

# Quality (pre-commit)
uv run ruff format src/ tests/       # Format code
uv run ruff check src/               # Lint code

# Module examples
uv run src/modules/ingest.py         # Ingest only
uv run src/modules/import_export_receipts/clean_inventory.py  # Clean inventory
```

## NOTES

### Gotchas
- AGENTS.md references `src/cli.py` which **doesn't exist** - entry point is `src/pipeline/orchestrator.py`
- `orchestrator.py` (996 lines) violates 300-line limit - contains CLI, pipeline logic, and Google Drive integration
- `extract_products.py` (1370 lines) deprecated but not removed - see `generate_products_xlsx.py`
- Legacy scripts can write directly to `data/03-erp-export/` (violates staging pattern)
- Sensitive files (credentials.json, token.json) never committed
- Configuration partially hardcoded in `orchestrator.py` - violates ADR-1 (config-driven)

### Complex Modules
- `src/modules/import_export_receipts/` - Multi-month consolidation, FIFO costing, reconciliation reporting
- `src/pipeline/orchestrator.py` - Central controller with fallback to legacy scripts
- `src/modules/google_api.py` - Manifest caching, request throttling, exponential backoff

### Testing Evidence
Every PR requires:
- All tests green on real data from `data/00-raw/`
- Lint & type check pass
- Diff confined to agreed paths
