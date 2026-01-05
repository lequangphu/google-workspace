# Tire Shop ERP Migration

Data pipeline for migrating business data from Google Sheets to KiotViet ERP.

## Quick Start

```bash
# Install dependencies
uv sync

# Run full pipeline
uv run src/pipeline/orchestrator.py

# Ingest only
uv run src/modules/ingest.py

# Transform only (skip ingestion)
uv run src/pipeline/orchestrator.py --step transform

# With specific period
uv run src/pipeline/orchestrator.py --period 2025_01
```

## Architecture

**Pipeline stages**: `ingest` → `transform` → `validate` → `export`

**Data directories**:
- `data/00-raw/` - Raw CSVs from Google Drive
- `data/01-staging/` - Cleaned/transformed data
- `data/02-validated/` - Master data extracted
- `data/03-erp-export/` - KiotViet XLSX files

## Documentation

| Document | Purpose |
|----------|---------|
| `AGENTS.md` | Development guidelines for AI agents |
| `docs/architecture-decisions.md` | ADR records for key design choices |
| `docs/development-workflow.md` | Git workflow, commits, code style |
| `docs/pipeline-io.md` | Complete I/O mapping for all scripts |
| `docs/refactoring-roadmap.md` | Legacy migration status |

## Raw Sources

| Source | Description | Output |
|--------|-------------|--------|
| `import_export_receipts` | Purchase (CT.NHAP), Sale (CT.XUAT), Inventory (XNT) | Products, PriceBook |
| `receivable` | Customer debts, customer info | Customers |
| `payable` | Supplier master, debt summary | Suppliers |
| `cashflow` | Bank deposits, cash transactions | (reporting only) |
