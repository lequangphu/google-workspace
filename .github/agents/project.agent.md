# AI Agent Instructions – ERP Data Migration System

**Project**: Google Workspace → KiotViet ERP Data Migration  
**Tech Stack**: Python 3.10+, pandas, openpyxl, uv, Google APIs  
**Agent Context**: ~120k tokens (use economically)

## Your Role

You are an AI assistant helping refactor and extend a tire distribution ERP data migration pipeline. The system ingests data from Google Drive, transforms it to ERP-ready format, and exports XLSX files for import into KiotViet.

**When working on this project**:
1. Read `AGENTS.md` first (rules that prevent costly mistakes)
2. Load only the specific files you need (not the entire codebase)
3. Prefer small, focused commits with clear messages
4. Always run `uv run pytest tests/ -v` before committing

## Quick Navigation

| Need to... | See... | Why |
|---|---|---|
| Remember strict rules | `AGENTS.md` | Critical DOs/DON'Ts, prevent mistakes |
| Understand project scope | `project-description.md` | Business context, raw sources |
| Map KiotViet columns | `docs/erp-mapping.md` | 27+ column reference tables |
| Handle git/commits | `docs/development-workflow.md` | Conventional commits, branch strategy |
| Track refactoring status | `docs/refactoring-roadmap.md` | Legacy → new module migration |
| Understand "why" decisions | `docs/architecture-decisions.md` | Rationale behind design choices |

## Key Constraints (memorize these)

### 🚨 Forbidden

- ❌ `python`, `pip`, `poetry` → **only `uv`**
- ❌ Hardcoded paths/IDs → **only from `pipeline.toml`**
- ❌ Skipping row tracking → **every row must have lineage**
- ❌ Direct writes to `data/03-erp-export/` → **always: staging → validate → promote**
- ❌ Creating `.md` files outside `docs/` → **keeps knowledge modular**

### ✅ Must Do

- ✅ Use `DataLineage` for every transformation
- ✅ Validate before export with `ERPTemplateRegistry`
- ✅ Test on real CSVs from `data/00-raw/`
- ✅ Keep each `.py` file < 300 lines
- ✅ Log at every major step with `logger.info("=" * 70)`
- ✅ Run `uv run pytest tests/ -v` before each commit

## Project Structure (60-second overview)

```
src/
├── modules/              # Grouped by RAW DATA SOURCE (not processing phase!)
│   ├── import_export_receipts/   # XUẤT NHẬP TỒN → Products, PriceBook
│   ├── receivable/               # CONG NO → Customers
│   ├── payable/                  # CÔNG NỢ NCC → Suppliers
│   └── cashflow/                 # SỔ QUỸ → Reporting
├── pipeline/             # Orchestrator (ingest → transform → export)
├── erp/                  # KiotViet templates & validation
└── services/             # Google Drive API, caching

data/
├── 00-raw/              # Downloaded CSVs
├── 01-staging/          # Versioned transforms (timestamp)
├── 02-validated/        # After validation (ready for export)
└── 03-erp-export/       # Final XLSX only
```

## Typical Workflow

### 1. Refactoring a Legacy Script to New Module

```bash
# 1. Create new module with tests (e.g., migrate clean_chung_tu_nhap.py)
#    src/modules/import_export_receipts/clean_receipts_purchase.py
#    tests/test_import_export_receipts_clean_receipts_purchase.py

# 2. Implement + pass all tests
uv run pytest tests/test_import_export_receipts_clean_receipts_purchase.py -v

# 3. Run full suite
uv run pytest tests/ -v

# 4. Format & lint
uv run ruff format src/ tests/
uv run ruff check src/

# 5. Commit with clear message (see docs/development-workflow.md)
git commit -m "refactor(import_export_receipts): migrate clean_chung_tu_nhap.py to clean_receipts_purchase.py

- Consolidate legacy cleaning scripts into raw-source-based modules
- Maintain 100% backward compatibility with output
- Add 15+ tests for happy path and edge cases
- Legacy script remains in legacy/ folder (deprecated)

Related: #15"
```

### 2. Adding a New Transformation

1. **Create script** in `src/modules/<raw_source>/<script_name>.py` (< 300 lines)
2. **Add test file** at `tests/test_<raw_source>_<script_name>.py`
3. **Implement** with:
   - Type hints on all functions
   - Google-format docstrings
   - `DataLineage` tracking (every row!)
   - `logger.info("=" * 70)` at major steps
4. **Validate** before export using `ERPTemplateRegistry`
5. **Test** on real data from `data/00-raw/`
6. **Run full suite**: `uv run pytest tests/ -v`
7. **Commit** with conventional message

### 3. Troubleshooting

- **Import error?** → Check `uv sync` has been run
- **Test failure?** → Ensure you're using real CSVs from `/data/00-raw/`, not mock data
- **Column mismatch?** → Verify exact Vietnamese name in `docs/erp-mapping.md`
- **Row missing from output?** → Check `DataLineage` logs for rejection reason
- **API quota hit?** → Use `HashCache` + batch operations (see `docs/architecture-decisions.md`)

## When to Read Which Doc

| Situation | Read | Time |
|---|---|---|
| "What are the rules?" | `AGENTS.md` | 5 min |
| "What is this project about?" | `project-description.md` | 10 min |
| "How do I refactor a script?" | `docs/development-workflow.md` | 5 min |
| "Why staging + validation pattern?" | `docs/architecture-decisions.md` | 10 min |
| "What are the exact KiotViet columns?" | `docs/erp-mapping.md` | (reference only) |
| "Which scripts still need refactoring?" | `docs/refactoring-roadmap.md` | 2 min |

## External Data Sources (Don't Forget!)

**Product enrichment** (used by `extract_products.py`):
- URL: https://docs.google.com/spreadsheets/d/16bGN2gjWspCqlFD4xB--7WtkYtTpDaWzRQx9sV97ed8/edit?gid=23224859
- Spreadsheet ID: `16bGN2gjWspCqlFD4xB--7WtkYtTpDaWzRQx9sV97ed8`
- Contains: Nhóm hàng, Thương hiệu (Product Group, Brand)

This is critical for enriching extracted product data. Always check if your code needs to call this sheet.

## Anti-patterns (Don't do these)

❌ **Skip logging** – Use `logger.info()` at major steps  
❌ **Silently fail on rows** – Track every row (success OR rejection reason)  
❌ **Test on fake data** – Use real CSVs from `/data/00-raw/`  
❌ **Combine scripts** – Keep each file < 300 lines, separate by raw source  
❌ **Create markdown files** – Keep them in `docs/` only  
❌ **Use python/pip directly** – Always `uv run`  

---

**Last updated**: December 2025  
**Context efficiency**: This file + AGENTS.md (1,800 tokens total) replaces the old 3,500-token AGENTS.md
