# AGENTS.md – Critical Rules for AI Coding Agents

**Updated**: December 2025  
**Context**: ~120k tokens → use economically. Load only files you need.

## 1. Absolute Rules – Failure if broken

### Tooling – ONLY these commands

```bash
uv run script.py              # ✅ CORRECT
uv run pytest tests/          # ✅ CORRECT
uv add package_name          # ✅ CORRECT
uv sync                       # ✅ CORRECT

python, pip, poetry           # ❌ FORBIDDEN
pip install, poetry add/run   # ❌ FORBIDDEN
```

### Module organization – mandatory

- Group by **raw data source** (never by processing phase)
- Raw sources: `import_export_receipts` / `receivable` / `payable` / `cashflow`
- Each `.py` file **< 300 lines** (strong safety preference)
- Test file per module: `tests/test_<raw_source>_<script>.py`

### Naming – must follow

| What | Pattern | Example |
|------|---------|---------|
| Functions & files | snake_case | `clean_receipts_purchase.py`, `extract_products()` |
| Classes | PascalCase | `ERPTemplate`, `DataValidator` |
| KiotViet columns | Exact Vietnamese, case-sensitive | "Mã hàng", "Tên hàng", "Nhóm hàng(3 Cấp)" |
| Config keys | snake_case | `erp_export`, `bundle_modules` |

### Dangerous mistakes – NEVER do

- ❌ Hardcode paths/IDs/periods → only from `pipeline.toml`
- ❌ Skip row tracking → **every row must have lineage entry** (success or rejection)
- ❌ Write directly to `data/03-erp-export/` → always: staging → validate → promote
- ❌ Test on mock data → **only real CSV files from `/data/00-raw/`**
- ❌ Create new `.md` files (except in `docs/`)
- ❌ Silently skip errors → always log reason
- ❌ Migrate script without testing on raw data → **always run migrated script on real data before commit**

### File creation rules

- **Allowed**: Update AGENTS.md, create files in `docs/`, add/modify src/tests files
- **Not allowed**: Create `.md` files outside `docs/`; this keeps knowledge modular

## 2. Quick Reminders (most frequent)

**Transformations**:
- Use `DataLineage` class to track every row
- Validate before export: `ERPTemplateRegistry.validate_dataframe()`
- Use `pathlib.Path` (not strings)

**Logging**:
- Log at every major step with separator: `logger.info("=" * 70)`
- Never silently fail

**Testing**:
- Before commit: `uv run pytest tests/ -v`
- Always test on real data from `/data/00-raw/`

## 3. Architecture Overview (ultra-brief)

**Pipeline flow**: Ingest (Google Drive) → Transform (by raw source) → Validate → Export XLSX

**Module structure**:
```
src/modules/
├── import_export_receipts/   # Products, PriceBook
├── receivable/               # Customers
├── payable/                  # Suppliers
└── cashflow/                 # Reporting (future)
```

**Data folders**:
- `data/00-raw/` ← Downloaded CSVs
- `data/01-staging/` ← Versioned transforms
- `data/02-validated/` ← Ready for export
- `data/03-erp-export/` ← Final XLSX only

## 4. Recommended Reading Order

1. **This file** (AGENTS.md) for rules
2. **project-description.md** for business context + raw sources
3. **docs/erp-mapping.md** for KiotViet column details (when needed)
4. **docs/development-workflow.md** for git/commits (when committing)
5. **docs/refactoring-roadmap.md** for migration status (rarely needed)
6. **docs/architecture-decisions.md** for design rationales (rarely needed)

## 5. Common Tasks (Quick Start)

### Run full pipeline
```bash
uv run src/cli.py
```

### Run tests
```bash
uv run pytest tests/ -v                                      # All
uv run pytest tests/test_import_export_receipts_*.py -v     # One module
```

### Format & lint before commit
```bash
uv run pytest tests/ -v
uv run ruff format src/ tests/
uv run ruff check src/
```

### Refactor legacy script to new module
1. Create `src/modules/<raw_source>/<script>.py`
2. Create matching test file
3. Pass all tests
4. Mark old script as deprecated
5. Commit with clear message (see docs/development-workflow.md)

## 6. External Data Sources (critical)

**Product lookup** (enrichment):
- URL: https://docs.google.com/spreadsheets/d/16bGN2gjWspCqlFD4xB--7WtkYtTpDaWzRQx9sV97ed8/edit?gid=23224859
- Spreadsheet ID: `16bGN2gjWspCqlFD4xB--7WtkYtTpDaWzRQx9sV97ed8`
- Contains: Nhóm hàng, Thương hiệu
- Used by: `extract_products.py`

**Data folder structure** (ingest → transform → validate → export):
- See `data/README.md` for staging pattern + folder organization
- Never write directly to `data/03-erp-export/`
- Use `DataLineage` class to track every row through pipeline

---

**For detailed reference** (KiotViet columns, git workflow, ADRs), see `docs/` folder.
