---
globs:
  - 'src/**/*.py'
  - 'tests/**/*.py'
  - 'pipeline.toml'
---

# Architecture Decision Records (ADRs)

This document explains the "why" behind key architectural decisions.

## ADR-1: Configuration-Driven Pipeline (vs Hardcoded)

**Decision**: All file paths, API IDs, feature flags → `pipeline.toml`, NOT hardcoded in code.

**Rationale**:
- Enables non-developers to adjust periods, paths without touching code
- Prevents accidental commits of sensitive IDs or local paths
- Makes it easy to run with different configurations (e.g., staging vs production)

**Example**:
```toml
[pipeline]
steps = ["ingest", "transform", "export"]

[dirs]
raw = "data/00-raw"
staging = "data/01-staging"
validated = "data/02-validated"
erp_export = "data/03-erp-export"

[export]
bundle_modules = true
period = "2025_01"
```

**Anti-pattern** (hardcoded):
```python
RAW_DIR = "data/raw"  # ❌
SHEETS_ID = "1KYz8S4WSL5vG2TIYsZKKIwNulKLvMc82iBMso_u49dk"  # ❌
```

---

## ADR-2: Staging Pattern (vs Direct Production)

**Decision**: Transform to staging first, validate, then promote to export.

**Rationale**:
- Safe re-runs: if validation fails, doesn't corrupt final export directory
- Enables rollback if issues discovered post-transformation
- Versioning: keep timestamps in staging filenames for audit trail
- Separation of concerns: transform, validate, and export are decoupled

**Flow**:
```
data/00-raw/           ← Downloaded CSVs
    ↓
data/01-staging/       ← Versioned staging (timestamp in filename)
    ↓
data/02-validated/     ← After validation (ready for export)
    ↓
data/03-erp-export/    ← Final, validated outputs only
```

**Key rule**: Never directly write to `03-erp-export/`. Always validate first.

---

## ADR-3: Raw Source Grouping (vs Processing Phase)

**Decision**: Group modules by raw data source (not processing phase).

**Rationale**:
- **AI context efficiency**: When fixing customer ID logic, agent loads only receivable/, not products/
- **Modularity**: related tasks (clean + extract) naturally belong together
- **Maintainability**: easier to understand each module's responsibility

**Example** (sheet names and ingest structure):
```
ingest.py configuration:
- import_export_receipts: Sheets CT.NHAP, CT.XUAT, XNT (multiple year/month files)
- receivable: Sheets TỔNG CÔNG NỢ, Thong tin KH → receivable_summary.csv, receivable_customers.csv
- payable: Sheets MÃ CTY, TỔNG HỢP → payable_master.csv, payable_summary.csv
- cashflow: Sheets Tiền gửi, Tien mat → cashflow_deposits.csv, cashflow_cash.csv

src/modules/
 ├── import_export_receipts/  # Raw source: XUẤT NHẬP TỒN TỔNG T* (multiple sheets)
 │   ├── clean_receipts_purchase.py
 │   ├── clean_receipts_sale.py
 │   ├── clean_inventory.py
 │   ├── extract_products.py
 │   ├── extract_attributes.py
 │   ├── verify_disambiguation.py
 │   ├── upload_cleaned_to_sheets.py
 │   └── generate_products_xlsx.py
├── receivable/              # Raw source: TỔNG CÔNG NỢ + Thong tin KH
│   └── generate_customers_xlsx.py
├── payable/                 # Raw source: MÃ CTY + TỔNG HỢP
│   └── generate_suppliers_xlsx.py
└── cashflow/                # Raw source: Tiền gửi + Tien mat (future)
    └── (future)
```

**Anti-pattern** (by phase):
```
src/
├── cleaning/           # ❌ mixes unrelated data sources
│   ├── receipts.py
│   ├── customers.py
│   └── suppliers.py
├── extraction/         # ❌ scattered responsibility
│   ├── products.py
│   ├── customer_ids.py
│   └── suppliers.py
```

---

## ADR-4: mtime-Based Change Detection (vs Content-Hash)

**Decision**: Use modification time (mtime) from Google Drive to detect file changes, not content hashing.

**Rationale**:
- **Simplicity**: No need to download + hash locally; faster ingestion
- **API efficiency**: Compare remote mtime vs local mtime without extra API calls
- **Practicality**: In practice, files are re-downloaded with updated timestamps
- **Current month re-ingest**: Always re-ingest current month to catch intraday edits

**Implementation**:
```python
# In ingest.py, use should_ingest_file() and should_ingest_import_export()
if should_ingest_import_export(csv_path, remote_modified_time, current_month, current_year):
    export_tab_to_csv(sheets_service, file_id, tab, csv_path)
```

**Trade-off**: If files are corrected retroactively without timestamp update, we won't detect the change. Acceptable risk.

---

## ADR-6: API Request Optimization (Priority Order)

**Decision**: Prioritize manifest cache → request throttling → retry when accessing Google APIs.

**Rationale**:
- **Quota management**: Google Sheets API has ~300 QPS limit; manifest caching avoids redundant folder scans
- **Cost**: Fewer requests = faster execution + lower quota consumption
- **Reliability**: Proper backoff prevents rate-limit errors
- **Simplicity**: Avoid over-engineering with parallelization until needed

**Implemented techniques** (in priority order):
1. **Manifest cache**: Cache folder→sheets metadata for 24h (saves ~90% of folder scan API calls)
2. **Request throttling**: `time.sleep(0.5s)` after each API call (stay under 60 QPS)
3. **Exponential backoff**: 3x retry on 429 rate-limit errors (1s, 2s, 4s)
4. **mtime-based skipping**: Only re-download if remote file newer than local

**Not implemented** (reserved for future if needed):
- Batch operations: `batchUpdate` not cell-by-cell writes
- Parallelization: ThreadPoolExecutor with max_workers=5 (Drive API safe limit)

```python
# Example: manifest cache usage in ingest.py (lines 191-200)
# Functions implemented in google_api.py (lines 34-123)
cached_sheets, is_fresh = get_cached_sheets_for_folder(manifest, folder_id)
if cached_sheets is not None:
    sheets = cached_sheets
    api_calls_saved += 1
else:
    sheets = find_sheets_in_folder(drive_service, folder_id)
    update_manifest_for_folder(manifest, folder_id, sheets)
```

---

## ADR-7: Validation Before Export

**Decision**: Validate data against ERP template before writing XLSX.

**Rationale**:
- **Early failure**: Catch issues before they reach production
- **Clear feedback**: Know exactly which fields/rows are invalid
- **Compliance**: KiotViet has strict column requirements; validation prevents import errors

**Implementation**:
```python
from src.erp.templates import ERPTemplateRegistry

templates = ERPTemplateRegistry()
template = templates.get_template("PRODUCT")
is_valid, errors = template.validate_dataframe(products_df)

if not is_valid:
    for error in errors:
        logger.error(f"Validation error: {error}")
    raise ValueError("Data validation failed for PRODUCT")
```

---

## ADR-8: uv as Exclusive Python Tool

**Decision**: Use only `uv` for all Python commands (run, add, sync, etc.).

**Rationale**:
- **Determinism**: uv.lock ensures reproducible builds across machines
- **Speed**: uv is ~10–20x faster than pip for dependency resolution
- **Clarity**: Single tool reduces confusion (no mix of pip/poetry/poetry)
- **Future-proofing**: Part of Python packaging ecosystem modernization

**Never use**:
- `python script.py` → use `uv run script.py`
- `pip install X` → use `uv add X`
- `poetry run pytest` → use `uv run pytest`

---

## ADR-9: Small Files (<300 lines) for AI Agent Context

**Decision**: Keep each `.py` file under 300 lines.

**Rationale**:
- **Context windows**: ~120k tokens can fit ~300–400 lines of code with documentation
- **AI efficiency**: Agent loads only the file it needs, not unrelated code
- **Maintainability**: Smaller modules are easier to reason about
- **Modularity**: Forces single-responsibility principle

**Exception**: Test files or orchestrators may exceed 300 lines if necessary, but transformation scripts must stay <300 lines.

---

## ADR-10: XLSX Export (vs Google Sheets Upload)

**Decision**: Generate XLSX files locally; do not upload directly to Google Sheets.

**Rationale**:
- **Control**: Sheets API cell-by-cell writing is slow; XLSX is 100x faster
- **Offline safety**: XLSX files can be validated/audited before upload
- **Versioning**: Keep timestamped XLSX files in `data/03-erp-export/` for audit trail
- **Tooling**: ERP systems expect XLSX format, not Google Sheets

**Flow**:
```
Transform → Validate → XLSX (data/03-erp-export/) → [Manual upload to ERP]
```

---

## ADR-11: Exact Vietnamese Column Names

**Decision**: Use exact Vietnamese names from KiotViet templates (case-sensitive).

**Rationale**:
- **Compliance**: KiotViet import expects exact column names; mismatch causes import errors
- **Clarity**: Code reflects real ERP column names (no "customer_name" → must be "Tên khách hàng")
- **Audit trail**: Easy to cross-reference code with official KiotViet templates

**Examples**:
- ✅ "Mã hàng" (not "product_code" or "Mã sản phẩm")
- ✅ "Tên khách hàng" (not "customer_name" or "Tên KH")
- ✅ "Nhóm hàng(3 Cấp)" (exact formatting with parentheses)

See `docs/erp-mapping.md` for complete column reference.
