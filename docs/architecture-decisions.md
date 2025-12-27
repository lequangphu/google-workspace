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
- **Visible data lineage**: clear what source each transformation comes from
- **Modularity**: related tasks (clean + extract) naturally belong together
- **Maintainability**: easier to understand each module's responsibility

**Example**:
```
src/modules/
├── import_export_receipts/  # Raw source: XUẤT NHẬP TỒN TỔNG T*
│   ├── clean_receipts_purchase.py
│   ├── clean_receipts_sale.py
│   ├── clean_inventory.py
│   └── extract_products.py
├── receivable/              # Raw source: CONG NO HANG NGAY
│   ├── clean_customers.py
│   └── extract_customer_ids.py
├── payable/                 # Raw source: BC CÔNG NỢ NCC
│   └── extract_suppliers.py
└── cashflow/                # Raw source: SỔ QUỸ TIỀN MẶT
    └── transform.py
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

## ADR-4: Content-Hash Caching (vs mtime)

**Decision**: Use content-based hashing (HashCache) instead of modification time for detecting changes.

**Rationale**:
- **Robustness**: Handles files re-downloaded with same timestamp
- **Correctness**: Detects actual content changes, not just date changes
- **Idempotency**: If file content unchanged, skip re-processing safely
- **API quota**: Avoids unnecessary Drive API reads

**Implementation**:
```python
from src.services.cache import HashCache

cache = HashCache(cache_file="data/.cache/hash_cache.json")
if not cache.is_modified(file_path):
    logger.info(f"Skipping {file_path} (content hash match)")
    continue
```

---

## ADR-5: Data Lineage Tracking (Mandatory)

**Decision**: Every transformation must track source → output mapping for audit trail.

**Rationale**:
- **Auditability**: Can trace any output row back to source row
- **Error diagnosis**: Know exactly which source rows were rejected and why
- **Compliance**: Required for ERP migrations (need audit trail for accountants)
- **Debugging**: Identify which rows caused downstream issues

**Implementation**:
```python
from src.modules.lineage import DataLineage

lineage = DataLineage(output_dir)

for idx, row in df.iterrows():
    try:
        result = clean_purchase_receipts(row)
        lineage.track(
            source_file=file.name,
            source_row=idx,
            output_row=len(output),
            operation="clean_receipts_purchase",
            status="success"
        )
    except ValueError as e:
        lineage.track(
            source_file=file.name,
            source_row=idx,
            output_row=None,
            operation="clean_receipts_purchase",
            status=f"rejected: {e}"
        )
```

**Rule**: No exceptions — every row must be tracked (success or rejection reason).

---

## ADR-6: API Request Optimization (Priority Order)

**Decision**: Prioritize cache → batch ops → parallel → retry when accessing Google APIs.

**Rationale**:
- **Quota management**: Google Sheets API has ~300 QPS limit; batch operations are more efficient
- **Cost**: Fewer requests = faster execution + lower quota consumption
- **Reliability**: Proper backoff prevents rate-limit errors

**Priority order** (fastest to slowest):
1. **Cache**: Use `HashCache` (content-based, not mtime)
2. **Batch ops**: `batchUpdate` not cell-by-cell writes
3. **Parallel**: ThreadPoolExecutor with max_workers=5 (Drive API safe limit)
4. **Retry**: Exponential backoff for rate limits

**Rule**: If reading > 5 Google Sheets, use ThreadPoolExecutor:
```python
from concurrent.futures import ThreadPoolExecutor

client = GoogleSheetsClient(creds, max_workers=5)
results = client.read_multiple_ranges_parallel(
    spreadsheet_id,
    ["Sheet1!A:Z", "Sheet2!A:Z", "Sheet3!A:Z"]
)
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
