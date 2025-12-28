# Refactoring Roadmap – Legacy Script Migration Status

Legacy scripts are being refactored into raw source modules for better organization and AI agent context efficiency.

## Progress Summary

**Completed**: 9 of 9 scripts (100%)
- ✅ `ingest.py` (original migration)
- ✅ `clean_chung_tu_nhap.py` (CT.NHAP)
- ✅ `clean_chung_tu_xuat.py` (CT.XUAT)
- ✅ `clean_xuat_nhap_ton.py` (XNT)
- ✅ `generate_product_info.py` (Product extraction)
- ✅ `clean_thong_tin_khach_hang.py` (Customer data)
- ✅ `generate_new_customer_id.py` (Customer IDs)
- ✅ `clean_tong_no.py` (Debt data)
- ✅ `pipeline.py` (orchestrator)

**Status**: Full migration complete. All tests passing (153 passed, 1 skipped).

## Migration Mapping

| Current Script | Target Module | Target File | Status |
|---|---|---|---|
| `ingest.py` | `src/modules/` | `ingest.py` + `google_api.py` | ✅ **Migrated** (b7a22c2) - Multi-sheet support, rate limiting |
| `clean_chung_tu_nhap.py` | `import_export_receipts` | `clean_receipts_purchase.py` | ✅ **Migrated** (T-019b5f16) - CT.NHAP processing, date parsing, lineage tracking |
| `clean_chung_tu_xuat.py` | `import_export_receipts` | `clean_receipts_sale.py` | ✅ **Migrated** (T-019b5f24) - CT.XUAT processing, validated on 67 files |
| `clean_xuat_nhap_ton.py` | `import_export_receipts` | `clean_inventory.py` | ✅ **Migrated** (T-019b5f24) - XNT processing, 48,272 rows, FIFO-ready |
| `generate_product_info.py` | `import_export_receipts` | `extract_products.py` | ✅ **Migrated** (T-019b5f24) - Product extraction, FIFO costing, price analysis |
| `clean_thong_tin_khach_hang.py` | `receivable` | `clean_customers.py` | ✅ **Migrated** (T-019b5f24) - Customer import, phone number splitting |
| `generate_new_customer_id.py` | `receivable` | `extract_customer_ids.py` | ✅ **Migrated** (T-019b5f31) - Customer ID generation from CT.XUAT, sequential ranking |
| `clean_tong_no.py` | `receivable` | `clean_debts.py` | ✅ **Migrated** (T-019b624e) - Debt info transformation, two-level customer join |
| `pipeline.py` | `src/pipeline/` | `orchestrator.py` | ✅ **Migrated** (T-019b625e) - Pipeline orchestration, ingest → transform → upload |

## Pipeline Flow (ingest → transform by raw source → validate → export)

```
Raw CSV from Google Drive
    ↓
Import/Export Receipts:
  ├── clean_receipts_purchase.py (CT.NHAP) → Products + PriceBook
  ├── clean_receipts_sale.py (CT.XUAT)
  ├── clean_inventory.py (XNT)
  └── extract_products.py
    ↓
Receivable:
  ├── clean_customers.py → Customers
  └── extract_customer_ids.py
    ↓
Payable:
  └── extract_suppliers.py → Suppliers
    ↓
CashFlow:
  └── transform.py (reporting only)
    ↓
ERPTemplateRegistry (validate against KiotViet specs)
    ↓
ExcelExporter (write XLSX files)
    ↓
ERP-ready files (data/03-erp-export/)
```

## Master Data Extraction (Products, Customers, Suppliers)

Master data is extracted from transaction data and enriched with external lookups.

**Raw Source Modules**:
- `src/modules/import_export_receipts/extract_products.py` - Extract from CT.NHAP + CT.XUAT with Google Sheet enrichment
- `src/modules/receivable/clean_customers.py` - Extract from CT.XUAT
- `src/modules/payable/extract_suppliers.py` - Extract from CT.NHAP

**External Data Source for Products** (CRITICAL):
- **Product Lookup**: https://docs.google.com/spreadsheets/d/16bGN2gjWspCqlFD4xB--7WtkYtTpDaWzRQx9sV97ed8/edit?gid=23224859
- **Spreadsheet ID**: `16bGN2gjWspCqlFD4xB--7WtkYtTpDaWzRQx9sV97ed8`
- **Sheet gid**: `23224859`
- **Contains**: `Nhóm hàng` (Product Group), `Thương hiệu` (Brand)
- **Usage**: Enrich extracted product master data with group and brand information

## Test Coverage

**Tests Created**: 153 unit and integration tests (153 passed, 1 skipped)
- ✅ `test_import_export_receipts_clean_receipts_purchase.py`
- ✅ `test_import_export_receipts_clean_receipts_sale.py`
- ✅ `test_import_export_receipts_clean_inventory.py`
- ✅ `test_import_export_receipts_extract_products.py`
- ✅ `test_receivable_clean_customers.py`
- ✅ `test_receivable_extract_customer_ids.py`
- ✅ `test_receivable_clean_debts.py`
- ✅ `test_pipeline_orchestrator.py`

**Real Data Validation**: All modules tested on actual CSV files
- 67 XNT (inventory) files processed → 48,272 rows
- Receipt data validated with full pipeline
- Phone number parsing tested with edge cases

## Key Architecture Patterns

See `docs/architecture-decisions.md` for detailed rationales behind:
- **Staging pattern** (safe re-runs)
- **Configuration-driven pipeline** (vs hardcoded)
- **Content-hash caching** (vs mtime)
- **Raw-source grouping** (vs processing phase grouping)

## Legacy Files Reference

All legacy scripts have been migrated to the modular structure and removed from the root directory. Original implementations are available in git history for reference.

**Migrated scripts** (now in `src/modules/` and `src/pipeline/`):
- Legacy `clean_chung_tu_nhap.py` → `src/modules/import_export_receipts/clean_receipts_purchase.py`
- Legacy `clean_chung_tu_xuat.py` → `src/modules/import_export_receipts/clean_receipts_sale.py`
- Legacy `clean_xuat_nhap_ton.py` → `src/modules/import_export_receipts/clean_inventory.py`
- Legacy `generate_product_info.py` → `src/modules/import_export_receipts/extract_products.py`
- Legacy `clean_thong_tin_khach_hang.py` → `src/modules/receivable/clean_customers.py`
- Legacy `generate_new_customer_id.py` → `src/modules/receivable/extract_customer_ids.py`
- Legacy `clean_tong_no.py` → `src/modules/receivable/clean_debts.py`
- Legacy `pipeline.py` → `src/pipeline/orchestrator.py`
- Legacy `ingest.py` → `src/modules/ingest.py`
