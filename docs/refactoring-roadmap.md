# Refactoring Roadmap – Legacy Script Migration Status

Legacy scripts are being refactored into raw source modules for better organization and AI agent context efficiency.

## Migration Mapping

| Current Script | Target Module | Target File | Status |
|---|---|---|---|
| `ingest.py` | `src/modules/` | `ingest.py` + `google_api.py` | ✅ Migrated |
| `clean_chung_tu_nhap.py` | `import_export_receipts` | `clean_receipts_purchase.py` | Pending |
| `clean_chung_tu_xuat.py` | `import_export_receipts` | `clean_receipts_sale.py` | Pending |
| `clean_xuat_nhap_ton.py` | `import_export_receipts` | `clean_inventory.py` | Pending |
| `generate_product_info.py` | `import_export_receipts` | `extract_products.py` | Pending |
| `clean_thong_tin_khach_hang.py` | `receivable` | `clean_customers.py` | Pending |
| `generate_new_customer_id.py` | `receivable` | `extract_customer_ids.py` | Pending |
| `clean_tong_no.py` | `receivable` | (deprecated, analyze usage) | Pending |
| `pipeline.py` | `src/pipeline/` | `orchestrator.py` | Pending |

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

## Key Architecture Patterns

See `docs/architecture-decisions.md` for detailed rationales behind:
- **Staging pattern** (safe re-runs)
- **Configuration-driven pipeline** (vs hardcoded)
- **Content-hash caching** (vs mtime)
- **Raw-source grouping** (vs processing phase grouping)

## Legacy Files Reference

Old scripts kept in root and legacy/ folder for reference during migration:
- `clean_chung_tu_nhap.py`
- `clean_chung_tu_xuat.py`
- `clean_xuat_nhap_ton.py`
- `generate_product_info.py`
- `clean_thong_tin_khach_hang.py`
- `generate_new_customer_id.py`
- `clean_tong_no.py`
- `pipeline.py`
- `ingest.py`

Once migration is complete, these will be moved to `legacy/` folder with deprecation notices.
