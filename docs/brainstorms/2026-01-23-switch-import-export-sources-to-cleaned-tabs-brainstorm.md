---
date: 2026-01-23
topic: switch-import-export-sources-to-cleaned-tabs
---

# Switch Import/Export Sources to Cleaned Google Sheets Tabs

## What We're Building

Change the import/export receipts module to ingest data from **cleaned Google Sheets tabs** (created by `upload_cleaned_to_sheets.py`) instead of original tabs (CT.NHAP, CT.XUAT, XNT). This simplifies the pipeline and uses validated, reconciled data as the single source of truth.

**New Pipeline Flow:**
```
Google Drive (Chi tiết nhập, Chi tiết xuất, Xuất nhập tồn, Chi tiết chi phí tabs)
    ↓ ingest.py (modified)
data/00-raw/import_export/ (raw CSVs)
    ↓ (cleaning scripts deprecated)
data/01-staging/import_export/ (unchanged - downstream consumers)
```

**Deprecated Steps:**
- `clean_receipts_purchase.py`, `clean_receipts_sale.py`, `clean_inventory.py` (marked as deprecated)
- `upload_cleaned_to_sheets.py` (stopped entirely)

## Why This Approach

Three key motivations driving this change:

1. **Data Quality**: Cleaned tabs contain validated, reconciled data (verified by `check_reconciliation_discrepancies.py`)
2. **Pipeline Simplification**: Remove 2 unnecessary transformation stages (clean + upload back)
3. **Single Source of Truth**: Eliminates ambiguity between original and cleaned data

**Alternatives Considered:**
- *Dual-source approach* (keep upload for reconciliation): Rejected as it creates competing sources of truth
- *Gradual migration* (A/B testing): Rejected as unnecessary complexity - cleaned data is already validated

**Chosen Approach**: Complete switch with deprecation warnings. Cleanest path forward with minimal ongoing maintenance burden.

## Key Decisions

- **Ingest Source Switch**: Modify `ingest.py` to read from cleaned tabs:
  - "Chi tiết nhập" instead of "CT.NHAP"
  - "Chi tiết xuất" instead of "CT.XUAT"
  - "Xuất nhập tồn" instead of "XNT"
  - "Chi tiết chi phí" (already in cleaned format)

- **Clean Scripts Deprecation**: Add deprecation warnings to `clean_receipts_*.py` but keep files for reference. Add docstring note: "DEPRECATED: Data now ingested from cleaned Google Sheets tabs. This script kept for historical reference."

- **Upload Script Removal**: Stop `upload_cleaned_to_sheets.py` entirely. Add `DEPRECATED` note to docstring. No further reconciliation uploads needed since cleaned tabs ARE the source.

- **Config Updates**: Modify `pipeline.toml`:
  ```toml
  [sources.import_export_receipts]
  # Old tabs (deprecated):
  # tabs = ["CT.NHAP", "CT.XUAT", "XNT"]
  # New cleaned tabs:
  tabs = ["Chi tiết nhập", "Chi tiết xuất", "Xuất nhập tồn", "Chi tiết chi phí"]
  ```

- **Downstream Validation**: Verify existing consumers still work:
  - `generate_products_xlsx.py` (uses DataLoader)
  - Receivables module (consumes `Chi tiết xuất`)
  - Payables module (consumes `Chi tiết nhập`)

- **Data Engine Plan Update**: Modify `docs/plans/2026-01-23-feat-data-engine-for-tire-shop-dashboard-plan.md` Phase 2 (Historical Data Ingestion):
  - Update source description: "Google Drive cleaned tabs" instead of "Google Drive spreadsheets"
  - Update tab names in all code examples
  - Remove references to `clean_receipts_*.py` scripts

## Open Questions

- **Column Mapping Validation**: Do cleaned tabs have identical column structure to original tabs? Need to verify `ingest.py` column indices still match.

- **Historical Data Handling**: What about 2020-2025 data already ingested from original tabs? Delete and re-ingest from cleaned tabs, or keep existing staging files?

- **Google Sheets Discovery**: Should `google_api.py::find_receipt_sheets()` be updated to search for cleaned tab names, or keep existing search (by spreadsheet name) and just change tab list in config?

## Next Steps

1. Run `ingest.py --dry-run` with new tab names to validate column mapping
2. Check historical staging data (`data/01-staging/`) against cleaned tabs for consistency
3. If validated, run full re-ingest from cleaned tabs
4. Update data engine plan before Phase 2 implementation

→ `/workflows:plan` for implementation details (blocker for data engine work)
