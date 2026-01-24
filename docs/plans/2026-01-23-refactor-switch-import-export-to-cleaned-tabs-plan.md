---
title: refactor: Switch import/export receipts to use cleaned Google Sheets tabs as source
type: refactor
date: 2026-01-23
---

# Refactor: Switch Import/Export Receipts to Use Cleaned Google Sheets Tabs

## Overview

Switch the import/export receipts module to ingest data directly from **cleaned Google Sheets tabs** ("Chi tiết nhập", "Chi tiết xuất", "Xuất nhập tồn", "Chi tiết chi phí") instead of original tabs ("CT.NHAP", "CT.XUAT", "XNT"). This eliminates unnecessary transformation stages and uses validated, reconciled data as single source of truth.

**Current Pipeline Flow:**
```
Google Drive (CT.NHAP, CT.XUAT, XNT tabs)
    ↓ ingest.py
data/00-raw/import_export/ (YYYY_MM_CT.NHAP/XUAT/XNT.csv)
    ↓ clean_receipts_purchase.py (transforms + renames columns)
    ↓ clean_receipts_sale.py (transforms + renames columns)
    ↓ clean_inventory.py (transforms + renames columns)
    ↓ generate_products_xlsx.py / data_loader.py
data/01-staging/import_export/ (Chi tiết nhập/Xuất nhập tồn/etc. CSVs)
    ↓ upload_cleaned_to_sheets.py
Google Drive (Chi tiết nhập, Chi tiết xuất tabs - reconciled)
```

**New Pipeline Flow:**
```
Google Drive (Chi tiết nhập, Chi tiết xuất, Xuất nhập tồn, Chi tiết chi phí tabs)
    ↓ ingest.py (reads cleaned tabs directly)
data/00-raw/import_export/ (YYYY_MM_Chi tiết nhập/Xuất/XNT/etc.csv)
    ↓ (clean scripts deprecated)
data/01-staging/import_export/ (unchanged - same standardized format)
```

## Problem Statement / Motivation

### Current State Issues

1. **Redundant Transformations**: Clean scripts (`clean_receipts_*.py`) perform column renaming and cleanup that's already been done during reconciliation process in Google Sheets.

2. **Dual Source of Truth**: Original tabs and cleaned tabs both exist, creating ambiguity about which data is authoritative.

3. **Pipeline Complexity**: Three transformation stages (ingest → clean → upload back) when cleaned tabs could serve as direct source.

4. **Manual Reconciliation Overhead**: Running `upload_cleaned_to_sheets.py` after every manual fix is unnecessary when cleaned tabs ARE the source.

### Why This Matters

- **Data Quality**: Cleaned tabs contain validated, reconciled data (verified by `scripts/check_reconciliation_discrepancies.py` - all discrepancy columns = 0)

- **Simplified Maintenance**: Eliminates 3 scripts (2 clean + 1 upload) from ongoing maintenance burden

- **Clearer Architecture**: Single source of truth, linear data flow without circular upload loop

- **Blocker for Data Engine**: Must complete this change before implementing `docs/plans/2026-01-23-feat-data-engine-for-tire-shop-dashboard-plan.md` Phase 2 (Historical Data Ingestion), as that plan assumes ingesting from validated cleaned data

## Proposed Solution

### High-Level Approach

**Approach: Complete source switch with deprecation** (chosen over gradual migration or dual-source)

1. **Update Configuration**: Modify `pipeline.toml` to reference cleaned tab names instead of original tabs
2. **Mark Scripts Deprecated**: Add deprecation warnings to `clean_receipts_*.py` scripts but keep files for historical reference
3. **Stop Upload Script**: Mark `upload_cleaned_to_sheets.py` as DEPRECATED - no longer needed since cleaned tabs ARE the source
4. **Verify Downstream Compatibility**: Ensure `generate_products_xlsx.py`, `data_loader.py`, receivables/payables modules continue working
5. **Update Data Engine Plan**: Modify Phase 2 to reflect new source (cleaned tabs) and remove references to clean scripts

### Architecture Impact

**No Breaking Changes**: Downstream consumers read from `data/01-staging/import_export/` which maintains same format. The change is internal to ingest stage.

**Removed Components**:
```
❌ src/modules/import_export_receipts/clean_receipts_purchase.py
❌ src/modules/import_export_receipts/clean_receipts_sale.py
❌ src/modules/import_export_receipts/clean_inventory.py
❌ src/modules/import_export_receipts/upload_cleaned_to_sheets.py
```

**Modified Components**:
```
✓ pipeline.toml (tabs array)
✓ src/pipeline/orchestrator.py (module registry)
```

**Unchanged Components**:
```
→ src/modules/ingest.py (no changes - reads from config)
→ src/modules/import_export_receipts/generate_products_xlsx.py
→ src/pipeline/data_loader.py
→ src/modules/receivable/generate_customers_xlsx.py
→ src/modules/payable/generate_suppliers_xlsx.py
```

## Technical Approach

### Phase 1: Configuration Update

**File**: `pipeline.toml`

**Change**: Update tab names from original to cleaned tabs

```toml
# BEFORE
[sources.import_export_receipts]
tabs = ["CT.NHAP", "CT.XUAT", "XNT"]

# AFTER
[sources.import_export_receipts]
tabs = ["Chi tiết nhập", "Chi tiết xuất", "Xuất nhập tồn", "Chi tiết chi phí"]
```

**Impact**: `ingest.py` automatically reads new tab names (Line 157-158). Output filenames will change:
- `2025_01_CT.NHAP.csv` → `2025_01_Chi tiết nhập.csv`
- `2025_01_CT.XUAT.csv` → `2025_01_Chi tiết xuất.csv`
- `2025_01_XNT.csv` → `2025_01_Xuất nhập tồn.csv`
- `2025_01_Chi tiết chi phí.csv` (new - not previously ingested)

### Phase 2: Orchestrator Update

**File**: `src/pipeline/orchestrator.py`

**Change**: Remove clean scripts from `TRANSFORM_MODULES_LEGACY` registry

```python
# BEFORE (Lines 87-93)
TRANSFORM_MODULES_LEGACY = {
    "import_export_receipts": [
        "clean_inventory.py",
        "clean_receipts_purchase.py",
        "clean_receipts_sale.py",
        "refine_product_master.py",
        "generate_product_master.py",
    ],
    ...
}

# AFTER
TRANSFORM_MODULES_LEGACY = {
    "import_export_receipts": [
        "refine_product_master.py",  # Keep if needed for product unification
        # clean_receipts_purchase.py - DEPRECATED
        # clean_receipts_sale.py - DEPRECATED
        # clean_inventory.py - DEPRECATED
        # generate_product_master.py - Keep for historical reference
    ],
    ...
}
```

**Or completely remove**: If `refine_product_master.py` and `generate_product_master.py` are no longer needed either, simplify to:

```python
TRANSFORM_MODULES_LEGACY = {
    "import_export_receipts": [],  # No transforms needed
    ...
}
```

### Phase 3: Deprecation Warnings

**Files to modify**:
- `src/modules/import_export_receipts/clean_receipts_purchase.py`
- `src/modules/import_export_receipts/clean_receipts_sale.py`
- `src/modules/import_export_receipts/clean_inventory.py`
- `src/modules/import_export_receipts/upload_cleaned_to_sheets.py`

**Change**: Add deprecation notices to module docstrings and `main()` entry points

```python
# Add to top of each file
"""DEPRECATED: This script is no longer needed.

Data is now ingested directly from cleaned Google Sheets tabs:
- "Chi tiết nhập" (instead of CT.NHAP)
- "Chi tiết xuất" (instead of CT.XUAT)
- "Xuất nhập tồn" (instead of XNT)

This file is kept for historical reference only.
Last used: 2026-01-23
"""

# In main() function
if __name__ == "__main__":
    import logging
    import sys

    logging.warning("=" * 70)
    logging.warning("DEPRECATED SCRIPT")
    logging.warning("=" * 70)
    logging.warning("This script is no longer in active use.")
    logging.warning("Data is now ingested directly from cleaned Google Sheets tabs.")
    logging.warning("")
    logging.warning("If you need to run this for historical reasons,")
    logging.warning("use the --override-deprecation flag.")
    logging.warning("=" * 70)

    parser = argparse.ArgumentParser(...)
    parser.add_argument(
        "--override-deprecation",
        action="store_true",
        help="Run this deprecated script anyway (for historical reference only)"
    )
    args = parser.parse_args()

    if not args.override_deprecation:
        logging.error("Cannot run deprecated script without --override-deprecation flag")
        sys.exit(1)

    # Continue with existing logic...
```

### Phase 4: Downstream Validation

**Tests to run** after implementing Phases 1-3:

1. **Dry Run Ingest**: Verify new tabs are discovered and processed correctly

```bash
uv run src/modules/ingest.py --dry-run --sources import_export_receipts --year 2025 --month 1
```

Expected output:
```
[INFO] Found 4 desired tabs: Chi tiết nhập, Chi tiết xuất, Xuất nhập tồn, Chi tiết chi phí
[INFO] Discovered tabs: Chi tiết nhập, Chi tiết xuất, Xuất nhập tồn, Chi tiết chi phí
[INFO] [DRY RUN] Would process 2025_01 spreadsheet with tab: Chi tiết nhập
[INFO] [DRY RUN] Would process 2025_01 spreadsheet with tab: Chi tiết xuất
[INFO] [DRY RUN] Would process 2025_01 spreadsheet with tab: Xuất nhập tồn
[INFO] [DRY RUN] Would process 2025_01 spreadsheet with tab: Chi tiết chi phí
```

2. **Generate Products XLSX**: Verify `generate_products_xlsx.py` works with new filenames

```bash
uv run src/modules/import_export_receipts/generate_products_xlsx.py
```

Expected: `data/03-erp-export/Products.xlsx` generated successfully.

3. **Run Data Loader Tests**: Verify `data_loader.py` finds and loads renamed files

```python
# Test in Python REPL
from src.pipeline.data_loader import DataLoader
loader = DataLoader()
products = loader.load_products()
print(f"Loaded {len(products)} products")  # Should succeed
```

### Phase 5: Historical Data Handling

**Decision Point**: Handle existing staging data (`data/01-staging/import_export/`)

**Option A: Re-ingest all** (Recommended)
- Delete existing staging files
- Run full ingest from cleaned tabs (2020-2025)
- Ensures complete consistency

**Option B: Keep existing**
- Only ingest future months from cleaned tabs
- Risks data inconsistencies between old and new formats

**Implementation**: Use backup before re-ingest

```bash
# Backup existing staging data
mkdir -p data/01-staging/import_export.backup
cp data/01-staging/import_export/*.csv data/01-staging/import_export.backup/

# Re-ingest from cleaned tabs
uv run src/modules/ingest.py --sources import_export_receipts

# Verify staging files are regenerated
ls -la data/01-staging/import_export/
```

### Phase 6: Data Engine Plan Update

**File**: `docs/plans/2026-01-23-feat-data-engine-for-tire-shop-dashboard-plan.md`

**Sections to update**:

1. **Problem Statement** (Lines 32-42): Update source description
   ```markdown
   # BEFORE
   Historical data (2020-2025): Google Drive spreadsheets (~450K transaction rows)

   # AFTER
   Historical data (2020-2025): Google Drive cleaned tabs (~450K transaction rows)
   ```

2. **Technical Approach - Historical Data Ingestion** (Lines 395-467): Update code example
   ```python
   # BEFORE (Line 421-424)
   for file_path in files:
       sales_df = parse_xlsx_tab(file_path, "CT.XUAT")
       purchases_df = parse_xlsx_tab(file_path, "CT.NHAP")
       inventory_df = parse_xlsx_tab(file_path, "XNT")

   # AFTER
   for file_path in files:
       sales_df = parse_google_sheet_tab(file_path, "Chi tiết xuất")
       purchases_df = parse_google_sheet_tab(file_path, "Chi tiết nhập")
       inventory_df = parse_google_sheet_tab(file_path, "Xuất nhập tồn")
   ```

3. **Phase 2 Tasks** (Lines 1107-1137): Remove references to clean scripts
   ```markdown
   # BEFORE
   - [ ] Create src/etl/transform_historical.py
   - [ ] Parse CT.NHAP, CT.XUAT, XNT tabs from XLSX files

   # AFTER
   - [ ] Create src/etl/transform_historical.py
   - [ ] Parse Chi tiết nhập, Chi tiết xuất, Xuất nhập tồn tabs from Google Sheets
   - [ ] Note: Data is already cleaned, transformation focuses on schema mapping only
   ```

4. **Alternative Approaches** (Lines 1421-1468): Update rationale
   ```markdown
   Add note: "Source tabs have been validated and cleaned via manual reconciliation.
   No additional data cleaning required beyond schema mapping."
   ```

## Alternative Approaches Considered

### Approach A: Complete Source Switch + Simplify (CHOSEN)

**Description**: Switch ingest.py to read cleaned tabs, deprecate clean scripts, and stop upload script entirely.

**Pros**:
- Eliminates 2 pipeline stages (clean + upload), reducing complexity
- Uses reconciled data as source of truth (validated by discrepancy checker)
- Cleaner architecture for data engine migration
- Removes redundant data transformations

**Cons**:
- Requires thorough testing of column mapping from cleaned tabs
- Breaks ability to manually reconcile via Google Sheets UI (but this is intentional)

**Best when**: Cleaned tabs are validated and want a simpler pipeline before starting data engine work.

---

### Approach B: Source Switch + Keep Upload for Manual Reconciliation

**Description**: Switch source to cleaned tabs for ingest, but keep `upload_cleaned_to_sheets.py` for ad-hoc reconciliation workflows.

**Pros**:
- Maintains manual reconciliation capability if needed
- Uses reconciled data as primary source
- Less breaking change than stopping upload entirely

**Cons**:
- Creates two potential sources of truth (ingest source vs reconciliation output)
- More complex to maintain and debug
- Potential confusion about which data is "real"

**Best when**: Still need manual reconciliation workflows occasionally.

**Why Rejected**: Chosen Approach A provides cleaner architecture. Manual reconciliation can be done in-place on cleaned tabs before ingest.

---

### Approach C: Gradual Migration with Dual Ingest

**Description**: Add config flag to switch between original tabs and cleaned tabs. Run both in parallel for validation period.

**Pros**:
- Lowest risk - can A/B test new approach
- Allows gradual validation and rollback
- Can compare outputs before committing

**Cons**:
- Highest complexity - two code paths to maintain
- Longer migration timeline
- Temporary complexity that might never be cleaned up

**Best when**: Uncertainty about data quality or need extensive validation before migration.

**Why Rejected**: Unnecessary complexity. Cleaned tabs are already validated via `scripts/check_reconciliation_discrepancies.py`. No need for A/B testing.

## Acceptance Criteria

### Functional Requirements

- [ ] **Configuration Updated**: `pipeline.toml` tabs array changed to `["Chi tiết nhập", "Chi tiết xuất", "Xuất nhập tồn", "Chi tiết chi phí"]`

- [ ] **Ingest Reads Cleaned Tabs**: `ingest.py` successfully reads all 4 cleaned tabs from Google Drive spreadsheets matching "Xuất Nhập Tồn YYYY-MM" pattern

- [ ] **Output Filenames Updated**: Raw CSVs saved as `{YYYY}_{MM}_Chi tiết nhập.csv`, `{YYYY}_{MM}_Chi tiết xuất.csv`, `{YYYY}_{MM}_Xuất nhập tồn.csv`, `{YYYY}_{MM}_Chi tiết chi phí.csv`

- [ ] **Clean Scripts Deprecated**: Deprecation warnings added to `clean_receipts_purchase.py`, `clean_receipts_sale.py`, `clean_inventory.py`, and `upload_cleaned_to_sheets.py`

- [ ] **Orchestrator Updated**: Clean scripts removed from `TRANSFORM_MODULES_LEGACY` registry in `orchestrator.py`

- [ ] **Downstream Compatibility**: `generate_products_xlsx.py` successfully generates `Products.xlsx` with new source filenames

- [ ] **Data Loader Works**: `data_loader.py` successfully loads products and transactions from new staging files

- [ ] **Historical Data Ingested**: Full re-ingest of 2020-2025 data from cleaned tabs completes successfully (or verified against existing staging files if Option B chosen)

- [ ] **Data Engine Plan Updated**: `docs/plans/2026-01-23-feat-data-engine-for-tire-shop-dashboard-plan.md` Phase 2 updated to reference cleaned tabs

### Quality Gates

- [ ] **Dry Run Validation**: `ingest.py --dry-run` shows correct tab discovery and processing order

- [ ] **Column Structure Verified**: Cleaned tabs have identical column structure to what staging files expect (verify against one month sample)

- [ ] **Row Count Validation**: Compare row counts between new staging files and existing staging files (if Option B) to ensure no data loss

- [ ] **Receivables Module Works**: `generate_customers_xlsx.py` still generates correct customer data from new staging files

- [ ] **Payables Module Works**: `generate_suppliers_xlsx.py` still generates correct supplier data from new staging files

### Testing Requirements

- [ ] **Unit Tests**: Update `tests/test_ingest.py` to test with cleaned tab names in config

- [ ] **Integration Test**: Run full pipeline with `uv run src/pipeline/orchestrator.py -m ier` and verify all stages complete

- [ ] **Manual Spot Check**: Compare a sample month's data between old and new staging files to confirm accuracy

- [ ] **Regression Test**: Run existing downstream modules (`generate_products_xlsx.py`, data_loader, receivables/payables) and verify no errors

## Success Metrics

- **Pipeline Simplicity**: Reduce transform modules from 3 to 0 (clean scripts removed)
- **Maintainability**: Eliminate circular data flow (no upload back to source)
- **Performance**: Ingest time should remain similar or improve (no additional transformation stages)
- **Data Accuracy**: Row counts in staging files should match expected totals (within 1% tolerance)
- **Zero Breaking Changes**: All downstream modules continue working without code changes

## Dependencies & Risks

### Dependencies

- **None blocking**: This is a self-contained refactoring within import_export module
- **Data Engine Plan**: This change must complete before starting data engine Phase 2 (Historical Data Ingestion)

### Risks & Mitigation

| Risk | Likelihood | Impact | Mitigation |
|-------|------------|--------|-------------|
| **Column structure mismatch** between cleaned tabs and what staging expects | Medium | Medium - ingest would fail or produce incorrect data | **Mitigation**: Phase 4 dry-run testing + manual spot check on sample month |
| **Historical data inconsistency** if re-ingesting all 2020-2025 data | Low | High - lose trust in historical data | **Mitigation**: Backup existing staging files before re-ingest, compare row counts |
| **Downstream modules fail** to parse renamed staging files | Low | High - break data engine and other consumers | **Mitigation**: Phase 4 downstream validation tests before committing |
| **Upload script needed** for emergency manual reconciliation after deprecation | Low | Medium - can't fix data in Google Sheets and re-upload | **Mitigation**: Keep `upload_cleaned_to_sheets.py` as script with `--override-deprecation` flag, document in ops guide |
| **Cleaned tabs missing** "Chi tiết chi phí" tab for historical months | Medium | Low - missing expense data | **Mitigation**: Make "Chi tiết chi phí" tab optional in ingest (skip if not present) |

### Rollback Plan

If issues arise after deployment:

1. **Revert Configuration**: Change `pipeline.toml` tabs back to `["CT.NHAP", "CT.XUAT", "XNT"]`
2. **Restore Orchestrator**: Add clean scripts back to `TRANSFORM_MODULES_LEGACY`
3. **Restore Staging Data** (if re-ingested): `cp data/01-staging/import_export.backup/*.csv data/01-staging/import_export/`
4. **Test**: Run full pipeline and verify functionality

## Resource Requirements

### Time Estimate

| Phase | Estimated Time | Notes |
|--------|----------------|-------|
| **Phase 1: Config Update** | 15 min | Simple config change |
| **Phase 2: Orchestrator Update** | 30 min | Remove from registry, test |
| **Phase 3: Deprecation Warnings** | 45 min | 4 files to update |
| **Phase 4: Downstream Validation** | 2 hours | Dry-run, products gen, data loader tests |
| **Phase 5: Historical Data Handling** | 1-4 hours | Depends on re-ingest decision |
| **Phase 6: Data Engine Plan Update** | 30 min | Update relevant sections |
| **Total** | **5-7 hours** | |

### Expertise Required

- Python (reading/writing pipeline code)
- Understanding of ETL data flow
- Familiarity with Google Sheets API (for understanding context, not for changes)

### Infrastructure

- Google Drive API access (for testing, not for changes)
- Local environment with `uv` package manager

## Documentation Plan

### Files to Update

1. **`README.md`** (if exists in `src/modules/import_export_receipts/`)
   - Update pipeline flow diagram
   - Document deprecated scripts and their purpose

2. **`AGENTS.md`** (if contains import_export guidance)
   - Update section on data ingestion to reflect cleaned tabs as source

3. **Add Migration Guide** (optional, in `docs/migrations/`)
   - Document this source switch for future reference
   - Include before/after pipeline flow
   - Add troubleshooting tips

## References & Research

### Internal References

- **Configuration**: `pipeline.toml:20` - Tab names array
- **Ingest Module**: `src/modules/ingest.py:157-209` - Tab processing logic
- **Orchestrator**: `src/pipeline/orchestrator.py:87-93` - Transform module registry
- **Clean Scripts**:
  - `src/modules/import_export_receipts/clean_receipts_purchase.py:60-75` - Column indices mapping
  - `src/modules/import_export_receipts/clean_receipts_sale.py:63-87` - Column rename mapping
  - `src/modules/import_export_receipts/clean_inventory.py:43-64` - Column rename mapping
- **Downstream Consumers**:
  - `src/modules/import_export_receipts/generate_products_xlsx.py:464-470` - File pattern matching
  - `src/pipeline/data_loader.py:80-125` - Product loading from staging

### External References

- **Validation Script**: `scripts/check_reconciliation_discrepancies.py` - Verifies cleaned tabs have zero discrepancies
- **Data Engine Plan**: `docs/plans/2026-01-23-feat-data-engine-for-tire-shop-dashboard-plan.md` - Must update before Phase 2

### Related Work

- **Brainstorm**: `docs/brainstorms/2026-01-23-switch-import-export-sources-to-cleaned-tabs-brainstorm.md` - Initial design exploration
- **Open Question**: Column structure of cleaned tabs needs verification during Phase 4

## Implementation Checklist

### Pre-Implementation

- [ ] Review `scripts/check_reconciliation_discrepancies.py` output to confirm cleaned tabs are validated
- [ ] Backup existing staging data: `cp -r data/01-staging/import_export data/01-staging/import_export.backup`
- [ ] Document rollback steps (see Rollback Plan section)

### Implementation

- [ ] **Phase 1**: Update `pipeline.toml` tabs array
- [ ] **Phase 2**: Update `src/pipeline/orchestrator.py` remove clean scripts
- [ ] **Phase 3**: Add deprecation warnings to 4 clean scripts + upload script
- [ ] **Phase 4.1**: Run `ingest.py --dry-run --sources import_export_receipts --year 2025 --month 1`
- [ ] **Phase 4.2**: Run `generate_products_xlsx.py` and verify output
- [ ] **Phase 4.3**: Test `data_loader.py` in Python REPL
- [ ] **Phase 5**: Re-ingest historical data (OR verify existing)
- [ ] **Phase 6**: Update data engine plan

### Post-Implementation

- [ ] Run full pipeline: `uv run src/pipeline/orchestrator.py -m ier`
- [ ] Compare row counts between old and new staging files
- [ ] Manually spot-check sample data in cleaned tabs vs staging files
- [ ] Run receivables and payables modules to verify they work
- [ ] Run tests: `pytest tests/`
- [ ] Update documentation (README.md, AGENTS.md if applicable)
- [ ] Commit changes with message: `refactor: Switch import/export to cleaned tabs as source`
- [ ] Delete backup folder after validation: `rm -rf data/01-staging/import_export.backup`
