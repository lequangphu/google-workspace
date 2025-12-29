# Lineage & Reconciliation Implementation

## Overview

All three receipt/inventory transformation scripts now include:
- ✅ DataLineage tracking for every row (success/rejection)
- ✅ Lineage audit trail CSV output
- ✅ Reconciliation reports comparing input vs output
- ✅ Centralized lineage storage in `data/.lineage/` (not 01-staging)

## Scripts Updated

1. **`clean_receipts_purchase.py`** (CT.NHAP)
2. **`clean_receipts_sale.py`** (CT.XUAT) — NEWLY ADDED
3. **`clean_inventory.py`** (XNT) — NEWLY ADDED

## Data Lineage Directory Structure

**Old structure (lineage files scattered in staging):**
```
data/01-staging/import_export/
├── Chi tiết nhập ....csv
├── lineage_20250101_120000.csv    ❌ Clutter
├── reconciliation_report.json      ❌ Clutter
└── ...
```

**New structure (centralized lineage storage):**
```
data/
├── .lineage/
│   ├── lineage_20250101_120000.csv          [clean_receipts_purchase run]
│   ├── lineage_20250101_120012.csv          [clean_receipts_sale run]
│   ├── lineage_20250101_120024.csv          [clean_inventory run]
│   └── reconciliation_report_20250101.json  [all scripts' reports]
│
├── 00-raw/
│   └── import_export/
│       ├── *CT.NHAP.csv
│       ├── *CT.XUAT.csv
│       └── *XNT.csv
│
├── 01-staging/
│   └── import_export/
│       ├── Chi tiết nhập ....csv
│       ├── Chi tiết xuất ....csv
│       └── xuat_nhap_ton_....csv
│
└── 02-validated/
    └── [product extraction outputs]
```

## Key Changes by Script

### 1. `clean_receipts_purchase.py`
- ✅ Already had lineage tracking (line 29: `from src.modules.lineage import DataLineage`)
- ✅ Initializes lineage in `transform_purchase_receipts()` (line 383)
- ✅ Tracks rows in `process_group_data()` (lines 386-403)
- ✅ Saves lineage (line 486: `lineage.save()`)
- ✅ Creates reconciliation report (line 497)
- **Lineage storage**: `data/.lineage/` (via `DATA_LINEAGE_DIR`)

### 2. `clean_receipts_sale.py` (NEW)
- ✅ **Imports**: Added `DataLineage` from `src.modules.lineage` (line 30)
- ✅ **Config**: Added `DATA_LINEAGE_DIR = Path.cwd() / "data" / ".lineage"` (line 36)
- ✅ **Lineage initialization**: In `transform_sale_receipts()` (lines 651-652)
  ```python
  DATA_LINEAGE_DIR.mkdir(parents=True, exist_ok=True)
  lineage = DataLineage(DATA_LINEAGE_DIR)
  ```
- ✅ **Lineage tracking**: Updated `process_groups()` to accept `lineage` parameter (line 351)
  - Tracks each row with source file, row index, output row index
- ✅ **Lineage save**: `lineage.save()` (line 747)
- ✅ **Reconciliation**: New `create_reconciliation_checkpoint()` function (lines 532-620)
  - Compares input CT.XUAT files vs output staging CSV
  - Tracks success/rejection rates from lineage
  - Logs alerts if >5% dropout or >1% rejection

### 3. `clean_inventory.py` (NEW)
- ✅ **Imports**: Added `DataLineage` and supporting modules (lines 17-27)
- ✅ **Config**: Added `DATA_LINEAGE_DIR` (line 25)
- ✅ **Lineage initialization**: In `process()` (lines 694-696)
- ✅ **Lineage tracking**: Updated `load_and_process_group()` (lines 261, 312-327)
  - Tracks each row from every XNT file
- ✅ **Lineage save**: `lineage.save()` (line 778)
- ✅ **Reconciliation**: New `create_reconciliation_checkpoint()` function (lines 585-668)
  - Compares input XNT files vs output inventory CSV
  - Focuses on row counts (inventory quantities harder to extract from raw)

## Lineage CSV Format

**File**: `data/.lineage/lineage_YYYYMMDD_HHMMSS.csv`

| Column | Type | Example | Purpose |
|--------|------|---------|---------|
| `source_file` | str | `2025_01_CT.NHAP.csv` | Raw input filename |
| `source_row` | int | `42` | 0-based row index in source |
| `output_row` | int or "REJECTED" | `815` | Final row in output CSV or REJECTED |
| `operation` | str | `process_group_data` | Processing function name |
| `status` | str | `success` or `rejected: <reason>` | Success or error reason |
| `timestamp` | ISO datetime | `2025-01-01T12:00:00.123456` | When row was tracked |

**Example rows:**
```csv
source_file,source_row,output_row,operation,status,timestamp
2025_01_CT.NHAP.csv,0,0,process_group_data,success,2025-01-01T12:00:00.123456
2025_01_CT.NHAP.csv,1,1,process_group_data,success,2025-01-01T12:00:00.123457
2025_01_CT.NHAP.csv,2,REJECTED,process_group_data,rejected: invalid date,2025-01-01T12:00:00.123458
```

## Reconciliation Report Format

**File**: `data/.lineage/reconciliation_report_YYYYMMDD.json` (shared, appended by each script)

```json
{
  "timestamp": "2025-01-01T12:00:00.123456",
  "script": "clean_receipts_purchase",
  "input": {
    "total_quantity": 125000.5,
    "total_rows": 1850
  },
  "output": {
    "total_quantity": 113000.2,
    "total_rows": 1720
  },
  "reconciliation": {
    "quantity_dropout_pct": 9.6,
    "row_dropout_pct": 7.0
  },
  "dropout_by_file": {
    "2025_01_CT.NHAP.csv": {
      "input_quantity": 50000.0,
      "output_quantity": 45000.0,
      "dropout_quantity": 5000.0,
      "dropout_pct": 10.0
    }
  },
  "lineage": {
    "total_tracked": 1850,
    "success": 1720,
    "rejected": 130,
    "success_rate": 92.97
  },
  "alerts": [
    "⚠️ WARNING: 9.6% quantity dropped (125000.5 → 113000.2) [Expected ~9.6% from warehouse filtering]"
  ]
}
```

## Usage

### Run all three scripts with lineage tracking

```bash
# Full pipeline (all scripts)
uv run src/cli.py --full

# Or individual steps
uv run src/cli.py --step transform

# Direct script runs
uv run src/modules/import_export_receipts/clean_receipts_purchase.py
uv run src/modules/import_export_receipts/clean_receipts_sale.py
uv run src/modules/import_export_receipts/clean_inventory.py
```

### View lineage and reconciliation

```bash
# Check lineage files
ls -la data/.lineage/lineage_*.csv

# View reconciliation report
cat data/.lineage/reconciliation_report_*.json | jq .

# Check lineage for a specific file
grep "2025_01_CT.NHAP.csv" data/.lineage/lineage_*.csv
```

## Design Decisions (ADR-5)

### 1. Centralized `.lineage/` Directory

**Why not in 01-staging/?**
- 01-staging should contain only transformed DATA files (CSVs ready for downstream)
- Lineage CSVs are AUDIT TRAIL artifacts, not data
- Cleaner separation of concerns
- Prevents polluting staging directory during incremental runs

**Tradeoff:**
- One more directory to manage
- But standard practice in data pipelines (e.g., Apache Airflow, dbt)

### 2. Shared Reconciliation Report

**Single JSON file vs separate per-script:**
- ✅ All scripts append to same report
- ✅ Easy to compare dropout across CT.NHAP, CT.XUAT, XNT in one view
- ✅ Timestamps track when each script ran
- Structure:
  ```json
  {
    "scripts": [
      { "timestamp": "...", "script": "clean_receipts_purchase", ... },
      { "timestamp": "...", "script": "clean_receipts_sale", ... },
      { "timestamp": "...", "script": "clean_inventory", ... }
    ]
  }
  ```

### 3. Lineage Tracking per Row

**Why not sample?**
- MUST have 100% traceability per ADR-5 (Mandatory lineage tracking)
- Required for audit trail (accountants need to know which rows succeeded)
- Small overhead: ~10 bytes per row in lineage CSV

## Future Enhancements

1. **Consolidated reconciliation report** (single JSON with all scripts)
2. **Lineage dashboard** (query lineage CSV to show success/rejection statistics)
3. **Lineage archival** (compress old lineage files to `data/.lineage/archive/`)
4. **Cross-script lineage** (track products from CT.NHAP → CT.XUAT → extract_products)

---

**Created**: December 2025  
**Updated**: Lineage tracking for CT.XUAT and XNT scripts
