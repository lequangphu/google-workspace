# Reconciliation Report – File-by-File Dropout Breakdown

## Overview

The `reconciliation_report.json` file now provides detailed file-by-file dropout analysis for purchase receipt data. This helps identify which source files contribute most to quantity loss during transformation.

## Report Structure

```json
{
  "timestamp": "2025-12-29T11:30:48.440386",
  "input": {
    "total_quantity": 2697561.0,
    "total_rows": 43788,
    "files": {
      "2021_5_CT.NHAP.csv": 298604.0,
      "2021_7_CT.NHAP.csv": 43648.0,
      ...
    }
  },
  "output": {
    "total_quantity": 2437561.0,
    "total_rows": 43787
  },
  "dropout_by_file": {
    "2021_5_CT.NHAP.csv": {
      "input_quantity": 298604.0,
      "output_quantity": 38604.0,
      "dropout_quantity": 260000.0,
      "dropout_pct": 87.07
    },
    "2021_7_CT.NHAP.csv": {
      "input_quantity": 43648.0,
      "output_quantity": 43648.0,
      "dropout_quantity": 0.0,
      "dropout_pct": 0.0
    },
    ...
  },
  "lineage": {
    "total_tracked": 3094414,
    "success": 3094414,
    "rejected": 0,
    "success_rate": 100.0
  },
  "reconciliation": {
    "quantity_dropout_pct": 9.64,
    "row_dropout_pct": 0.0023
  },
  "alerts": []
}
```

## Sections Explained

### `input`
- **total_quantity**: Sum of all quantities from raw CSV files (all warehouse columns: Kho 1–5)
- **total_rows**: Total input rows processed
- **files**: Breakdown of input quantities per source file (e.g., 2021_5_CT.NHAP.csv, 2021_7_CT.NHAP.csv)

### `output`
- **total_quantity**: Sum of quantities in final output CSV (Kho 1 only)
- **total_rows**: Final output row count

### `dropout_by_file` (NEW)
Per-file analysis showing:
- **input_quantity**: Raw quantity from source file (all warehouses)
- **output_quantity**: Quantity preserved in output after filtering
- **dropout_quantity**: Amount lost (input − output)
- **dropout_pct**: Percentage loss for this file

Files are sorted alphabetically by filename.

### `lineage`
- **total_tracked**: All rows processed through DataLineage tracker
- **success**: Rows successfully transformed
- **rejected**: Rows rejected (validation failures)
- **success_rate**: Percentage of rows that succeeded

### `reconciliation`
- **quantity_dropout_pct**: Overall dropout percentage (expected ~9.6% from warehouse filtering)
- **row_dropout_pct**: Row-level dropout (should be < 1% in normal operation)

### `alerts`
List of warnings for:
- Dropout > 15% (beyond expected 9.6%)
- Success rate < 99% (rejections)

## How to Use

### Identify Problem Files

```bash
jq '.dropout_by_file | to_entries[] | select(.value.dropout_pct > 15) | {file: .key, dropout_pct: .value.dropout_pct}' \
  data/01-staging/import_export/reconciliation_report.json
```

Output:
```json
{
  "file": "2021_5_CT.NHAP.csv",
  "dropout_pct": 87.07
}
```

### Calculate Total Loss by File

```bash
jq '.dropout_by_file | map({file: .key, loss: .value.dropout_quantity}) | sort_by(-.loss) | .[0:10]' \
  data/01-staging/import_export/reconciliation_report.json
```

### Verify All Files Accounted For

```bash
# Count files in report
jq '.dropout_by_file | length' data/01-staging/import_export/reconciliation_report.json

# Count actual files
ls -1 data/00-raw/import_export/*CT.NHAP.csv | wc -l
```

Should match.

## Interpretation Guide

### Normal Cases

| Scenario | Expected Dropout % | Reason |
|----------|-------------------|--------|
| Most files | ~0% | All quantities in Kho 1 (kept) |
| Overall average | ~9.6% | Dropping Kho 2, 3, Asc, Đào Khánh |
| Occasional file | 5–15% | Mixed warehouse data |

### Warning Cases

| Scenario | Dropout % | Action |
|----------|-----------|--------|
| Single file > 50% | High | Audit raw data; check date mismatches |
| Multiple files > 15% | Alert logged | Review warehouse distribution in source |
| Row dropout > 1% | Alert logged | Check lineage for rejections |
| Success rate < 99% | Alert logged | Investigate rejected rows in lineage CSV |

## Files Generated

| File | Purpose |
|------|---------|
| `reconciliation_report.json` | Main report with all metrics |
| `Chi tiết nhập 2020-04_2025-12.csv` | Final cleaned output data |
| `lineage_YYYYMMDD_HHMMSS.csv` | Row-by-row audit trail (source → output mapping) |

## Debugging Workflow

1. **Run transformation**: `uv run src/modules/import_export_receipts/clean_receipts_purchase.py`
2. **Check overall dropout**: Look at `reconciliation.quantity_dropout_pct`
3. **Identify problem files**: Find entries in `dropout_by_file` with high %
4. **Audit file dates**: Check if dates in problem file are valid/within expected range
5. **Review lineage**: Look at `lineage_*.csv` for rejected rows from problem file
6. **Fix source data**: Correct dates, formats, or values in raw CSV
7. **Re-run transformation**: Repeat until alerts are clear

## Configuration

The 9.6% expected dropout threshold is currently hardcoded in the function:

```python
# NOTE: 9.6% quantity loss is EXPECTED - we only keep Kho 1 (main warehouse),
# dropping Kho 2, Kho 3, Asc, Đào Khánh.
# Threshold: warn if > 15% (beyond expected 9.6%), fail if > 20%.
```

To adjust thresholds, modify the warning/failure conditions in `clean_receipts_purchase.py` line ~730.

---

**Generated**: Dec 2025  
**Related**: docs/pipeline-io.md, AGENTS.md (ADR-5: Data Lineage Tracking)
